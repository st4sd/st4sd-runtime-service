# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

import os

from typing import (
    Dict,
    Any,
    List,
    Optional,
    NamedTuple,
)

import experiment.model.frontends.flowir
import pydantic.error_wrappers
import apis.models.errors

import experiment.model.errors
import apis.db.exp_packages
import apis.db.relationships
import apis.models.common
import apis.models.constants
import apis.models.query_relationship
import apis.models.relationships
import apis.models.virtual_experiment
import apis.kernel.experiments
import apis.runtime.package
import apis.runtime.package_derived
import apis.runtime.package_transform
import apis.storage
import utils


class DSLAndPackage(NamedTuple):
    dsl: Dict[str, Any]
    package: apis.models.virtual_experiment.ParameterisedPackage


class MetadataAndParameterisedPackage(NamedTuple):
    metadata: apis.models.virtual_experiment.VirtualExperimentMetadata
    package: apis.models.virtual_experiment.ParameterisedPackage


def synthesize_ve_from_transformation(
        transform: apis.models.relationships.Transform,
        packages: apis.storage.PackageMetadataCollection,
        parameterisation: Optional[apis.models.virtual_experiment.Parameterisation] = None,
        name: str = "synthetic"
) -> apis.runtime.package_derived.DerivedPackage:
    transform = apis.runtime.package_transform.TransformRelationshipToDerivedPackage(transform)
    if parameterisation is None:
        parameterisation = apis.models.virtual_experiment.Parameterisation()

    ve = transform.prepare_derived_package(name, parameterisation)

    if packages.get_parameterised_package() != ve:
        packages.update_parameterised_package(ve)

    with packages:
        transform.synthesize_derived_package(packages, ve)
        apis.runtime.package.prepare_parameterised_package_for_download_definition(ve, db_secrets=packages.db_secrets)
        apis.runtime.package.get_and_validate_parameterised_package(ve, packages)
        return apis.runtime.package.combine_multipackage_parameterised_package(ve, packages)


def get_default_platform_of_package(
        db_experiments: apis.db.exp_packages.DatabaseExperiments,
        ve: apis.models.virtual_experiment.ParameterisedPackage,
) -> str:
    tag = ve.metadata.registry.tags[0] if ve.metadata.registry.tags else None
    identifier = apis.models.common.PackageIdentifier.from_parts(
        ve.metadata.package.name, tag=tag, digest=ve.metadata.registry.digest).identifier

    with db_experiments:
        docs = db_experiments.query_identifier(identifier)

    try:
        ve_target = apis.models.virtual_experiment.ParameterisedPackage.model_validate(docs[0])
        return (ve_target.parameterisation.get_available_platforms() or ['default'])[0]
    except KeyError:
        raise apis.models.errors.ApiError(f"Unknown package {identifier}")
    except pydantic.error_wrappers.ValidationError as e:
        raise apis.models.errors.InvalidModelError.from_pydantic(
            f"Package {identifier} is invalid. Fix it first and then retry.", e)


def get_relationship(
        identifier: str,
        db_relationships: apis.db.relationships.DatabaseRelationships,
) -> apis.models.relationships.Relationship:
    # VV: Lookup and validate relationship
    with db_relationships:
        ql = db_relationships.construct_query(identifier)
        docs = db_relationships.query(ql)

    if len(docs) == 0:
        raise apis.models.errors.DBError(identifier)
    try:
        rel = apis.models.relationships.Relationship.model_validate(docs[0])
    except pydantic.error_wrappers.ValidationError as e:
        raise apis.models.errors.InvalidModelError.from_pydantic(f"Invalid relationship {identifier}", e)

    if not rel.transform:
        raise apis.models.errors.InvalidModelError("Relationship is not Transform", problems=[])

    return rel


def get_relationship_transformation(
        identifier: str,
        db_relationships: apis.db.relationships.DatabaseRelationships,
) -> apis.models.relationships.Relationship:
    # VV: Lookup and validate relationship
    rel = get_relationship(identifier, db_relationships)

    # VV: Double check that this is actually a transformation
    if not rel.transform:
        raise apis.models.errors.InvalidModelError("Relationship is not Transform", problems=[])

    return rel


def parameterisation_of_synthesized_from_outputgraph(
        dsl: experiment.model.frontends.flowir.FlowIRConcrete,
        outputgraph_param: apis.models.virtual_experiment.Parameterisation,
) -> apis.models.virtual_experiment.Parameterisation:
    """Update the parameterisation of the synthesized package @ve using the parameterisation of its outputGraph

    Args:
        dsl: The DSL 1.0 of the synthesized virtual experiment
        outputgraph_param: The parameterisation options of the outputGraph that the transformation references

    Returns:
        The parameterisation options that layer the @outputgraph_param settings over the auto-generated parameterisation
        options from the synthesized virtual experiment
    """
    # VV: The outputGraph may have parameterisation options for variables that no longer exist, throw those away
    all_vars = set()

    for platform in dsl.platforms:
        platform_vars = dsl.get_platform_global_variables(platform)
        all_vars.update(platform_vars)

    param_outputgraph = apis.models.virtual_experiment.Parameterisation()
    param_outputgraph.presets.variables = [x for x in outputgraph_param.presets.variables
                                           if x.name in all_vars]
    param_outputgraph.executionOptions.variables = [
        x for x in outputgraph_param.executionOptions.variables
        if x.name in all_vars]

    # VV: This will include information extracted from the synthesized pvep
    platforms = outputgraph_param.get_available_platforms()
    param_auto = apis.models.virtual_experiment.parameterisation_from_flowir(dsl, platforms=platforms)

    #  VV: finally use the parameterisation options
    return apis.models.virtual_experiment.merge_parameterisation(param_auto, param_outputgraph)


def synthesize_from_transformation(
        rel: apis.models.relationships.Relationship,
        new_package_name: str,
        packages: apis.storage.PackageMetadataCollection,
        db_experiments: apis.db.exp_packages.DatabaseExperiments,
        synthesize: apis.models.relationships.PayloadSynthesize,
        update_experiments_database: bool,
        path_multipackage: Optional[str],

):
    """Generates a virtual experiment standalone project and the Parameterised Virtual Experiment Package to wrap it
    from a transformation relationship

    Args:
        rel: A transformation relationship
        new_package_name: The name of the new PVEP wrapper
        packages: The collection of the package metadata (DSL 1.0, manifest, etc) of the underlying base packages
        db_experiments: The database containing definitions of experiments
        synthesize: the payload to the synthesize API
        update_experiments_database: Whether to store the resulting PVEP to the experiments database
        path_multipackage: the path under which the runtime service stores the standalone projects it synthesizes
            the final path will be ${path_multipackage}/${new_package_name}/${wrapper_pvep.registry.digest}.
            If this is None, then the resulting virtual experiment is not persisted on the disk

    Returns:
        A MetadataAndParameterisedPackage containing the VirtualExperimentMetadata (dsl, manifestdata, etc) and the
        auto-generated PVEP wrapper
    """
    target_parameterisation: Optional[apis.models.virtual_experiment.Parameterisation] = None

    with db_experiments:
        docs = db_experiments.query_identifier(rel.transform.outputGraph.identifier)
        if len(docs) == 1:
            try:
                target = apis.models.virtual_experiment.ParameterisedPackage.model_validate(docs[0])
                target_parameterisation = target.parameterisation
            except pydantic.error_wrappers.ValidationError as e:
                raise apis.models.errors.InvalidModelError.from_pydantic(
                    f"outputGraph {rel.transform.outputGraph.identifier} is invalid", e)

    transform = apis.runtime.package_transform.TransformRelationshipToDerivedPackage(rel.transform)

    # VV: If the payload does not set platform settings then copy them from the target
    if synthesize.parameterisation.get_available_platforms() is None and target_parameterisation:
        synthesize.parameterisation.presets.platform = target_parameterisation.presets.platform
        synthesize.parameterisation.executionOptions.platform = target_parameterisation.executionOptions.platform

    ve = transform.prepare_derived_package(new_package_name, synthesize.parameterisation)
    packages.update_parameterised_package(ve)

    with packages:
        # VV: Here we figure out which platforms we should include in the derived package
        # 1. those in the synthesize payload, if missing
        # 2. those in the target parameterisation, if missing
        # 3. those defined in the target DSL 1.0

        platforms = synthesize.parameterisation.get_available_platforms()

        if not platforms:
            if not platforms and target_parameterisation:
                platforms = target_parameterisation.get_available_platforms()

            if not platforms:
                platforms = packages.get_concrete_of_package(rel.transform.outputGraph.identifier).platforms

            synthesize.parameterisation.executionOptions.platform = platforms

        ve.parameterisation = synthesize.parameterisation
        transform.synthesize_derived_package(packages, ve)

        try:
            metadata: apis.runtime.package_derived.DerivedVirtualExperimentMetadata = \
                apis.runtime.package.access_and_validate_virtual_experiment_packages(
                    ve=ve, packages=packages, path_multipackage=path_multipackage)
        except experiment.model.errors.FlowIRConfigurationErrors as e:
            unique_errors = {str(e): e for e in e.underlyingErrors}
            raise apis.models.errors.TransformationManyErrors([unique_errors[k] for k in sorted(unique_errors)])
        except experiment.model.errors.FlowIRException as e:
            raise apis.models.errors.TransformationError(
                f"The transformation would produce invalid FlowIR. Underlying errors are: {e}")

        if synthesize.options.generateParameterisation:
            param_outputgraph = parameterisation_of_synthesized_from_outputgraph(
                metadata.concrete, target_parameterisation or apis.models.virtual_experiment.Parameterisation())

            # VV: Layering order (i-th is overriden by i+1 th)
            # 1. auto-generated (from the DSL of the synthesized virtual experiment)
            # 2. from outputGraph (target)
            # 3. from synthesize payload
            auto_param = apis.models.virtual_experiment.parameterisation_from_flowir(
                metadata.concrete, ve.get_known_platforms() or metadata.concrete.platforms)

            param = apis.models.virtual_experiment.merge_parameterisation(auto_param, param_outputgraph)
            param = apis.models.virtual_experiment.merge_parameterisation(param, synthesize.parameterisation)

            ve.parameterisation = param

        ve.update_digest()

        if path_multipackage:
            # VV: HACK Store the derived package in the same PVC that contains the virtual experiment
            # instances till we decide how we will use the derived package instructions to build the
            # synthesized package. Recall that the digest depends on the base packages AND parameterisation options.
            # The parameterisation options can change post push of PVEP therefore we cannot use digest here as we'd
            # need to re-derive the PVEP (or move it on the disk) on an update to its parameterisation fields.
            # Instead, we'll just use the information about the base packages.
            path_exp = os.path.join(path_multipackage, ve.metadata.package.name, ve.get_packages_identifier())
            metadata.derived.persist_to_directory(path_exp, packages)


        apis.runtime.package.validate_parameterised_package(ve=ve, metadata=metadata)
        if update_experiments_database:
            with db_experiments:
                db_experiments.push_new_entry(ve)

    return MetadataAndParameterisedPackage(metadata=metadata, package=ve)


def preview_synthesize_dsl(
        rel: apis.models.relationships.Relationship,
        packages: apis.storage.PackageMetadataCollection,
        db_experiments: apis.db.exp_packages.DatabaseExperiments,
        dsl_version: Optional[str] = None,
) -> DSLAndPackage:
    if dsl_version is None:
        dsl_version = "2.0.0_0.1.0"

    # VV: Emulate a synthesis, but do not persist it on disk
    synthesize = apis.models.relationships.PayloadSynthesize()

    metadata = synthesize_from_transformation(
        rel=rel,
        new_package_name="synthetic",
        packages=packages,
        db_experiments=db_experiments,
        synthesize=synthesize,
        update_experiments_database=False,
        path_multipackage=None,
    )

    # VV: Finally generate the DSL from that synthesis
    if dsl_version == "1":
        return DSLAndPackage(dsl=metadata.metadata.concrete.raw(), package=metadata.package)

    dsl = apis.models.virtual_experiment.dsl_from_concrete(
        concrete=metadata.metadata.concrete,
        manifest=metadata.metadata.manifestData,
        platform=(metadata.package.get_known_platforms() or ['default'])[0]
    )

    return DSLAndPackage(dsl=dsl, package=metadata.package)


def resolve_base_package(
        name: str,
        kind: str,
        db_experiments: apis.db.exp_packages.DatabaseExperiments
) -> apis.models.virtual_experiment.BasePackage:
    try:
        query = apis.kernel.experiments.api_get_experiment(name, db_experiments)
    except apis.models.errors.InvalidModelError as e:
        raise apis.models.errors.InvalidModelError(
            f"The {kind} parameterised virtual experiment package {name} is invalid", e.problems)

    if len(query.experiment.base.packages) != 1:
        raise apis.models.errors.ApiError(
            f"{kind} must point to a parameterised virtual experiment package with exactly 1 base package, "
            f"however it points to a package with {len(query.experiment.base.packages)} base packages")
    return query.experiment.base.packages[0]


def push_relationship(
        rel: apis.models.relationships.Relationship,
        db_relationships: apis.db.relationships.DatabaseRelationships,
        db_experiments: apis.db.exp_packages.DatabaseExperiments,
        packages: apis.storage.PackageMetadataCollection,
) -> apis.models.relationships.Relationship:
    """Enhance the relationship and then store it in the database

    Args:
        rel: A relationship
        db_relationships: The database to store the relationship in
        db_experiments: The database containing the parameterised virtual experiment packages that the relationship
            references
        packages: The collection of the package metadata (DSL 1.0, manifest, etc) of the underlying base packages
    """

    if rel.transform.inputGraph.package is None:
        identifier = rel.transform.inputGraph.identifier
        rel.transform.inputGraph.package = resolve_base_package(identifier, "inputGraph", db_experiments)

    if rel.transform.outputGraph.package is None:
        identifier = rel.transform.outputGraph.identifier
        rel.transform.outputGraph.package = resolve_base_package(identifier, "outputGraph", db_experiments)

    _ = preview_synthesize_dsl(
        rel=rel,
        packages=packages,
        db_experiments=db_experiments,
    )

    with db_relationships:
        db_relationships.upsert(rel.model_dump(exclude_none=False), ql=db_relationships.construct_query(rel.identifier))

    return rel


########### apis


def api_list_queries(
        query: apis.models.query_relationship.QueryRelationship,
        db: Optional[apis.db.relationships.DatabaseRelationships] = None,
) -> List[Dict[str, Any]]:
    if db is None:
        db = utils.database_relationships_open(apis.models.constants.LOCAL_DEPLOYMENT)

    with db:
        query = db.construct_complex_query(query)
        return db.query(query)


def api_preview_synthesize_dsl(
        identifier: str,
        packages: apis.storage.PackageMetadataCollection,
        db_relationships: apis.db.relationships.DatabaseRelationships,
        db_experiments: apis.db.exp_packages.DatabaseExperiments,
        dsl_version: Optional[str] = None,
) -> DSLAndPackage:
    """Previews the DSL of a would-be output of the api_synthesize_from_transformation() method

    Args:
        identifier: The relationship identifier
        packages: The collection of the package metadata (DSL 1.0, manifest, etc) of the underlying base packages
        db_relationships: The database containing definitions of relationships
        db_experiments: The database containing definitions of experiments
        dsl_version: The expected DSL version ("1", or "2.0.0_0.1.0")
    Returns:
        The DSL representation of the would-be synthesized parameterised virtual experiment package and the
        parameterised virtual experiment package
    """
    rel = get_relationship_transformation(identifier, db_relationships)

    return preview_synthesize_dsl(
        rel=rel,
        packages=packages,
        db_experiments=db_experiments,
        dsl_version=dsl_version,
    )


def api_synthesize_from_transformation(
        identifier: str,
        new_package_name: str,
        packages: apis.storage.PackageMetadataCollection,
        db_relationships: apis.db.relationships.DatabaseRelationships,
        db_experiments: apis.db.exp_packages.DatabaseExperiments,
        synthesize: apis.models.relationships.PayloadSynthesize,
        path_multipackage: Optional[str],
) -> MetadataAndParameterisedPackage:
    """Generates a virtual experiment standalone project and the Parameterised Virtual Experiment Package to wrap it
    from a transformation relationship

    Args:
        identifier: The relationship identifier
        new_package_name: The name of the new PVEP wrapper
        packages: The collection of the package metadata (DSL 1.0, manifest, etc) of the underlying base packages
        db_relationships: The database containing definitions of relationships
        db_experiments: The database containing definitions of experiments
        synthesize: the payload to the synthesize API
        path_multipackage: the path under which the runtime service stores the standalone projects it synthesizes
            the final path will be ${path_multipackage}/${new_package_name}/${wrapper_pvep.registry.digest}.
            If this is None, then the resulting virtual experiment is not persisted on the disk

    Returns:
        A MetadataAndParameterisedPackage containing the VirtualExperimentMetadata (dsl, manifestdata, etc) and the
        auto-generated PVEP wrapper
    """
    rel = get_relationship_transformation(identifier, db_relationships)

    return synthesize_from_transformation(
        rel=rel,
        new_package_name=new_package_name,
        packages=packages,
        db_experiments=db_experiments,
        synthesize=synthesize,
        update_experiments_database=True,
        path_multipackage=path_multipackage
    )


def api_push_relationship(
        rel: Dict[str, Any],
        db_relationships: apis.db.relationships.DatabaseRelationships,
        db_experiments: apis.db.exp_packages.DatabaseExperiments,
        packages: apis.storage.PackageMetadataCollection,
) -> apis.models.relationships.Relationship:
    """Enhance the relationship and then store it in the database

    Args:
        rel: A relationship
        db_relationships: The database to store the relationship in
        db_experiments: The database containing the parameterised virtual experiment packages that the relationship
            references
        packages: The collection of the package metadata (DSL 1.0, manifest, etc) of the underlying base packages
    """

    try:
        rel = apis.models.relationships.Relationship.model_validate(rel)
    except pydantic.error_wrappers.ValidationError as e:
        raise apis.models.errors.InvalidModelError.from_pydantic(f"Relationship is invalid", e)

    return push_relationship(
        rel=rel,
        db_relationships=db_relationships,
        db_experiments=db_experiments,
        packages=packages
    )
