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

import apis.db.exp_packages
import apis.db.relationships
import apis.models.common
import apis.models.query_relationship
import apis.models.relationships
import apis.models.virtual_experiment
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
        apis.runtime.package.prepare_parameterised_package_for_download_definition(ve)
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
        ve_target = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(docs[0])
        return (ve_target.parameterisation.get_available_platforms() or ['default'])[0]
    except KeyError:
        raise apis.models.errors.ApiError(f"Unknown package {identifier}")
    except pydantic.error_wrappers.ValidationError as e:
        raise apis.models.errors.InvalidModelError(
            f"Package {identifier} is invalid. Fix it first and then retry.", problems=e.errors())


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
        rel = apis.models.relationships.Relationship.parse_obj(docs[0])
    except pydantic.error_wrappers.ValidationError as e:
        raise apis.models.errors.InvalidModelError(f"Invalid relationship {identifier}", problems=e.errors())

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
    with db_experiments:
        docs = db_experiments.query_identifier(rel.transform.outputGraph.identifier)
        if len(docs) == 0:
            raise apis.models.errors.ParameterisedPackageNotFoundError(rel.transform.outputGraph.identifier)

        try:
            pvep_target = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(docs[0])
        except pydantic.error_wrappers.ValidationError as e:
            raise apis.models.errors.InvalidModelError(
                f"outputGraph {rel.transform.outputGraph.identifier} is invalid", problems=e.errors())

    transform = apis.runtime.package_transform.TransformRelationshipToDerivedPackage(rel.transform)

    # VV: If the payload does not set platform settings then copy them from the target
    if synthesize.parameterisation.get_available_platforms() is None:
        synthesize.parameterisation.presets.platform = pvep_target.parameterisation.presets.platform
        synthesize.parameterisation.executionOptions.platform = pvep_target.parameterisation.executionOptions.platform

    ve = transform.prepare_derived_package(new_package_name, synthesize.parameterisation)
    packages.update_parameterised_package(ve)

    with packages:
        transform.synthesize_derived_package(packages, ve)

        metadata: apis.runtime.package_derived.DerivedVirtualExperimentMetadata = \
            apis.runtime.package.access_and_validate_virtual_experiment_packages(
                ve=ve, packages=packages, path_multipackage=path_multipackage)

        if path_multipackage:
            # VV: HACK Store the derived package in the same PVC that contains the virtual experiment
            # instances till we decide how we will use the derived package instructions to build the
            # synthesized package.
            path_exp = os.path.join(path_multipackage, ve.metadata.package.name, ve.metadata.registry.digest)
            metadata.derived.persist_to_directory(path_exp, packages)

        if synthesize.options.generateParameterisation:
            param_outputgraph = parameterisation_of_synthesized_from_outputgraph(
                metadata.concrete, pvep_target.parameterisation)

            # VV: Layering order (i-th is overriden by i+1 th)
            # 1. auto-generated (from the DSL of the synthesized virtual experiment)
            # 2. from outputGraph (target)
            # 3. from synthesize payload
            auto_param = apis.models.virtual_experiment.parameterisation_from_flowir(
                metadata.concrete, ve.get_known_platforms() or metadata.concrete.platforms)

            param = apis.models.virtual_experiment.merge_parameterisation(auto_param, param_outputgraph)
            param = apis.models.virtual_experiment.merge_parameterisation(param, synthesize.parameterisation)

            ve.parameterisation = param

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


########### apis


def api_list_queries(
        query: apis.models.query_relationship.QueryRelationship,
        db: Optional[apis.db.relationships.DatabaseRelationships] = None,
) -> List[Dict[str, Any]]:
    if db is None:
        db = utils.database_relationships_open()

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
    _ = preview_synthesize_dsl(
        rel=rel,
        packages=packages,
        db_experiments=db_experiments,
    )

    with db_relationships:
        db_relationships.upsert(rel.dict(exclude_none=False), ql=db_relationships.construct_query(rel.identifier))

    return rel
