# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import json
import logging
import os
import tempfile

import apis.db.exp_packages
import apis.models
import apis.models.relationships
import apis.models.virtual_experiment
import apis.runtime
import apis.runtime.package_derived
import apis.runtime.package_transform
import apis.storage
import tests.conftest

package_from_flowir = tests.conftest.package_from_flowir
package_from_files = tests.conftest.package_from_files

logger = logging.getLogger("test")


def test_parse_surrogate_substitute():
    transform = apis.models.relationships.Transform(
        outputGraph=apis.models.relationships.GraphDescription(
            identifier="homo-lumo-dft-gamess-us",
            components=["AnionSMILESToGAMESSInput"]
        ),
        inputGraph=apis.models.relationships.GraphDescription(
            identifier="configuration-generator-ani-gamess",
            components=["GenerateOptimizedConfiguration"]
        ),
    )

    assert transform.outputGraph.components == ["stage0.AnionSMILESToGAMESSInput"]
    assert transform.inputGraph.components == ["stage0.GenerateOptimizedConfiguration"]


def test_gamess_homo_lumo_dft_surrogate_ani_single_component_graphs_flesh_out_twice(
        ve_homo_lumo_dft_gamess_us: apis.models.virtual_experiment.ParameterisedPackage,
        ve_configuration_generator_ani: apis.models.virtual_experiment.ParameterisedPackage,
        homolumogamess_ani_package_metadata: apis.storage.PackageMetadataCollection,
        output_dir: str,
):
    transform = apis.models.relationships.Transform(
        outputGraph=apis.models.relationships.GraphDescription(
            identifier="homo-lumo-dft-gamess-us",
            components=["AnionSMILESToGAMESSInput"]
        ),
        inputGraph=apis.models.relationships.GraphDescription(
            identifier="configuration-generator-ani-gamess",
            components=["GenerateOptimizedConfiguration"]
        ),
    )

    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            db.push_new_entry(ve_homo_lumo_dft_gamess_us)
            db.push_new_entry(ve_configuration_generator_ani)

            multi = apis.runtime.package_transform.TransformRelationship(transform)

    packages_metadata = homolumogamess_ani_package_metadata
    transform = multi.try_infer(packages_metadata).copy(deep=True)

    # VV: infer twice and make sure that both times you get the exact same answer
    # this tests adding a relationship to the database then synthesizing a package and re-applying the logic
    # to infer the parameters/results OR asking to infer parameters/results for a Transform relationship
    # that already contains some parameter/results mappings.
    transform_copy = multi.try_infer(packages_metadata)
    assert transform_copy.dict() == transform.dict()


def test_gamess_homo_lumo_dft_surrogate_ani_single_component_graphs_flesh_out(
        ve_homo_lumo_dft_gamess_us: apis.models.virtual_experiment.ParameterisedPackage,
        ve_configuration_generator_ani: apis.models.virtual_experiment.ParameterisedPackage,
        homolumogamess_ani_package_metadata: apis.storage.PackageMetadataCollection,
        output_dir: str,
):
    transform = apis.models.relationships.Transform(
        outputGraph=apis.models.relationships.GraphDescription(
            identifier="homo-lumo-dft-gamess-us",
            components=["AnionSMILESToGAMESSInput"]
        ),
        inputGraph=apis.models.relationships.GraphDescription(
            identifier="configuration-generator-ani-gamess",
            components=["GenerateOptimizedConfiguration"]
        ),
    )

    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            db.push_new_entry(ve_homo_lumo_dft_gamess_us)
            db.push_new_entry(ve_configuration_generator_ani)

            multi = apis.runtime.package_transform.TransformRelationship(transform)

    packages_metadata = homolumogamess_ani_package_metadata
    transform = multi.try_infer(packages_metadata)

    assert len(transform.relationship.graphResults) == 1

    x = transform.relationship.get_result_relationship_by_name_output("stage0.AnionSMILESToGAMESSInput:ref")
    assert x.inputGraphResult.name == "stage0.GenerateOptimizedConfiguration:ref"

    assert len(transform.relationship.graphParameters) == 2
    x = transform.relationship.get_parameter_relationship_by_name_input("input/pag_data.csv:copy")
    assert x.outputGraphParameter.name == "input/pag_data.csv:copy"

    x = transform.relationship.get_parameter_relationship_by_name_input("input/input_molecule.txt:copy")
    assert x.outputGraphParameter.name == "stage0.SetFunctional/input_molecule.txt:copy"


def test_gamess_homo_lumo_dft_surrogate_ani_stable_digest(
        ve_homo_lumo_dft_gamess_us: apis.models.virtual_experiment.ParameterisedPackage,
        ve_configuration_generator_ani: apis.models.virtual_experiment.ParameterisedPackage,
        homolumogamess_ani_package_metadata: apis.storage.PackageMetadataCollection,
        output_dir: str,
):
    transform = apis.models.relationships.Transform(
        outputGraph=apis.models.relationships.GraphDescription(
            identifier="homo-lumo-dft-gamess-us",
            components=["AnionSMILESToGAMESSInput"],
        ),
        inputGraph=apis.models.relationships.GraphDescription(
            identifier="configuration-generator-ani-gamess",
            components=["GenerateOptimizedConfiguration"],
        ),
    )

    multi = apis.runtime.package_transform.TransformRelationshipToDerivedPackage(transform)

    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            db.push_new_entry(ve_homo_lumo_dft_gamess_us)
            db.push_new_entry(ve_configuration_generator_ani)

            multi.discover_parameterised_packages(db)

    packages_metadata = homolumogamess_ani_package_metadata
    derived_ve = multi.prepare_derived_package("hello", apis.models.virtual_experiment.Parameterisation(
        presets=apis.models.virtual_experiment.ParameterisationPresets(platform="openshift")))
    multi.synthesize_derived_package(packages_metadata, derived_ve)

    for x in derived_ve.base.includePaths:
        logger.info(x.json(indent=2, exclude_none=True))
        logger.info(x.source.path)
        logger.info(packages_metadata.get_root_directory_containing_package(x.source.packageName))

    assert derived_ve.metadata.registry.digest == "sha256x415a78745ea5c4cec7237b091ca19b4ebf07e09b20225c3866efd40c"


def test_gamess_homo_lumo_dft_surrogate_ani_derive_persist(
        ve_homo_lumo_dft_gamess_us: apis.models.virtual_experiment.ParameterisedPackage,
        ve_configuration_generator_ani: apis.models.virtual_experiment.ParameterisedPackage,
        homolumogamess_ani_package_metadata: apis.storage.PackageMetadataCollection,
        output_dir: str,
):
    transform = apis.models.relationships.Transform(
        outputGraph=apis.models.relationships.GraphDescription(
            identifier="homo-lumo-dft-gamess-us",
            components=["AnionSMILESToGAMESSInput"],
        ),
        inputGraph=apis.models.relationships.GraphDescription(
            identifier="configuration-generator-ani-gamess",
            components=["GenerateOptimizedConfiguration"],
        ),
    )

    multi = apis.runtime.package_transform.TransformRelationshipToDerivedPackage(transform)

    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            db.push_new_entry(ve_homo_lumo_dft_gamess_us)
            db.push_new_entry(ve_configuration_generator_ani)

            multi.discover_parameterised_packages(db)

    packages_metadata = homolumogamess_ani_package_metadata
    derived_ve = multi.prepare_derived_package("hello", apis.models.virtual_experiment.Parameterisation(
        presets=apis.models.virtual_experiment.ParameterisationPresets(platform="openshift")))
    multi.synthesize_derived_package(packages_metadata, derived_ve)
    logger.info(f"Resulting derived {json.dumps(derived_ve.dict(), indent=2)}")

    dir_persist = os.path.join(output_dir, "persist")
    package = apis.runtime.package_derived.DerivedPackage(
        derived_ve, directory_to_place_derived=output_dir)
    package.synthesize(package_metadata=packages_metadata, platforms=derived_ve.get_known_platforms())
    package.persist_to_directory(dir_persist, packages_metadata)

    # VV: Ensure paths exist
    open(os.path.join(dir_persist, "conf", "flowir_package.yaml")).close()
    for x in ['input_cation.txt', 'input_anion.txt', 'input_molecule.txt', 'input_neutral.txt']:
        open(os.path.join(dir_persist, 'data', x)).close()

    for x in [
        'csv2inp.py', 'features_and_convergence.py', 'rdkit_smiles2coordinates.py',
        'featurize_gamess.py', 'extract_gmsout.py',
        # VV: `optimize_ani.py` is part of the surrogate VE
        'optimize_ani.py'
    ]:
        open(os.path.join(dir_persist, 'bin', x)).close()

    for x in ['interface.py', '__init__.py', 'dft_restart.py', 'semi_empirical_restart.py', ]:
        open(os.path.join(dir_persist, 'hooks', x)).close()


def test_psi4_surrogate_neural_potential_persist(
        ve_psi4: apis.models.virtual_experiment.ParameterisedPackage,
        ve_neural_potential: apis.models.virtual_experiment.ParameterisedPackage,
        package_metadata_psi4_neural_potential: apis.storage.PackageMetadataCollection,
        output_dir: str,
):
    packages_metadata = package_metadata_psi4_neural_potential
    rel = apis.models.relationships.Relationship(
        identifier="neural-potential-to-psi4",
        transform=apis.models.relationships.Transform(
            outputGraph=apis.models.relationships.GraphDescription(
                identifier="psi4",
                components=["Psi4Optimize"],
                package=ve_psi4.base.packages[0],
            ),
            inputGraph=apis.models.relationships.GraphDescription(
                identifier="neural-potential",
                components=["AniOptimize"],
                package=ve_neural_potential.base.packages[0],
            )))

    logger.info(f"Relationship: {rel.json(exclude_none=True, exclude_unset=True, indent=2)}")

    transform = apis.runtime.package_transform.TransformRelationshipToDerivedPackage(rel.transform)

    derived_ve = transform.prepare_derived_package(
        "hello", parameterisation=apis.models.virtual_experiment.Parameterisation())
    transform.synthesize_derived_package(packages_metadata, derived_ve)
    logger.info(f"Resulting derived {json.dumps(derived_ve.dict(), indent=2)}")

    dir_persist = os.path.join(output_dir, "persist")
    package = apis.runtime.package_derived.DerivedPackage(
        derived_ve, directory_to_place_derived=output_dir)
    package.synthesize(package_metadata=packages_metadata, platforms=None)
    package.persist_to_directory(dir_persist, packages_metadata)

    # VV: Ensure paths exist
    open(os.path.join(dir_persist, "conf", "flowir_package.yaml")).close()

    for x in [
        'aggregate_energies.py', 'optimize_ff.py', 'optimize_psi4.py',
        # VV: `optimize_ani.py` is part of the surrogate VE
        'optimize_ani.py'
    ]:
        open(os.path.join(dir_persist, 'bin', x)).close()
