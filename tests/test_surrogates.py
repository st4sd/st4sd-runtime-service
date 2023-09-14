# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import collections
from typing import Any
from typing import Dict

import json
import logging
import os
import tempfile

import experiment.model.errors
import experiment.model.frontends.flowir

import pytest
import yaml

import apis.db.relationships
import apis.db.exp_packages
import apis.models
import apis.models.errors
import apis.models.common
import apis.kernel.relationships
import apis.models.relationships
import apis.models.virtual_experiment
import apis.runtime
import apis.runtime.package
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

    parameters = {
        x.inputGraphParameter.name: x.outputGraphParameter.name for x in transform.relationship.graphParameters
    }

    assert parameters == {
        "input/pag_data.csv:copy": "input/pag_data.csv:copy",
        "input/input_molecule.txt:copy": "stage0.SetFunctional/input_molecule.txt:copy",
        "backend": "backend",
        "conflicting": "conflicting",
    }

    assert len(transform.relationship.graphParameters) == len(parameters)

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

    assert derived_ve.metadata.registry.digest == "sha256x025cafd8675294cbb12e973d257c0c299f2e69ea3c8f5525f78ef368"


@pytest.mark.parametrize("variable_merge_policy", [
    apis.models.relationships.VariablesMergePolicy.InputGraphOverridesOutputGraph,
    apis.models.relationships.VariablesMergePolicy.OutputGraphOverridesInputGraph,
])
def test_gamess_homo_lumo_dft_surrogate_ani_derive_persist(
        ve_homo_lumo_dft_gamess_us: apis.models.virtual_experiment.ParameterisedPackage,
        ve_configuration_generator_ani: apis.models.virtual_experiment.ParameterisedPackage,
        homolumogamess_ani_package_metadata: apis.storage.PackageMetadataCollection,
        output_dir: str,
        variable_merge_policy: apis.models.relationships.VariablesMergePolicy
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
        relationship=apis.models.relationships.TransformRelationship(variablesMergePolicy=variable_merge_policy.value)
    )

    multi = apis.runtime.package_transform.TransformRelationshipToDerivedPackage(transform)

    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            db.push_new_entry(ve_homo_lumo_dft_gamess_us)
            db.push_new_entry(ve_configuration_generator_ani)

            multi.discover_parameterised_packages(db)

    packages_metadata = homolumogamess_ani_package_metadata
    # presets = apis.models.virtual_experiment.ParameterisationPresets(platform="openshift"))
    derived_ve = multi.prepare_derived_package("hello", apis.models.virtual_experiment.Parameterisation(
        executionOptions=apis.models.virtual_experiment.ParameterisationExecutionOptions(
            platform=['openshift', 'openshift-kubeflux'])
    ))
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

    variables = package.concrete_synthesized.get_platform_variables('openshift')
    if variable_merge_policy == apis.models.relationships.VariablesMergePolicy.OutputGraphOverridesInputGraph:
        assert variables['global']['conflicting'] == 'from foundation'
    elif variable_merge_policy == apis.models.relationships.VariablesMergePolicy.InputGraphOverridesOutputGraph:
        assert variables['global']['conflicting'] == 'from surrogate'
    else:
        raise ValueError(f"Unknown merge policy {variable_merge_policy}")


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
    package.synthesize(package_metadata=packages_metadata, platforms=ve_psi4.get_known_platforms())
    package.persist_to_directory(dir_persist, packages_metadata)

    # VV: Ensure paths exist
    open(os.path.join(dir_persist, "conf", "flowir_package.yaml")).close()

    for x in [
        'aggregate_energies.py', 'optimize_ff.py', 'optimize_psi4.py',
        # VV: `optimize_ani.py` is part of the surrogate VE
        'optimize_ani.py'
    ]:
        open(os.path.join(dir_persist, 'bin', x)).close()


def relationship_modular_ani_band_gap_gamess(all_mappings: bool) -> Dict[str, Any]:
    relationship = {
        "identifier": "ani-to-optimise-gamess-input:latest",
        "transform": {
            "inputGraph": {
                "identifier": "ani-geometry-optimisation:latest",
                "components": [
                    "stage0.GeometryOptimisationANI",
                    "stage0.XYZToGAMESS"
                ]
            },
            "outputGraph": {
                "identifier": "band-gap-dft-gamess-us:latest",
                "components": [
                    "stage0.XYZToGAMESS"
                ]
            },
            "relationship": {
                "graphParameters": [
                    {
                        "inputGraphParameter": {
                            "name": "stage0.XYZToGAMESS:ref"
                        },
                        "outputGraphParameter": {
                            "name": "stage0.SMILESToGAMESSInput:ref"
                        }
                    },
                    {
                        "inputGraphParameter": {
                            "name": "stage0.GetMoleculeIndex:output"
                        },
                        "outputGraphParameter": {
                            "name": "stage0.GetMoleculeIndex:output"
                        }
                    },
                    {
                        "inputGraphParameter": {
                            "name": "stage0.SMILESToXYZ:ref"
                        },
                        "outputGraphParameter": {
                            "name": "stage0.SMILESToXYZ:ref"
                        }
                    },
                    {
                        "inputGraphParameter": {
                            "name": "stage0.SetFunctional:ref"
                        },
                        "outputGraphParameter": {
                            "name": "stage0.SetFunctional:ref"
                        }
                    }
                ]
            }
        }
    }

    if all_mappings is False:
        # VV: We just need the 1st relationship, we can infer the rest
        transform = relationship['transform']
        transform['relationship']['graphParameters'] = transform['relationship']['graphParameters'][:1]

    return relationship


@pytest.mark.parametrize("relationship", [
    relationship_modular_ani_band_gap_gamess(True), relationship_modular_ani_band_gap_gamess(False)])
def test_modular_ani_band_gap_gamess_persist(
        ve_modular_ani: apis.models.virtual_experiment.ParameterisedPackage,
        ve_modular_band_gap_gamess: apis.models.virtual_experiment.ParameterisedPackage,
        package_metadata_modular_ani_band_gap_gamess: apis.storage.PackageMetadataCollection,
        relationship: Dict[str, Any],
        output_dir: str,
):
    packages = package_metadata_modular_ani_band_gap_gamess

    rel: apis.models.relationships.Relationship = apis.models.relationships.Relationship.parse_obj(relationship)

    rel.transform.inputGraph.package = ve_modular_ani.base.packages[0]
    rel.transform.outputGraph.package = ve_modular_band_gap_gamess.base.packages[0]

    logger.info(f"Relationship: {rel.json(exclude_none=True, exclude_unset=True, indent=2)}")

    transform = apis.runtime.package_transform.TransformRelationshipToDerivedPackage(rel.transform)

    derived_ve = transform.prepare_derived_package(
        "hello", parameterisation=apis.models.virtual_experiment.Parameterisation())
    transform.synthesize_derived_package(packages, derived_ve)
    logger.info(f"Resulting derived {json.dumps(derived_ve.dict(), indent=2)}")

    dir_persist = os.path.join(output_dir, "persist")
    platforms = packages.get_concrete_of_package("band-gap-dft-gamess-us:latest").platforms
    package = apis.runtime.package_derived.DerivedPackage(
        derived_ve, directory_to_place_derived=output_dir)
    package.synthesize(package_metadata=packages, platforms=platforms)
    package.persist_to_directory(dir_persist, packages)

    # VV: Ensure paths exist
    open(os.path.join(dir_persist, "conf", "flowir_package.yaml")).close()

    for x in [
        "rdkit_smiles2coordinates.py", "run-gamess.sh", "extract_gmsout_geo_opt.py",
        # VV: `optimize_ani.py` is part of the surrogate VE
        "optimize_ani.py"
    ]:
        open(os.path.join(dir_persist, 'bin', x)).close()

    for x in ["input_anion.txt", "input_cation.txt", "input_molecule.txt", "input_neutral.txt"]:
        open(os.path.join(dir_persist, 'data', x)).close()


def test_modular_optimizer_band_gap_reference_data(
        package_metadata_modular_optimizer_band_gap_gamess: apis.storage.PackageMetadataCollection,
        rel_optimizer_band_gap: apis.models.relationships.Relationship,
        output_dir: str,
):
    packages = package_metadata_modular_optimizer_band_gap_gamess
    rel = rel_optimizer_band_gap
    logger.info(f"Relationship: {rel.json(exclude_none=True, exclude_unset=True, indent=2)}")

    transform = apis.runtime.package_transform.TransformRelationshipToDerivedPackage(rel.transform)

    derived_ve = transform.prepare_derived_package(
        "hello", parameterisation=apis.models.virtual_experiment.Parameterisation())
    transform.synthesize_derived_package(packages, derived_ve)
    logger.info(f"Resulting derived {json.dumps(derived_ve.dict(), indent=2)}")

    dir_persist = os.path.join(output_dir, "persist")
    platforms = packages.get_concrete_of_package("band-gap-dft-gamess-us:latest").platforms
    package = apis.runtime.package_derived.DerivedPackage(
        derived_ve, directory_to_place_derived=output_dir)
    package.synthesize(package_metadata=packages, platforms=platforms)
    package.persist_to_directory(dir_persist, packages)

    # VV: Ensure paths exist
    open(os.path.join(dir_persist, "conf", "flowir_package.yaml")).close()

    for x in [
        "rdkit_smiles2coordinates.py", "run-gamess.sh", "extract_gmsout_geo_opt.py",
        # VV: Surrogate binaries go here
        "smiles-to-xyz", "molecule-index", "optimizer", "generate-gamess"
    ]:
        open(os.path.join(dir_persist, 'bin', x)).close()

    for x in ["input_anion.txt", "input_cation.txt", "input_molecule.txt", "input_neutral.txt",
              "model-weights.checkpoint"]:
        open(os.path.join(dir_persist, 'data', x)).close()


def test_generate_parameterisation_for_derived_from_optimizer_and_bandgap(
        package_metadata_modular_optimizer_band_gap_gamess: apis.storage.PackageMetadataCollection,
        rel_optimizer_band_gap: apis.models.relationships.Relationship,
        output_dir: str,
):
    synthesize = apis.models.relationships.PayloadSynthesize()

    metadata = apis.kernel.relationships.synthesize_from_transformation(
        rel=rel_optimizer_band_gap,
        new_package_name="synthetic",
        packages=package_metadata_modular_optimizer_band_gap_gamess,
        db_experiments=apis.db.exp_packages.DatabaseExperiments(os.path.join(output_dir, "empty-database.txt")),
        synthesize=synthesize,
        update_experiments_database=False,
        path_multipackage=None)

    parameterisation = apis.models.virtual_experiment.parameterisation_from_flowir(metadata.metadata.concrete)
    all_vars = apis.models.virtual_experiment.characterize_variables(metadata.metadata.concrete)

    assert all_vars.multipleValues == {"backend"}

    assert len(parameterisation.executionOptions.variables) == 0
    assert len(parameterisation.presets.variables) == len(all_vars.uniqueValues)
    assert len(all_vars.uniqueValues) > 0
    assert all([x.name != "backend" for x in parameterisation.presets.variables])


def test_characterize_variables():
    flowir = """
variables:
  default:
    global:
      # VV: References python script in hooks directory to use for restartHook of GeometryOptimisation
      gamess-restart-hook-file: dft_restart.py
      gamess-image: nvcr.io/hpc/gamess:17.09-r2-libcchem
      gamess-command: bin/run-gamess.sh
      mem: '4295000000'
      backend: local
      number-processors: '8'
      startIndex: '0'
      numberMolecules: '1'
      basis: GBASIS=N31 NGAUSS=6 NDFUNC=2 NPFUNC=1 DIFFSP=.TRUE. DIFFS=.TRUE.
      functional: B3LYP
      collabel: label
      # VV: how long k8s should let the pod run before it sends it a SIGTERM
      gamess-walltime-minutes: 700
      # VV: how long k8s should wait between SIGTERM and SIGKILL
      gamess-grace-period-seconds: 1800
      gamess-gpus: 0
      gamess-version: "00"
    stages:
      0:
        stage-name: SMILES_to_GAMESS
      1:
        stage-name: GeometryOptimisationRun
  openshift:
    global:
      backend: kubernetes
  openshift-kubeflux:
    global:
      backend: kubernetes
  openshift-cpu:
    global:
      backend: kubernetes
      gamess-restart-hook-file: cpu_dft_restart.py
      gamess-image: docker-na-public.artifactory.swg-devops.com/res-st4sd-community-team-applications-docker-virtual/gamess-st4sd:2019.11.30
      gamess-command: rungms
      gamess-version: "01"
components:
- name: dummy
  command:
    executable: echo
    arguments: hello world
    """

    flowir = yaml.load(flowir, Loader=yaml.SafeLoader)
    concrete = experiment.model.frontends.flowir.FlowIRConcrete(flowir, 'default', {})
    all_vars = apis.models.virtual_experiment.characterize_variables(
        concrete, ['openshift', 'openshift-kubeflux', 'openshift-cpu'])

    assert all_vars.dict() == {
        'multipleValues': {
            'gamess-command',
            'gamess-image',
            'gamess-restart-hook-file',
            'gamess-version'},
        'platforms': ['openshift', 'openshift-kubeflux', 'openshift-cpu'],
        'uniqueValues': {
            'backend': 'kubernetes',
            'basis': 'GBASIS=N31 NGAUSS=6 NDFUNC=2 NPFUNC=1 DIFFSP=.TRUE. DIFFS=.TRUE.',
            'collabel': 'label',
            'functional': 'B3LYP',
            'gamess-gpus': 0,
            'gamess-grace-period-seconds': 1800,
            'gamess-walltime-minutes': 700,
            'mem': '4295000000',
            'number-processors': '8',
            'numberMolecules': '1',
            'startIndex': '0'}}


def validate_band_gap_optimizer_dsl_args(
        dsl: Dict[str, Any],
        package: apis.models.virtual_experiment.ParameterisedPackage,
        ve_modular_band_gap_gamess: apis.models.virtual_experiment.ParameterisedPackage,
):
    args = {k: v for (k, v) in dsl['entrypoint']['execute'][0]['args'].items()}

    assert args == {
        'backend': 'kubernetes',
        'basis': 'GBASIS=N31 NGAUSS=6 NDFUNC=2 NPFUNC=1 DIFFSP=.TRUE. DIFFS=.TRUE.',
        'collabel': 'label',
        'functional': 'B3LYP',
        'gamess-grace-period-seconds': '1800',
        'gamess-restart-hook-file': 'dft_restart.py',
        'gamess-walltime-minutes': '700',
        'mem': '4295000000',
        'number-processors': '1',
        'numberMolecules': '1',
        'startIndex': '0',
        'input.smiles.csv': 'input/smiles.csv',
        'data.input_molecule.txt': 'data/input_molecule.txt',
        'data.model-weights.checkpoint': 'data/model-weights.checkpoint',
    }

    final_executionOptions_variables = package.parameterisation.executionOptions.variables
    outputgraph_executionOptions_variables = ve_modular_band_gap_gamess.parameterisation.executionOptions.variables

    assert len(outputgraph_executionOptions_variables) > 0

    assert final_executionOptions_variables == outputgraph_executionOptions_variables


def test_preview_synthesize_dsl(
        package_metadata_modular_optimizer_band_gap_gamess: apis.storage.PackageMetadataCollection,
        rel_optimizer_band_gap: apis.models.relationships.Relationship,
        ve_modular_band_gap_gamess: apis.models.virtual_experiment.ParameterisedPackage,
        output_dir: str,
):
    packages = package_metadata_modular_optimizer_band_gap_gamess

    path_db_experiments = os.path.join(output_dir, "experiments.json")
    path_db_relationships = os.path.join(output_dir, "relationships.json")

    db_experiments = apis.db.exp_packages.DatabaseExperiments(path_db_experiments)
    db_relationships = apis.db.relationships.DatabaseRelationships(path_db_relationships)

    with db_relationships:
        db_relationships.insert_many([rel_optimizer_band_gap.dict()])

    with db_experiments:
        db_experiments.push_new_entry(ve_modular_band_gap_gamess)

    ret = apis.kernel.relationships.api_preview_synthesize_dsl(
        identifier=rel_optimizer_band_gap.identifier,
        packages=packages,
        db_relationships=db_relationships,
        db_experiments=db_experiments,
        dsl_version="2.0.0_0.1.0"
    )

    validate_band_gap_optimizer_dsl_args(ret.dsl, ret.package, ve_modular_band_gap_gamess)


def test_synthesize(
        package_metadata_modular_optimizer_band_gap_gamess: apis.storage.PackageMetadataCollection,
        rel_optimizer_band_gap: apis.models.relationships.Relationship,
        ve_modular_band_gap_gamess: apis.models.virtual_experiment.ParameterisedPackage,
        output_dir: str,
):
    packages = package_metadata_modular_optimizer_band_gap_gamess

    path_db_experiments = os.path.join(output_dir, "experiments.json")
    path_db_relationships = os.path.join(output_dir, "relationships.json")

    db_experiments = apis.db.exp_packages.DatabaseExperiments(path_db_experiments)
    db_relationships = apis.db.relationships.DatabaseRelationships(path_db_relationships)

    with db_relationships:
        db_relationships.insert_many([rel_optimizer_band_gap.dict()])

    with db_experiments:
        db_experiments.push_new_entry(ve_modular_band_gap_gamess)

    synthesize = apis.models.relationships.PayloadSynthesize()
    synthesize.options.generateParameterisation = True

    metadata = apis.kernel.relationships.api_synthesize_from_transformation(
        identifier=rel_optimizer_band_gap.identifier,
        new_package_name="synthetic",
        packages=packages,
        db_relationships=db_relationships,
        db_experiments=db_experiments,
        synthesize=synthesize,
        path_multipackage=os.path.join(output_dir, "synthesized")
    )

    logger.warning(f"The resulting parameterisation options are {metadata.package.parameterisation.json(indent=2)}")

    dsl = apis.models.virtual_experiment.dsl_from_concrete(
        concrete=metadata.metadata.concrete,
        manifest=metadata.metadata.manifestData,
        platform=(metadata.package.get_known_platforms() or ['default'])[0]
    )

    validate_band_gap_optimizer_dsl_args(dsl, metadata.package, ve_modular_band_gap_gamess)


def simple_push_and_synthesize(
        packages: apis.storage.PackageMetadataCollection,
        rel_optimizer_band_gap: apis.models.relationships.Relationship,
        ve_modular_band_gap_gamess: apis.models.virtual_experiment.ParameterisedPackage,
        db_experiments: apis.db.exp_packages.DatabaseExperiments,
        db_relationships: apis.db.relationships.DatabaseRelationships,
        new_package_name: str,
        output_dir: str,
):
    with db_relationships:
        db_relationships.insert_many([rel_optimizer_band_gap.dict()])

    with db_experiments:
        db_experiments.push_new_entry(ve_modular_band_gap_gamess)

    rel = apis.kernel.relationships.api_push_relationship(
        rel=rel_optimizer_band_gap,
        db_relationships=db_relationships,
        db_experiments=db_experiments,
        packages=packages
    )

    synthesize = apis.models.relationships.PayloadSynthesize()
    synthesize.options.generateParameterisation = True

    return apis.kernel.relationships.synthesize_from_transformation(
        rel=rel,
        new_package_name=new_package_name,
        packages=packages,
        db_experiments=db_experiments,
        synthesize=synthesize,
        update_experiments_database=True,
        path_multipackage=output_dir,
    )


def test_transformation_push_and_then_synthesize(
        package_metadata_modular_optimizer_band_gap_gamess: apis.storage.PackageMetadataCollection,
        rel_optimizer_band_gap: apis.models.relationships.Relationship,
        ve_modular_band_gap_gamess: apis.models.virtual_experiment.ParameterisedPackage,
        output_dir: str,
):
    path_db_experiments = os.path.join(output_dir, "experiments.json")
    path_db_relationships = os.path.join(output_dir, "relationships.json")

    db_experiments = apis.db.exp_packages.DatabaseExperiments(path_db_experiments)
    db_relationships = apis.db.relationships.DatabaseRelationships(path_db_relationships)

    metadata = simple_push_and_synthesize(
        packages=package_metadata_modular_optimizer_band_gap_gamess,
        rel_optimizer_band_gap=rel_optimizer_band_gap,
        ve_modular_band_gap_gamess=ve_modular_band_gap_gamess,
        db_experiments=db_experiments,
        db_relationships=db_relationships,
        new_package_name="synthetic",
        output_dir=output_dir,
    )

    with db_relationships:
        docs = db_relationships.query(db_relationships.construct_query(rel_optimizer_band_gap.identifier))

    assert len(docs) == 1

    with db_experiments:
        docs = db_experiments.query_identifier("synthetic")

    assert len(docs) == 1

    ve = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(docs[0])

    assert ve.metadata.registry.digest == metadata.package.metadata.registry.digest

    ve.parameterisation.presets.variables.append(
        apis.models.common.Option(name="numberMolecules", value="1000")
    )

    ve.update_digest()

    assert ve.metadata.registry.digest != metadata.package.metadata.registry.digest

    assert ve.base.to_digestable().dict() == metadata.package.base.to_digestable().dict()

    assert ve.get_packages_identifier() == metadata.package.get_packages_identifier()


def test_simple_relationship(
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage,
        package_metadata_simple: apis.storage.PackageMetadataCollection,
        rel_simple_relationship: Dict[str, Any],
        output_dir: str,
):
    rel = apis.models.relationships.Relationship.parse_obj(rel_simple_relationship)

    db_experiments = apis.db.exp_packages.DatabaseExperiments(os.path.join(output_dir, "experiments.txt"))
    db_relationships = apis.db.relationships.DatabaseRelationships(os.path.join(output_dir, "relationship.txt"))

    ve_fake_slow = ve_sum_numbers.copy(deep=True)
    ve_fake_slow.parameterisation.executionOptions.platform = ['default']
    ve_fake_slow.metadata.package.name = "simple-slow"

    ve_fake_fast = ve_sum_numbers.copy(deep=True)
    ve_fake_fast.parameterisation.executionOptions.platform = ['default']
    ve_fake_fast.metadata.package.name = "simple-fast"

    with db_experiments:
        db_experiments.push_new_entry(ve_fake_slow)
        db_experiments.push_new_entry(ve_fake_fast)

    rel = apis.kernel.relationships.push_relationship(
        rel=rel,
        db_relationships=db_relationships,
        db_experiments=db_experiments,
        packages=package_metadata_simple)

    logger.info(f"Updated relationship: {rel.json(indent=2)}")

    # VV: The 1-outputGraph components consume 1 component from outputGraph
    assert len(rel.transform.relationship.graphResults) == 1
    assert rel.transform.relationship.graphResults[0].inputGraphResult.name == "stage0.simulation:ref"
    assert rel.transform.relationship.graphResults[0].outputGraphResult.name == "stage0.simulation:ref"

    # VV: The inputGraph components consume 1 component from 1-outputGraph and 1 variable %(option)s

    input_params = {
        x.inputGraphParameter.name: x.outputGraphParameter.name for x in rel.transform.relationship.graphParameters
    }

    assert input_params == {
        "stage0.generate-inputs:output": "stage0.generate-inputs:output",
        "option": "option",
    }

    assert len(rel.transform.relationship.graphParameters) == 2

    synthesize = apis.models.relationships.PayloadSynthesize()
    synthesize.options.generateParameterisation = True

    metadata = apis.kernel.relationships.synthesize_from_transformation(
        rel=rel,
        new_package_name="synthetic",
        packages=package_metadata_simple,
        db_experiments=db_experiments,
        synthesize=synthesize,
        update_experiments_database=True,
        path_multipackage=output_dir,
    )

    concrete = metadata.metadata.concrete

    comp = concrete.get_component((0, 'simulation'))
    assert comp['command']['arguments'] == "fast simulation of stage0.generate-inputs:output %(option)s"

    all_vars = concrete.get_platform_variables()

    assert all_vars['global']['option'] == 'FROM_FOUNDATION'

    explained = apis.runtime.package_derived.explain_choices_in_derived(
        ve=metadata.package, packages=package_metadata_simple)

    assert explained.dict() == {
        'variables': {
            'option': {
                'fromBasePackage': 'simple-slow:latest',
                'preset': 'FROM_FOUNDATION',
                'values': [
                    {
                        'overrides': [
                            {
                                'fromBasePackage': 'simple-fast:latest',
                                'value': 'FROM_SURROGATE'
                            }
                        ],
                        'platform': 'default',
                        'value': 'FROM_FOUNDATION'
                    }]
            }
        }
    }


def test_simple_relationship_with_variables(
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage,
        package_metadata_simple: apis.storage.PackageMetadataCollection,
        rel_simple_relationship: Dict[str, Any],
        output_dir: str,
):
    rel = apis.models.relationships.Relationship.parse_obj(rel_simple_relationship)

    rel.transform.relationship.graphParameters.append(
        apis.models.relationships.RelationshipParameters(
            inputGraphParameter=apis.models.relationships.GraphValue(name="option"),
            outputGraphParameter=apis.models.relationships.GraphValue(value="use foundation %(option)s")
        )
    )

    db_experiments = apis.db.exp_packages.DatabaseExperiments(os.path.join(output_dir, "experiments.txt"))
    db_relationships = apis.db.relationships.DatabaseRelationships(os.path.join(output_dir, "relationship.txt"))

    ve_fake_slow = ve_sum_numbers.copy(deep=True)
    ve_fake_slow.parameterisation.executionOptions.platform = ['default']
    ve_fake_slow.metadata.package.name = "simple-slow"

    ve_fake_fast = ve_sum_numbers.copy(deep=True)
    ve_fake_fast.parameterisation.executionOptions.platform = ['default']
    ve_fake_fast.metadata.package.name = "simple-fast"

    with db_experiments:
        db_experiments.push_new_entry(ve_fake_slow)
        db_experiments.push_new_entry(ve_fake_fast)

    rel = apis.kernel.relationships.push_relationship(
        rel=rel,
        db_relationships=db_relationships,
        db_experiments=db_experiments,
        packages=package_metadata_simple)

    logger.info(f"Updated relationship: {rel.json(indent=2)}")

    # VV: The 1-outputGraph components consume 1 component from outputGraph
    assert len(rel.transform.relationship.graphResults) == 1
    assert rel.transform.relationship.graphResults[0].inputGraphResult.name == "stage0.simulation:ref"
    assert rel.transform.relationship.graphResults[0].outputGraphResult.name == "stage0.simulation:ref"

    # VV: The inputGraph components consume 1 component from 1-outputGraph and 1 variable wrapped inside some text
    parameters = {
        x.inputGraphParameter.name: x.outputGraphParameter.dict() for x in rel.transform.relationship.graphParameters
    }

    assert parameters == {
        'option': {'value': 'use foundation %(option)s'},
        'stage0.generate-inputs:output': {'name': 'stage0.generate-inputs:output'}
    }
    assert len(rel.transform.relationship.graphParameters) == 2

    synthesize = apis.models.relationships.PayloadSynthesize()
    synthesize.options.generateParameterisation = True

    metadata = apis.kernel.relationships.synthesize_from_transformation(
        rel=rel,
        new_package_name="synthetic",
        packages=package_metadata_simple,
        db_experiments=db_experiments,
        synthesize=synthesize,
        update_experiments_database=True,
        path_multipackage=output_dir,
    )

    concrete = metadata.metadata.concrete

    comp = concrete.get_component((0, 'simulation'))
    assert comp['command']['arguments'] == "fast simulation of stage0.generate-inputs:output use foundation %(option)s"

    all_vars = concrete.get_platform_variables()

    assert all_vars['global']['option'] == 'FROM_FOUNDATION'

    explained = apis.runtime.package_derived.explain_choices_in_derived(
        ve=metadata.package, packages=package_metadata_simple)

    assert explained.dict() == {
        'variables': {
            'option': {
                'fromBasePackage': 'simple-slow:latest',
                'preset': 'FROM_FOUNDATION',
                'values': [
                    {
                        'overrides': [
                            {
                                'fromBasePackage': 'simple-fast:latest',
                                'value': 'FROM_SURROGATE'
                            }
                        ],
                        'platform': 'default',
                        'value': 'FROM_FOUNDATION'
                    }]
            }
        }
    }


def test_surface_invalid_transformation_dangling_component(
        package_metadata_modular_optimizer_band_gap_gamess: apis.storage.PackageMetadataCollection,
        rel_optimizer_band_gap: apis.models.relationships.Relationship,
        ve_modular_band_gap_gamess: apis.models.virtual_experiment.ParameterisedPackage,
        output_dir: str,
):
    path_db_experiments = os.path.join(output_dir, "experiments.json")
    path_db_relationships = os.path.join(output_dir, "relationships.json")

    db_experiments = apis.db.exp_packages.DatabaseExperiments(path_db_experiments)
    db_relationships = apis.db.relationships.DatabaseRelationships(path_db_relationships)

    # VV: Remove a component and forget to specify how to fill in the hole left behind
    rel_optimizer_band_gap.transform.outputGraph.components = ["stage0.XYZToGAMESS", "stage1.GeometryOptimisation"]

    with pytest.raises(apis.models.errors.TransformationManyErrors) as e:
        metadata = simple_push_and_synthesize(
            packages=package_metadata_modular_optimizer_band_gap_gamess,
            rel_optimizer_band_gap=rel_optimizer_band_gap,
            ve_modular_band_gap_gamess=ve_modular_band_gap_gamess,
            db_experiments=db_experiments,
            db_relationships=db_relationships,
            new_package_name="synthetic",
            output_dir=output_dir,
        )

    exc = e.value
    assert len(exc.problems) == 2

    assert isinstance(exc.problems[0], experiment.model.errors.FlowIRUnknownReferenceInArguments)
    assert isinstance(exc.problems[1], experiment.model.errors.FlowIRReferenceToUnknownComponent)

    unknown_ref_vars: experiment.model.errors.FlowIRUnknownReferenceInArguments = exc.problems[0]
    unknown_comp: experiment.model.errors.FlowIRReferenceToUnknownComponent = exc.problems[1]

    assert unknown_ref_vars.ref_unknown == "GeometryOptimisation:ref"
    assert unknown_ref_vars.stage == 1
    assert unknown_ref_vars.component == "ExtractEnergies"

    assert unknown_comp.references == ["stage1.GeometryOptimisation"]
