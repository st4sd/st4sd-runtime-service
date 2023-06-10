# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


import logging
import os

import yaml

import apis.models.virtual_experiment
import apis.runtime.package_derived
import apis.storage
import tests.conftest

logger = logging.getLogger('td')

package_from_flowir = tests.conftest.package_from_flowir
package_from_files = tests.conftest.package_from_files


def test_load_surrogate(derived_ve: apis.models.virtual_experiment.ParameterisedPackage):
    assert len(derived_ve.base.packages) == 2

    expensive = [x for x in derived_ve.base.packages if x.name == "expensive"][0]
    surrogate = [x for x in derived_ve.base.packages if x.name == "surrogate"][0]

    assert len(expensive.graphs) == 1
    assert len(surrogate.graphs) == 1

    assert expensive.graphs[0].name == "prologue-epilogue"
    assert surrogate.graphs[0].name == "simulation"


def test_conflict_discovery():
    conflicts = apis.runtime.package_derived.PackageConflict.find_conflicts({
        'expensive': {
            'platforms': {'default': {'global': {}, 'stages': {}}, 'openshift': {
                'global': {'numberMolecules': '1', 'backend': 'kubernetes', 'T': '298.15', 'number-processors': '1',
                           'force_tolerance': '0.005', 'P': '101325.0', 'max_opt_steps': '5000', 'rep_key': 'smiles',
                           'startIndex': '0', 'optimizer': 'bfgs'}, 'stages': {0: {}}}}},
        'surrogate': {
            'platforms': {'default': {'global': {}, 'stages': {}}, 'openshift': {
                'global': {'numberMolecules': '1', 'ani_minimize_all_conformers': '0', 'backend': 'kubernetes',
                           'ff_minimize': '1', 'T': '298.15', 'defaultq': 'normal', 'number-processors': '1',
                           'force_tolerance': '0.05', 'P': '101325.0', 'max_opt_steps': '5000', 'rep_key': 'smiles',
                           'startIndex': '0', 'optimizer': 'bfgs', 'ani_model': 'ani2x'}, 'stages': {0: {}}}}}})

    assert conflicts[0].location == ["platforms", "openshift", "global", "force_tolerance"]
    assert conflicts[0].get_package('expensive').value == "0.005"
    assert conflicts[0].get_package('surrogate').value == "0.05"


def test_extract_graphs_and_metadata(
        derived_ve: apis.models.virtual_experiment.ParameterisedPackage,
        output_dir: str,
        flowir_psi4: str,
        flowir_neural_potential
):
    expensive = package_from_flowir(
        flowir_psi4, location=os.path.join(output_dir, "expensive"),
        extra_files={
            'data/smiles.csv': 'expensive',
            'bin/aggregate_energies.py': 'expensive',
            'bin/optimize_ff.py': 'expensive',
            'bin/optimize_psi4.py': 'expensive',
        },
        platform="openshift"
    )

    surrogate = package_from_flowir(
        flowir_neural_potential, location=os.path.join(output_dir, "surrogate"),
        extra_files={
            'data/smiles.csv': "surrogate",
            'bin/optimize_ani.py': "surrogate",
            'bin/aggregate_energies.py': 'surrogate',
            'bin/optimize_ff.py': 'surrogate',
        }
    )

    # VV: The above FlowIRs have errors so I should hit an exception at some point which will tell me how
    # to fix the problems

    package = apis.runtime.package_derived.DerivedPackage(derived_ve)
    graph_meta = package.extract_graphs(
        apis.storage.PackageMetadataCollection({
            'expensive': apis.models.virtual_experiment.StorageMetadata(
                location=expensive.location, concrete=expensive.configuration.get_flowir_concrete(),
                manifestData=expensive.configuration.manifestData, data=[]
            ),
            'surrogate': apis.models.virtual_experiment.StorageMetadata(
                location=surrogate.location, concrete=surrogate.configuration.get_flowir_concrete(),
                manifestData=surrogate.configuration.manifestData, data=[]
            )
        }),
        platforms=['openshift']
    )
    # VV: The important one is `force_tolerance` because its value (0.05) is the one in surrogate,
    # expensive has a different tolerance (0.005)
    aggregate_variables = graph_meta.aggregate_variables.dict(exclude_none=True)
    assert sorted(aggregate_variables['platforms']) == sorted(['default', 'openshift'])

    assert graph_meta.aggregate_blueprints.dict(exclude_none=True) == {
        'platforms': {
            'openshift': {'global': {}, 'stages': {0: {}}},
            'default': {'global': {}, 'stages': {0: {}}}
        }}

    comps = {f"stage{x.get('stage', 0)}.{x['name']}": x for x in graph_meta.aggregate_components}

    assert sorted(comps.keys()) == [
        'stage0.AniOptimize', 'stage0.ForceFieldOptANIUncertainty',
        'stage0.GetMoleculeIndex', 'stage0.aggregateEnergies']

    assert sorted(comps['stage0.AniOptimize']['references']) == [
        'input/smiles.csv:copy',
        'stage0.ForceFieldOptANIUncertainty/optimized.xyz:ref',
        'stage0.GetMoleculeIndex:output']

    assert comps['stage0.AniOptimize']['command']['arguments'] == (
        '-xyz stage0.ForceFieldOptANIUncertainty/optimized.xyz:ref  -rk %(rep_key)s '
        '-ri stage0.GetMoleculeIndex:output --ani_model %(ani_model)s -o '
        '%(optimizer)s -i %(max_opt_steps)s --temperature %(T)s --pressure %(P)s '
        '--force_tolerance %(force_tolerance)s --ff_minimize 0 -amac 0')

    assert graph_meta.aggregate_variables.platforms['default'].vGlobal == {
        'P': '101325.0',
        'T': '298.15',
        'ani_model': 'ani2x',
        'backend': 'local',
        'force_tolerance': '0.05',
        'max_opt_steps': '5000',
        'numberMolecules': '1',
        'optimizer': 'bfgs',
        'rep_key': 'smiles',
        'startIndex': '0'}

    assert graph_meta.aggregate_variables.platforms['openshift'].vGlobal == {"backend": "kubernetes"}

    for x in graph_meta.aggregate_variables.platforms:
        assert graph_meta.aggregate_variables.platforms[x].stages == {0: {}}


def test_synthesize_derived(
        derived_ve: apis.models.virtual_experiment.ParameterisedPackage,
        output_dir: str,
        flowir_psi4: str,
        flowir_neural_potential
):
    expensive = package_from_flowir(
        flowir_psi4, location=os.path.join(output_dir, "expensive"),
        extra_files={
            'data/smiles.csv': 'expensive',
            'bin/aggregate_energies.py': 'expensive',
            'bin/optimize_ff.py': 'expensive',
            'bin/optimize_psi4.py': 'expensive',
        },
        platform="openshift"
    )

    surrogate = package_from_flowir(
        flowir_neural_potential, location=os.path.join(output_dir, "surrogate"),
        extra_files={
            'data/smiles.csv': "surrogate",
            'bin/optimize_ani.py': "surrogate",
            'bin/aggregate_energies.py': 'surrogate',
            'bin/optimize_ff.py': 'surrogate',
        }
    )

    package = apis.runtime.package_derived.DerivedPackage(derived_ve)
    package.synthesize(apis.storage.PackageMetadataCollection({
        'expensive': apis.models.virtual_experiment.StorageMetadata(
            location=expensive.location, concrete=expensive.configuration.get_flowir_concrete(),
            manifestData=expensive.configuration.manifestData, data=[]
        ),
        'surrogate': apis.models.virtual_experiment.StorageMetadata(
            location=surrogate.location, concrete=surrogate.configuration.get_flowir_concrete(),
            manifestData=surrogate.configuration.manifestData, data=[]
        )
    }),
        platforms=['openshift']
    )


def test_synthesize_gamess_homo_lumo_dft_and_ANI(
        homolumogamess_ani_package_metadata: apis.storage.PackageMetadataCollection,
        derived_ve_gamess_homo_dft_ani: apis.models.virtual_experiment.ParameterisedPackage,
        output_dir: str,
):
    derived_ve = derived_ve_gamess_homo_dft_ani
    known_platforms = derived_ve.get_known_platforms() or ['default']

    assert known_platforms[0] == 'openshift'

    packages_metadata = homolumogamess_ani_package_metadata

    many = packages_metadata.get_all_package_metadata()
    for x in many:
        many[x].discover_data_files()

    assert packages_metadata.get_datafiles_of_package("homo-lumo-dft-gamess-us:latest") == sorted([
        'input_cation.txt', 'input_anion.txt', 'input_molecule.txt', 'input_neutral.txt'])

    assert packages_metadata. \
               get_datafiles_of_package("configuration-generator-ani-gamess:latest") == []

    package = apis.runtime.package_derived.DerivedPackage(
        derived_ve, directory_to_place_derived=output_dir)
    package.synthesize(packages_metadata, platforms=['openshift'])

    logger.info(f"IncludePaths")
    for x in derived_ve.base.includePaths:
        logger.info(x.json(exclude_none=True, indent=2))

    dir_persist = os.path.join(output_dir, "persist")
    package.persist_to_directory(dir_persist, packages_metadata)


def test_simple_reference_with_reference():
    conf = yaml.load("""
    name: GeometryOptimisation
    command:
      arguments: molecule.inp %(gamess-version-number)s %(number-processors)s
      environment: gamess
      executable: rungms
    references:
    - stage0.AnionSMILESToGAMESSInput/molecule.inp:copy
    """, Loader=yaml.SafeLoader)

    instruction = apis.runtime.package_derived.InstructionRewireSymbol(
        source=apis.runtime.package_derived.RewireSymbol(
            reference="stage0.GeometryGenerator/molecule.inp:copy"),
        destination=apis.runtime.package_derived.RewireSymbol(
            reference="stage0.AnionSMILESToGAMESSInput/molecule.inp:copy")
    )

    instruction.apply_to_component(conf)

    assert conf['references'] == ["stage0.GeometryGenerator/molecule.inp:copy"]
    assert conf['command']['arguments'] == "molecule.inp %(gamess-version-number)s %(number-processors)s"


def test_simple_reference_with_text():
    conf = yaml.load("""
    name: GeometryOptimisation
    command:
      arguments: stage0.AnionSMILESToGAMESSInput/molecule.inp:ref %(gamess-version-number)s %(number-processors)s
      environment: gamess
      executable: rungms
    references:
    - stage0.AnionSMILESToGAMESSInput/molecule.inp:ref
    """, Loader=yaml.SafeLoader)

    instruction = apis.runtime.package_derived.InstructionRewireSymbol(
        source=apis.runtime.package_derived.RewireSymbol(
            text="hello %(world)s"),
        destination=apis.runtime.package_derived.RewireSymbol(
            reference="stage0.AnionSMILESToGAMESSInput/molecule.inp:ref")
    )

    instruction.apply_to_component(conf)

    assert conf['references'] == []
    assert conf['command']['arguments'] == "hello %(world)s %(gamess-version-number)s %(number-processors)s"
