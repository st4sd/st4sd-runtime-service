import logging
import os

import pytest
import requests

import apis.db.exp_packages
import apis.db.relationships
import apis.db.secrets
import apis.kernel.relationships
import apis.models.constants
import apis.models.relationships
import apis.models.virtual_experiment
import apis.storage

logger = logging.getLogger('TEST')

real_packages = pytest.mark.skipif("not config.getoption('real_packages')")


def add_from_github(
        url: str, db_experiments: apis.db.exp_packages.DatabaseExperiments
) -> apis.models.virtual_experiment.ParameterisedPackage:
    r = requests.get(url, allow_redirects=True)
    ve_raw = r.json()
    ve = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(ve_raw)

    with db_experiments:
        db_experiments.push_new_entry(ve)

    return ve


def add_band_gap_dft(
        db_experiments: apis.db.exp_packages.DatabaseExperiments
) -> apis.models.virtual_experiment.ParameterisedPackage:
    url = "https://raw.githubusercontent.com/st4sd/band-gap-gamess/main/dft/parameterised-packages/gamess_us.json"
    return add_from_github(url, db_experiments)


def add_band_gap_pm3(
        db_experiments: apis.db.exp_packages.DatabaseExperiments
) -> apis.models.virtual_experiment.ParameterisedPackage:
    url = "https://raw.githubusercontent.com/st4sd/band-gap-gamess/main/semi-empirical/" \
          "parameterised-packages/se_pm3.json"
    return add_from_github(url, db_experiments)


@real_packages
def test_transformation_pm3_to_dft(output_dir: str, local_deployment: bool):
    rel_raw = {
        "identifier": "pm3-to-dft",
        "description": "Configure GAMESS-US to use PM3 instead of DFT",
        "transform": {
            "inputGraph": {
                "identifier": "band-gap-pm3-gamess-us:latest",
                "components": [
                    "stage0.SetBasis",
                    "stage0.XYZToGAMESS",
                    "stage1.GeometryOptimisation"
                ]
            },
            "outputGraph": {
                "identifier": "band-gap-dft-gamess-us:latest",
                "components": [
                    "stage0.SetBasis",
                    "stage0.SetFunctional",
                    "stage0.XYZToGAMESS",
                    "stage1.GeometryOptimisation"
                ]
            }
        }
    }

    relationship: apis.models.relationships.Relationship = apis.models.relationships.Relationship.parse_obj(rel_raw)

    logger.info(f"Original relationship {relationship.json(indent=2, exclude_none=True)}")

    db_experiments = apis.db.exp_packages.DatabaseExperiments(os.path.join(output_dir, "exps.txt"))
    db_relationships = apis.db.relationships.DatabaseRelationships(os.path.join(output_dir, "relationships.txt"))
    db_secrets = apis.db.secrets.DatabaseSecrets(os.path.join(output_dir, "secrets.txt"))

    _ = add_band_gap_dft(db_experiments)
    _ = add_band_gap_pm3(db_experiments)

    apis.kernel.relationships.push_relationship(
        rel=relationship,
        db_experiments=db_experiments,
        db_relationships=db_relationships,
        packages=apis.storage.PackagesDownloader(ve=None, db_secrets=db_secrets),
    )

    rel_expanded = apis.kernel.relationships.get_relationship(relationship.identifier, db_relationships)

    logger.info(f"Expanded relationship {rel_expanded.json(indent=2, exclude_none=True)}")

    params = {
        x.inputGraphParameter.name: x.outputGraphParameter.name
        for x in rel_expanded.transform.relationship.graphParameters
    }

    # VV: These are parameters that the inputGraph consumes from 1-outputGraph
    assert params == {
        # stage0.SetBasis dependencies has a dependency to a data file but that will be auto-copied from the surrogate

        # stage0.XYZToGAMESS dependencies
        "stage0.SMILESToXYZ:ref": "stage0.SMILESToXYZ:ref",
        "stage0.GetMoleculeIndex:output": "stage0.GetMoleculeIndex:output",

        # stage1.GeometryOptimisation has no dependencies to 1-outputGraph
    }

    # VV: Double check that there are no duplicat graphParameters
    assert len(rel_expanded.transform.relationship.graphParameters) == len(params)

    results = {
        x.outputGraphResult.name: x.inputGraphResult.name
        for x in rel_expanded.transform.relationship.graphResults
    }

    # VV: Dependencies in 1-outputGraph to the outputGraph components
    assert results == {
        # Single reference to stage0.SetBasis is in stage0.SetFunctional - comp in outputGraph and therefore removed

        # Single reference to stage0.SetFunctional is in stage0.XYZToGAMESS - comp in outputGraph and therefore removed

        # Single reference to stage0.XYZToGamess is in stage1.GeometryOptimisation
        #                               - comp in outputGraph and therefore removed

        # There is a references to stage1.GeometryOptimisation in stage1.ExtractEnergies
        "stage1.GeometryOptimisation:ref": "stage1.GeometryOptimisation:ref"
    }

    assert len(rel_expanded.transform.relationship.graphResults) == len(results)
    db_secrets = apis.db.secrets.DatabaseSecrets(os.path.join(output_dir, "secrets.txt"))

    dir_path_multipackage = os.path.join(output_dir, "download-packages")
    metadata = apis.kernel.relationships.synthesize_from_transformation(
        rel=rel_expanded,
        new_package_name="synthetic",
        packages=apis.storage.PackagesDownloader(ve=None, db_secrets=db_secrets),
        db_experiments=db_experiments,
        synthesize=apis.models.relationships.PayloadSynthesize(),
        update_experiments_database=True,
        path_multipackage=dir_path_multipackage
    )

    logger.info(f"Digest of synthesized is {metadata.package.metadata.registry.digest}")
    logger.info(f"Its base-packages identifier is {metadata.package.get_packages_identifier()}")

    with db_experiments:
        docs = db_experiments.query_identifier("synthetic")

    assert len(docs) == 1

    ve = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(docs[0])

    assert ve.get_packages_identifier() == metadata.package.get_packages_identifier()

    dir_package = os.path.join(dir_path_multipackage, "synthetic", ve.get_packages_identifier())
    logger.info(f"Package is at {dir_package}")

    assert os.path.isdir(dir_package)
