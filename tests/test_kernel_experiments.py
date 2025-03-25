# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


import json
import logging
import tempfile

import apis.db.exp_packages
import apis.db.relationships
import apis.kernel.experiments
import apis.models.common
import apis.models.errors
import apis.models.query_experiment
import apis.models.relationships
import apis.models.virtual_experiment
import apis.runtime.package_transform
import apis.storage

logger = logging.getLogger('krnl_exp')


def test_query_pveps_for_base_packages(
        ve_homo_lumo_dft_gamess_us: apis.models.virtual_experiment.ParameterisedPackage,
        output_dir: str,
):
    query = apis.models.query_experiment.QueryExperiment.model_validate({
        "common": {
            "matchPackageVersion": False
        },
        "package": {
            "definition": {
                "source": {
                    "git": {
                        "location": {
                            "url": "https://github.ibm.com/st4sd-contrib-experiments/homo-lumo-dft.git",
                            "branch": "main",
                        }
                    }
                },
                "config": {
                    "path": "dft/homo-lumo-dft.yaml",
                    "manifestPath": "dft/manifest.yaml"
                }
            }
        }
    })

    # VV: make sure that what we'll add in the DB has the correct branch, and version
    ve_homo_lumo_dft_gamess_us.base.packages[0].source.git.location.branch = "main"

    ve_other_branch = ve_homo_lumo_dft_gamess_us.model_copy(deep=True)
    ve_other_branch.metadata.package.name = "other branch"
    ve_other_branch.base.packages[0].source.git.location.branch = "not main"

    # VV: Generate a PVEP that looks very similar to the one we care for
    ve_semi_empirical = ve_homo_lumo_dft_gamess_us.model_copy(deep=True)
    ve_semi_empirical.metadata.package.name = "semi-empirical"
    ve_semi_empirical.base.packages[0].config.path = "semi-empirical/semi-empirical.yml"
    ve_semi_empirical.base.packages[0].config.manifestPath = "semi-empirical/manifest.yml"

    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            db.push_new_entry(ve_homo_lumo_dft_gamess_us)
            db.push_new_entry(ve_semi_empirical)
            db.push_new_entry(ve_other_branch)

            assert len(db.query()) == 3

            docs = apis.kernel.experiments.api_query_experiments(query, db, None)

            for x in docs:
                print(json.dumps(x['metadata']['package'], indent=2))

            assert len(docs) == 1
            assert docs[0]['metadata']['package']['name'] == ve_homo_lumo_dft_gamess_us.metadata.package.name


def test_query_pveps_for_base_packages_with_versioning(
        ve_homo_lumo_dft_gamess_us: apis.models.virtual_experiment.ParameterisedPackage,
        output_dir: str,
):
    query = apis.models.query_experiment.QueryExperiment.model_validate({
        "common": {
            "matchPackageVersion": True
        },
        "package": {
            "definition": {
                "source": {
                    "git": {
                        "location": {
                            "url": "https://github.ibm.com/st4sd-contrib-experiments/homo-lumo-dft.git",
                        },
                        "version": "special version"
                    }
                },
                "config": {
                    "path": "dft/homo-lumo-dft.yaml",
                    "manifestPath": "dft/manifest.yaml"
                }
            }
        }
    })

    # VV: make sure that what we'll add in the DB has a branch with the name "main"
    ve_homo_lumo_dft_gamess_us.base.packages[0].source.git.version = "special version"

    # VV: Generate a PVEP that looks very similar to the one we care for but has a different version
    ve_other_version = ve_homo_lumo_dft_gamess_us.model_copy(deep=True)
    ve_other_version.parameterisation.presets.runtime.args.append('--makethisunique')
    ve_other_version.base.packages[0].source.git.version = "do not care about this version"

    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            db.push_new_entry(ve_homo_lumo_dft_gamess_us)
            db.push_new_entry(ve_other_version)

            assert len(db.query()) == 2

            docs = apis.kernel.experiments.api_query_experiments(query, db, None)

            for x in docs:
                print(json.dumps(x['metadata']['package'], indent=2))

            assert len(docs) == 1
            assert docs[0]['metadata']['package']['name'] == ve_homo_lumo_dft_gamess_us.metadata.package.name


def test_query_pveps_for_base_packages_no_results(
        ve_homo_lumo_dft_gamess_us: apis.models.virtual_experiment.ParameterisedPackage,
        output_dir: str,
):
    query = apis.models.query_experiment.QueryExperiment.model_validate({
        "package": {
            "definition": {
                "source": {
                    "git": {
                        "location": {
                            "url": "https://github.ibm.com/st4sd-contrib-experiments/homo-lumo-dft.git"
                        }
                    }
                },
                "config": {
                    "path": "special/special.yaml"
                }
            }
        }
    })

    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            db.push_new_entry(ve_homo_lumo_dft_gamess_us)
            ve_homo_lumo_dft_gamess_us.base.packages[0].config.path = "semi-empirical/semi-empirical.yml"
            ve_homo_lumo_dft_gamess_us.base.packages[0].config.manifestPath = "semi-empirical/manifest.yml"
            db.push_new_entry(ve_homo_lumo_dft_gamess_us)

            docs = apis.kernel.experiments.api_query_experiments(query, db, None)

            assert len(docs) == 0


def test_query_synthesized_with_package(
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
    packages_metadata = homolumogamess_ani_package_metadata

    query = apis.models.query_experiment.QueryExperiment.model_validate({
        "package": {
            "definition": {
                "source": {
                    "git": {
                        "location": {
                            "url": "https://github.ibm.com/st4sd-contrib-experiments/homo-lumo-dft.git"
                        }
                    }
                },
                "config": {
                    "path": "dft/homo-lumo-dft.yaml",
                    "manifestPath": "dft/manifest.yaml"
                }
            }
        }
    })

    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with tempfile.NamedTemporaryFile(suffix='.json', prefix='relationships', delete=True) as g:
            with apis.db.exp_packages.DatabaseExperiments(f.name) as db_exps:
                db_exps.push_new_entry(ve_homo_lumo_dft_gamess_us)
                db_exps.push_new_entry(ve_configuration_generator_ani)
                multi.discover_parameterised_packages(db_exps)

            with apis.db.exp_packages.DatabaseExperiments(f.name) as db_exps:
                multi.discover_parameterised_packages(db_exps)
                derived_ve = multi.prepare_derived_package("hello", apis.models.virtual_experiment.Parameterisation(
                    presets=apis.models.virtual_experiment.ParameterisationPresets(platform="openshift")))
                multi.synthesize_derived_package(packages_metadata, derived_ve)
                db_exps.push_new_entry(derived_ve)

            with apis.db.relationships.DatabaseRelationships(g.name) as db_rels:
                rel = apis.models.relationships.Relationship(identifier="homo-lumo-ani", transform=transform)
                db_rels.upsert(rel.model_dump(exclude_none=False), ql=db_rels.construct_query(rel.identifier))

            query.common.mustHaveOnePackage = False
            docs = apis.kernel.experiments.api_query_experiments(
                query, db_experiments=db_exps, db_relationships=db_rels)

            # VV: First identify any pvep with at least 1 base package
            assert len(docs) == 2

            many_base_packages = [x for x in docs if len(x['base']['packages']) > 1]
            assert len(many_base_packages) == 1

            assert many_base_packages[0]['metadata']['package']['name'] == "hello"
            assert 'auto-generated' in many_base_packages[0]['metadata']['package']['keywords']

            # VV: now only find those which contain exactly 1 base package
            query.common.mustHaveOnePackage = True
            docs = apis.kernel.experiments.api_query_experiments(
                query, db_experiments=db_exps, db_relationships=db_rels)

            assert len(docs) == 1


def test_query_synthesized_with_relationship(
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
    packages_metadata = homolumogamess_ani_package_metadata

    query = apis.models.query_experiment.QueryExperiment.model_validate({
        "relationship": {
            "identifier": "homo-lumo-ani",
            "transform": {
                "matchOutputGraph": True
            }
        }
    })

    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with tempfile.NamedTemporaryFile(suffix='.json', prefix='relationships', delete=True) as g:
            with apis.db.exp_packages.DatabaseExperiments(f.name) as db_exps:
                db_exps.push_new_entry(ve_homo_lumo_dft_gamess_us)
                db_exps.push_new_entry(ve_configuration_generator_ani)
                multi.discover_parameterised_packages(db_exps)

            with apis.db.exp_packages.DatabaseExperiments(f.name) as db_exps:
                multi.discover_parameterised_packages(db_exps)
                derived_ve = multi.prepare_derived_package("hello", apis.models.virtual_experiment.Parameterisation(
                    presets=apis.models.virtual_experiment.ParameterisationPresets(platform="openshift")))
                multi.synthesize_derived_package(packages_metadata, derived_ve)
                db_exps.push_new_entry(derived_ve)

            with apis.db.relationships.DatabaseRelationships(g.name) as db_rels:
                rel = apis.models.relationships.Relationship(identifier="homo-lumo-ani", transform=transform)
                db_rels.upsert(rel.model_dump(exclude_none=False), ql=db_rels.construct_query(rel.identifier))

            query.common.mustHaveOnePackage = False
            docs = apis.kernel.experiments.api_query_experiments(
                query, db_experiments=db_exps, db_relationships=db_rels)

            # VV: First identify any pvep with at least 1 base package
            assert len(docs) == 2

            many_base_packages = [x for x in docs if len(x['base']['packages']) > 1]
            assert len(many_base_packages) == 1

            assert many_base_packages[0]['metadata']['package']['name'] == "hello"
            assert 'auto-generated' in many_base_packages[0]['metadata']['package']['keywords']

            # VV: now only find those which contain exactly 1 base package
            query.common.mustHaveOnePackage = True
            docs = apis.kernel.experiments.api_query_experiments(
                query, db_experiments=db_exps, db_relationships=db_rels)

            assert len(docs) == 1
