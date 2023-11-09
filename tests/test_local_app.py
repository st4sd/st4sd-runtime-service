# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Theo Kanakis
#   Vassilis Vassiliadis

from __future__ import annotations

import logging
import subprocess
import sys
import time
import socket
import os
import typing

import requests
import experiment.service.db
import experiment.service.errors
import pytest
import contextlib

import apis.models.relationships

from .test_library import simple_dsl2

logger = logging.getLogger("tapp")

rest_api = pytest.mark.skipif("not config.getoption('rest_api')")


@pytest.fixture(scope="function")
def initialise_api(output_dir) -> int:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(('', 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        port = s.getsockname()[1]

    logger.info(f"initialise_api on port {port}")
    pid_path = os.path.join(output_dir, 'webserver.pid')

    curr_env = os.environ.copy()
    curr_env['LOG_DIR'] = os.path.join(output_dir, "logs")
    process = subprocess.Popen(
        [
            "gunicorn",
            "--bind",
            f"localhost:{port}",
            "app:app",
            f"-p {pid_path}",
            f"-e GUNICORN_PID_PATH={pid_path}",
            "-e LOCAL_DEPLOYMENT=True",
            f"-e LOCAL_STORAGE={output_dir}",
            f"-e S3_ROOT_GRAPH_LIBRARY={output_dir}/library",
            "--timeout=120",
            "--threads=1",
            "--keep-alive=3",
        ],
        stdout=sys.stdout,
        stderr=sys.stderr,
        env=curr_env
    )

    # VV: Wait for gunicorn to start serving requests
    logger.info("Waiting for REST api to begin serving requests")

    time.sleep(1)

    try:
        started = time.time()
        while True:
            try:
                requests.get(f"http://localhost:{port}")
                break
            except requests.exceptions.ConnectionError:
                time.sleep(0.1)

            if time.time() - started > 30:
                raise ValueError("Unable to start webserver")

        yield port
    finally:
        process.kill()


@pytest.fixture()
def api_wrapper(initialise_api: int) -> experiment.service.db.ExperimentRestAPI:
    try:
        api = experiment.service.db.ExperimentRestAPI(
            f"http://localhost:{initialise_api}/",
            cdb_registry_url=None,
            cdb_rest_url=None,
            max_retries=2,
            secs_between_retries=1,
            test_cdb_connection=False,
        )

        return api
    except Exception as e:
        logger.warning(f"Could not initialize api_wrapper")
        raise e


@pytest.fixture
def dummy_payload():
    def _build_payload(name: str):
        return {
            "base": {
                "packages": [
                    {
                        "source": {
                            "git": {
                                "location": {
                                    "url": "https://github.com/st4sd/sum-numbers.git",
                                    "branch": "main",
                                }
                            }
                        },
                        "config": {
                            "path": ".",
                            "manifestPath": None,
                        },
                    }
                ]
            },
            "metadata": {
                "package": {
                    "name": name,
                    "tags": ["dummy_tag"],
                    "maintainer": "vassilis.vassiliadis@ibm.com",
                    "description": "Toy virtual experiment",
                    "keywords": ["openshift", "hello-world"],
                }
            },
            "parameterisation": {
                "presets": {"runtime": {"args": ["--registerWorkflow=yes"]}},
                "executionOptions": {
                    "variables": [{"name": "numberOfPoints", "value": "1"}],
                    "data": [{"name": "cat_me.txt"}],
                    "platform": ["openshift", "default"],
                },
            },
        }

    return _build_payload



# /authorisation/token
@rest_api()
def test_authorisation_token(api_wrapper):
    # GET /authorisation/token
    with pytest.raises(experiment.service.errors.InvalidHTTPRequest):
        api_wrapper.api_request_get("authorisation/token")


# /datasets/
@rest_api()
def test_datasets(api_wrapper):
    # GET /datasets/
    with pytest.raises(experiment.service.errors.InvalidHTTPRequest):
        api_wrapper.api_datasets_list("datasets/")

    # POST /datasets/s3/{id}
    with pytest.raises(experiment.service.errors.InvalidHTTPRequest):
        api_wrapper.api_datasets_create(
            dataset_name="dummy_dataset_name",
            endpoint="dummy_endpoint",
            bucket="dummy_bucket",
            access_key_id="dummy_access_key_id",
            secret_access_key="dummy_secret_access_key",
            region="dummy_region",
        )


# /experiments/
@rest_api()
def test_experiments(api_wrapper, dummy_payload):
    # GET /experiments/
    experiments = api_wrapper.api_experiment_list()
    assert len(experiments.keys()) == 0

    # POST /experiments/
    api_wrapper.api_experiment_push(dummy_payload("sum-numbers"))
    experiments = api_wrapper.api_experiment_list()
    assert len(experiments.keys()) == 1

    # GET /experiments/{identifier}
    res = api_wrapper.api_experiment_get("sum-numbers")

    # Ignore timestamp for assertion
    res["metadata"]["registry"].pop("createdOn", None)

    # PUT /experiments/{identifier}/tag
    res = api_wrapper.api_request_put(
        "/experiments/sum-numbers:dummy_tag/tag?newTags=new_dummy_tag,latest"
    )

    # GET /experiments/{package_name}/history
    res = api_wrapper.api_request_get("/experiments/sum-numbers/history")

    # Ignore timestamps for assertion
    for index in [0, 1]:
        res["tags"][index].pop("createdOn", None)

    assert len(res['tags']) == 2
    assert len(res['untagged']) == 0

    assert sorted([x['tag'] for x in res['tags']]) == ['latest', 'new_dummy_tag']

    # DELETE /experiments/{identifier}
    api_wrapper.api_request_delete("/experiments/sum-numbers")
    experiments = api_wrapper.api_experiment_list()
    assert len(experiments.keys()) == 0

    # POST /experiments/{identifier}/start
    with pytest.raises(experiment.service.errors.InvalidHTTPRequest):
        api_wrapper.api_experiment_start("sum-numbers", dummy_payload("sum-numbers"))

    # POST /experiments/lambda/start
    with pytest.raises(experiment.service.errors.InvalidHTTPRequest):
        api_wrapper.api_experiment_start_lambda({"lambdaFlowIR": {}})


# /image-pull-secrets/
@pytest.mark.parametrize(
    "api_fixture,api_request",
    [
        (
                "api_wrapper",
                "api_image_pull_secrets_create",
        ),  # POST /image_pull_secrets/{id}
        (
                "api_wrapper",
                "api_image_pull_secrets_upsert",
        ),  # PUT /image_pull_secrets/{id}
    ],
)
@rest_api()
def test_image_pull_secrets_payload(api_fixture, api_request, request):
    api_wrapper = request.getfixturevalue(api_fixture)
    api_wrapper_call = getattr(api_wrapper, api_request)

    with pytest.raises(experiment.service.errors.InvalidHTTPRequest):
        api_wrapper_call(
            secret_name="dummy_secret_name",
            registry="dummy_registry",
            username="dummy_username",
            password="dummy_password",
        )


@rest_api()
def test_image_pull_secrets_get_secrets(api_wrapper):
    # GET /image_pull_secrets/

    ret = api_wrapper.api_image_pull_secrets_list()
    assert ret == {}

    # GET /image_pull_secrets/{id}
    with pytest.raises(experiment.service.errors.InvalidHTTPRequest):
        api_wrapper.api_request_get("image-pull-secrets/image_pull_secret_identifier")


# /instances/
@rest_api()
def test_instances_list(api_wrapper):
    # GET /instances/
    instances = api_wrapper.api_request_get("/instances")
    assert instances == []


@rest_api()
@pytest.mark.parametrize(
    "api_fixture,api_request,endpoint",
    [
        (
                "api_wrapper",
                "api_request_get",
                "/instances/instance_id",
        ),  # GET /instances/{id}
        (
                "api_wrapper",
                "api_request_get",
                "/instances/instance_id/outputs",
        ),  # GET /instances/{id}/outputs
        (
                "api_wrapper",
                "api_request_get",
                "/instances/instance_id/outputs/some_key",
        ),  # GET /instances/{id}/outputs/{key}
        (
                "api_wrapper",
                "api_request_get",
                "/instances/instance_id/properties",
        ),  # GET /instances/{id}/properties
        (
                "api_wrapper",
                "api_request_delete",
                "/instances/instance_id",
        ),  # DELETE /instances/{id
    ],
)
@rest_api()
def test_instances_exceptions(api_fixture, api_request, endpoint, request):
    api_wrapper = request.getfixturevalue(api_fixture)
    api_wrapper_call = getattr(api_wrapper, api_request)

    with pytest.raises(experiment.service.errors.InvalidHTTPRequest):
        api_wrapper_call(endpoint)


# /query/experiments/
@rest_api()
def test_query(api_wrapper):
    # POST /query/experiments/
    api_wrapper.api_request_post("/query/experiments", {})
    pass


@rest_api()
def test_relationships(api_wrapper, dummy_payload):
    # GET /relationships/
    relationships = api_wrapper.api_relationship_list()
    assert len(relationships.keys()) == 0

    # POST /relationships/
    api_wrapper.api_experiment_push(
        {
            "base": {
                "packages": [
                    {
                        "name": "main",
                        "source": {
                            "git": {
                                "location": {
                                    "url": "https://github.com/st4sd/configuration-generator-ani.git",
                                    "tag": "1.1.0",
                                },
                                "version": "9517e9ff76cb2e50cdfd7018c7ecde3576136050",
                            }
                        },
                        "dependencies": {"imageRegistries": []},
                        "config": {
                            "path": "ani-surrogate.yaml",
                            "manifestPath": "manifest.yaml",
                        },
                    }
                ]
            },
            "metadata": {
                "package": {
                    "name": "configuration-generator-ani",
                    "tags": ["latest", "1.1.0"],
                    "keywords": [
                        "smiles",
                        "computational chemistry",
                        "geometry-optimization",
                        "gamess",
                        "surrogate",
                    ],
                    "maintainer": "https://github.com/Jammyzx1",
                    "description": "Surrogate that optimizes the geometry of a molecule using the ANI neural potential (ani2x, functional: vWB97x) and adds it to a GAMESS molecule.inp file",
                }
            },
            "parameterisation": {
                "presets": {
                    "variables": [{"name": "ani-model", "value": "ani2x"}],
                    "runtime": {
                        "resources": {},
                        "args": ["--registerWorkflow=yes", "--failSafeDelays=no"],
                    },
                    "data": [],
                    "environmentVariables": [],
                    "platform": "openshift",
                },
                "executionOptions": {
                    "variables": [
                        {"name": "ani-walltime"},
                        {"name": "ani-grace-period"},
                        {"name": "force-tol"},
                        {"name": "iterations"},
                        {"name": "thermo-chem-T"},
                        {"name": "thermo-chem-P"},
                    ],
                    "data": [],
                    "runtime": {"resources": {}, "args": []},
                    "platform": [],
                },
            },
        }
    )
    api_wrapper.api_experiment_push(
        {
            "base": {
                "packages": [
                    {
                        "name": "main",
                        "source": {
                            "git": {
                                "location": {
                                    "url": "https://github.com/st4sd/band-gap-gamess.git",
                                    "tag": "1.1.0",
                                },
                                "version": "d5ad401af5e6e69262a83132e7dc37c75daa22b2",
                            }
                        },
                        "dependencies": {"imageRegistries": []},
                        "config": {
                            "path": "dft/homo-lumo-dft.yaml",
                            "manifestPath": "dft/manifest.yaml",
                        },
                    }
                ]
            },
            "metadata": {
                "package": {
                    "name": "band-gap-dft-gamess-us",
                    "tags": ["latest", "1.1.0"],
                    "keywords": [
                        "smiles",
                        "computational chemistry",
                        "homo-lumo",
                        "semi-empirical",
                        "kubeflux",
                    ],
                    "maintainer": "https://github.com/michael-johnston",
                    "description": "Uses the DFT functional and basis set B3LYP/6-31G(d,p) with Grimme et al's D3 correction to perform geometry optimization and HOMO-LUMO band gap calculation",
                }
            },
            "parameterisation": {
                "presets": {
                    "variables": [
                        {"name": "functional", "value": "B3LYP"},
                        {
                            "name": "basis",
                            "value": "GBASIS=N31 NGAUSS=6 NDFUNC=2 NPFUNC=1 DIFFSP=.TRUE. DIFFS=.TRUE.",
                        },
                    ],
                    "runtime": {
                        "resources": {},
                        "args": ["--failSafeDelays=no", "--registerWorkflow=yes"],
                    },
                    "data": [],
                    "environmentVariables": [],
                },
                "executionOptions": {
                    "variables": [
                        {"name": "numberMolecules"},
                        {"name": "startIndex"},
                        {"name": "mem"},
                        {"name": "gamess-walltime-minutes"},
                        {"name": "gamess-grace-period-seconds"},
                        {"name": "number-processors"},
                        {"name": "gamess-gpus"},
                    ],
                    "data": [],
                    "runtime": {"resources": {}, "args": []},
                    "platform": ["openshift", "openshift-kubeflux", "openshift-cpu"],
                },
            },
        }
    )

    api_wrapper.api_relationship_push(
        {
            "identifier": "ani-to-band-gap-dft",
            "description": "Uses ANI to generate the inputs to GAMESS US",
            "transform": {
                "outputGraph": {
                    "package": {
                        "name": "band-gap-dft-gamess-us:latest",
                        "source": {
                            "git": {
                                "location": {
                                    "url": "https://github.com/st4sd/band-gap-gamess.git",
                                    "tag": "1.1.0",
                                },
                                "version": "d5ad401af5e6e69262a83132e7dc37c75daa22b2",
                            }
                        },
                        "dependencies": {"imageRegistries": []},
                        "config": {
                            "path": "dft/homo-lumo-dft.yaml",
                            "manifestPath": "dft/manifest.yaml",
                        },
                        "graphs": [],
                    },
                    "identifier": "band-gap-dft-gamess-us:latest",
                    "components": ["stage0.XYZToGAMESS"],
                },
                "inputGraph": {
                    "package": {
                        "name": "configuration-generator-ani:latest",
                        "source": {
                            "git": {
                                "location": {
                                    "url": "https://github.com/st4sd/configuration-generator-ani.git",
                                    "tag": "1.1.0",
                                },
                                "version": "9517e9ff76cb2e50cdfd7018c7ecde3576136050",
                            }
                        },
                        "dependencies": {"imageRegistries": []},
                        "config": {
                            "path": "ani-surrogate.yaml",
                            "manifestPath": "manifest.yaml",
                        },
                        "graphs": [],
                    },
                    "identifier": "configuration-generator-ani:latest",
                    "components": [
                        "stage0.GeometryOptimisationANI",
                        "stage0.XYZToGAMESS",
                    ],
                },
                "relationship": {
                    "graphParameters": [
                        {
                            "outputGraphParameter": {"name": "stage0.SMILESToXYZ:ref"},
                            "inputGraphParameter": {"name": "stage0.SMILESToXYZ:ref"},
                        },
                        {
                            "outputGraphParameter": {
                                "name": "stage0.SMILESToGAMESSInput:ref"
                            },
                            "inputGraphParameter": {"name": "stage0.XYZToGAMESS:ref"},
                        },
                        {
                            "outputGraphParameter": {
                                "name": "stage0.SetFunctional/input_molecule.txt:ref"
                            },
                            "inputGraphParameter": {
                                "name": "input/input_molecule.txt:ref"
                            },
                        },
                        {
                            "outputGraphParameter": {
                                "name": "stage0.GetMoleculeIndex:output"
                            },
                            "inputGraphParameter": {
                                "name": "stage0.GetMoleculeIndex:output"
                            },
                        },
                    ],
                    "graphResults": [
                        {
                            "outputGraphResult": {
                                "name": "stage0.XYZToGAMESS/molecule.inp:copy"
                            },
                            "inputGraphResult": {
                                "name": "stage0.XYZToGAMESS/molecule.inp:copy"
                            },
                        }
                    ],
                    "inferParameters": True,
                    "inferResults": True,
                },
            },
        }
    )

    # GET /relationships/{identifier}
    relationship = api_wrapper.api_request_get("/relationships/ani-to-band-gap-dft")
    logger.info(relationship)

    rel = apis.models.relationships.Relationship.parse_obj(relationship['entry'])

    parameters = {
        x.inputGraphParameter.name: x.outputGraphParameter.name for x in rel.transform.relationship.graphParameters
    }

    assert parameters == {
        "stage0.SMILESToXYZ:ref": "stage0.SMILESToXYZ:ref",
        "stage0.XYZToGAMESS:ref": "stage0.SMILESToGAMESSInput:ref",
        "input/input_molecule.txt:ref": "stage0.SetFunctional/input_molecule.txt:ref",
        "stage0.GetMoleculeIndex:output": "stage0.GetMoleculeIndex:output",
        "backend": "backend",
        "mem": "mem",
        "number-processors": "number-processors",
    }

    assert len(parameters) == len(rel.transform.relationship.graphParameters)

    results = {
        x.outputGraphResult.name: x.inputGraphResult.name for x in rel.transform.relationship.graphResults
    }
    assert results == {
        "stage0.XYZToGAMESS/molecule.inp:copy": "stage0.XYZToGAMESS/molecule.inp:copy",
        "stage0.XYZToGAMESS:ref": "stage0.XYZToGAMESS:ref"
    }

    assert len(results) == len(rel.transform.relationship.graphResults)

    del relationship['entry']['transform']['relationship']

    assert relationship == {
        "entry": {
            "identifier": "ani-to-band-gap-dft",
            "description": "Uses ANI to generate the inputs to GAMESS US",
            "transform": {
                "outputGraph": {
                    "package": {
                        "name": "band-gap-dft-gamess-us:latest",
                        "source": {
                            "git": {
                                "location": {
                                    "url": "https://github.com/st4sd/band-gap-gamess.git",
                                    "tag": "1.1.0",
                                },
                                "version": "d5ad401af5e6e69262a83132e7dc37c75daa22b2",
                            }
                        },
                        "dependencies": {"imageRegistries": []},
                        "config": {
                            "path": "dft/homo-lumo-dft.yaml",
                            "manifestPath": "dft/manifest.yaml",
                        },
                        "graphs": [],
                    },
                    "identifier": "band-gap-dft-gamess-us:latest",
                    "components": ["stage0.XYZToGAMESS"],
                },
                "inputGraph": {
                    "package": {
                        "name": "configuration-generator-ani:latest",
                        "source": {
                            "git": {
                                "location": {
                                    "url": "https://github.com/st4sd/configuration-generator-ani.git",
                                    "tag": "1.1.0",
                                },
                                "version": "9517e9ff76cb2e50cdfd7018c7ecde3576136050",
                            }
                        },
                        "dependencies": {"imageRegistries": []},
                        "config": {
                            "path": "ani-surrogate.yaml",
                            "manifestPath": "manifest.yaml",
                        },
                        "graphs": [],
                    },
                    "identifier": "configuration-generator-ani:latest",
                    "components": [
                        "stage0.GeometryOptimisationANI",
                        "stage0.XYZToGAMESS",
                    ],
                },
            },
        },
        "problems": [],
    }

    # POST /relationships/{identifier}/synthesize/{new_package_name}
    api_wrapper.api_relationship_synthesize(
        "ani-to-band-gap-dft", {"parameterisation": {}}, "new-ani-to-band-gap-dft"
    )
    assert api_wrapper.api_experiment_get("new-ani-to-band-gap-dft")

    # DELETE /relationships/
    api_wrapper.api_relationship_delete(relationship_identifier="ani-to-band-gap-dft")
    relationships = api_wrapper.api_relationship_list()
    assert len(relationships.keys()) == 0


# /url-map/
@rest_api()
def test_url_map(api_wrapper):
    # GET /url-map/
    with pytest.raises(experiment.service.errors.InvalidHTTPRequest):
        api_wrapper.api_request_get("url-map/")

    # GET /url-map/{service}
    with pytest.raises(experiment.service.errors.InvalidHTTPRequest):
        api_wrapper.api_request_get("url-map/consumable-computing")


@rest_api()
def test_basic_library_operations(
    api_wrapper: experiment.service.db.ExperimentRestAPI,
    simple_dsl2: typing.Dict[str, typing.Any]
):
    name = api_wrapper.api_request_post("library/", json_payload=simple_dsl2, decode_json=False)

    ret = api_wrapper.api_request_get("library/")

    api_wrapper.api_request_delete(f"library/{name}/")

    assert ret == {
        "entries": [
            {
                "graph": {
                    'components': [
                        {
                            'command': {'arguments': '%(message)s',
                                        'executable': 'echo',
                                        'expandArguments': 'double-quote',
                                        'resolvePath': True},
                            'resourceManager': {'config': {'backend': 'local',
                                                           'walltime': 60.0}},
                            'resourceRequest': {'numberProcesses': 1,
                                                'numberThreads': 1,
                                                'ranksPerNode': 1,
                                                'threadsPerCore': 1},
                            'signature': {'name': 'echo',
                                          'parameters': [{'name': 'message'}]},
                            'variables': {},
                            'workflowAttributes': {'aggregate': False,
                                                   'memoization': {'disable': {'fuzzy': False,
                                                                               'strong': False}},
                                                   'restartHookOn': ['ResourceExhausted'],
                                                   'shutdownOn': []}
                        }
                    ],
                    'entrypoint': {
                        'entry-instance': 'main',
                        'execute': [
                            {'args': {}, 'target': '<entry-instance>'}
                        ]
                    },
                    'workflows': [
                        {
                            'execute': [{'args': {'message': '%(foo)s'},
                                         'target': '<hello>'}],
                            'signature': {'name': 'main',
                                          'parameters': [{'default': 'hello world',
                                                          'name': 'foo'}]},
                            'steps': {'hello': 'echo'}
                        }
                    ]
                }
            }
        ],

        "problems": []
    }
