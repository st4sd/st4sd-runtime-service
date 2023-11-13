# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


import os
import pathlib
import typing

import pytest
import yaml

import apis.db.exp_packages
import apis.db.secrets
import apis.kernel.internal_experiments
import apis.kernel.experiments

import apis.models.virtual_experiment
import apis.storage
import apis.storage.actuators
import apis.storage.actuators.local
import apis.storage.actuators.memory
import apis.storage.actuators.s3
import apis.storage.downloader

import experiment.model.frontends.flowir
import experiment.model.frontends.dsl

import apis.models.errors

import random
import string

internal_storage_s3 = pytest.mark.skipif("not config.getoption('internal_storage_s3')")

@pytest.fixture()
def pvep_on_s3() -> typing.Dict[str, typing.Any]:
    return {
        "base": {
        },
        "metadata": {
            "package": {
                "name": "example",
                "description": "the-description",
                "keywords": ["something"],
                "maintainer": "someone"
            }
        }
    }


def test_point_internal_experiment_to_s3(pvep_on_s3: typing.Dict[str, typing.Any]):
    pvep = apis.models.virtual_experiment.ParameterisedPackage(**pvep_on_s3)
    dsl = {}

    pvep = apis.kernel.internal_experiments.point_base_package_to_s3_storage(
        pvep=pvep,
        credentials=apis.models.virtual_experiment.SourceS3SecurityCredentials(
            value=apis.models.virtual_experiment.SourceS3SecurityCredentialsValue(
                accessKeyID="access-key-id",
                secretAccessKey="secret-access-key",
            )
        ),
        location=apis.models.virtual_experiment.BasePackageSourceS3Location(
            bucket="a-bucket",
            endpoint="https://my.endpoint",
        )
    )
    assert len(pvep.base.packages) == 1

    assert pvep.base.packages[0].dict(exclude_none=True) == {
        "name": "main",
        'dependencies': {
            'imageRegistries': []
        },
        'graphs': [],
        "source": {
            "s3": {
                "security": {
                    "credentials": {
                        "value": {
                            "accessKeyID": "access-key-id",
                            "secretAccessKey": "secret-access-key",
                        }
                    }
                },
                "location": {
                    "bucket": "a-bucket",
                    "endpoint": "https://my.endpoint"
                }
            }
        },
        "config": {
            "path": "experiments/example"
        }
    }


@pytest.fixture()
def simple_dsl2() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load(
        """
        entrypoint:
          entry-instance: main
          execute:
          - target: <entry-instance>
            args:
              foo: bar
        workflows:
        - signature:
            name: main
            parameters:
            - name: foo
              default: hello world
          steps:
            hello: echo
          execute:
          - target: <hello>
            args:
              message: "%(foo)s"
        components:
        - signature:
            name: echo
            parameters:
            - name: message
          command:
            executable: echo
            arguments: "%(message)s"
        """
    )


@pytest.fixture()
def simple_dsl2_with_inputs() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load(
        """
        entrypoint:
          entry-instance: main
          execute:
          - target: <entry-instance>
        workflows:
        - signature:
            name: main
            parameters:
            - name: input.my-inputs.csv
          steps:
            hello: echo
          execute:
          - target: <hello>
            args:
              message: "%(input.my-inputs.csv)s:output"
        components:
        - signature:
            name: echo
            parameters:
            - name: message
          command:
            executable: echo
            arguments: "%(message)s"
        """
    )


@pytest.fixture(scope="function")
def mock_s3_storage():
    in_memory = apis.storage.actuators.memory.InMemoryStorage({})
    initialized = 0

    def mock_s3(
            endpoint_url: str,
            bucket: str,
            access_key_id: str,
            secret_access_key: str,
            region_name: str,
    ) -> apis.storage.actuators.memory.InMemoryStorage:
        nonlocal initialized
        settings = yaml.safe_dump(
            {
                "endpoint": endpoint_url,
                "bucket": bucket,
                "access_key_id": access_key_id,
                "secret_access_key": secret_access_key,
                "region_name": region_name
            }
        ).encode()
        in_memory.write(f"initialized/{initialized}.yaml", settings)
        initialized += 1
        return in_memory

    actual_s3 = apis.storage.actuators.s3.S3Storage

    try:
        apis.storage.actuators.s3.S3Storage = mock_s3
        yield in_memory
    finally:
        apis.storage.actuators.s3.S3Storage = actual_s3


def test_internal_experiment_with_edges_and_extracting_dsl_for_viewing(
    output_dir: str,
    mock_s3_storage: apis.storage.actuators.memory.InMemoryStorage,
):
    payload = {
        'pvep': {
            'base': {
                'packages': [
                    {'name': 'main', 'source': {}}],
            },
            'metadata': {'package': {'name': 'aptest', 'tags': [], 'keywords': []}, 'registry': {
                'digest': 'sha256x9a87f2f54cbb165f3b5ec2bc64b0e1fd7c3af5d258c5a12087490b40', 'tags': [],
                'timesExecuted': 0, 'interface': {}, 'inputs': [], 'data': [], 'containerImages': [],
                'executionOptionsDefaults': {'variables': [{'name': 'input.number.txt', 'valueFrom': [
                    {'value': 'input/number.txt', 'platform': 'default'}]}, {'name': 'b', 'valueFrom': [
                    {'value': '2', 'platform': 'default'}]}]}, 'platforms': ['default']}},
            'parameterisation': {
                'presets': {'variables': [], 'runtime': {'resources': {}, 'args': []}, 'data': [],
                            'environmentVariables': []},
                'executionOptions': {'variables': [], 'data': [], 'runtime': {'resources': {}, 'args': []},
                                     'platform': []}}}, 'workflow': {
            'dsl': {'entrypoint': {'entry-instance': 'main', 'execute': [
                {'target': '<entry-instance>', 'args': {}}]},
                    'workflows': [
                        {'signature': {'name': 'main',
                                       'description': "Adds 2 numbers, one from the input file `number.txt` "
                                                      "and a second one from the parameter 'b'",
                                       'parameters': [{'name': 'input.number.txt', 'default': ''},
                                                      {'name': 'b', 'default': '2'}]},
                         'steps': {'generator': 'generator', 'sum': 'sum'}, 'execute': [
                            {'target': '<generator>', 'args': {'a': '%(input.number.txt)s', 'b': '%(b)s'}},
                            {'target': '<sum>',
                             'args': {'left': '<generator>/a.txt:output', 'right': '<generator>/b.txt:output'}}]}],
                    'components': [
                        {'signature': {
                            'name': 'generator',
                            'description': "Produces 2 files (a.txt and b.txt) using the contents of the "
                                           "parameters 'a' and 'b' respectively",
                            'parameters': [{'name': 'a', 'default': '10'},
                                           {'name': 'b', 'default': '20'}]},
                            'command': {'executable': 'bash',
                                        'arguments': '-c "echo \'generating files\' && echo -n \'%(a)s\'>a.txt '
                                                     '&& echo -n \'%(b)s\'>b.txt && echo Done"',
                                        'expandArguments': 'none'},
                            'workflowAttributes': {'aggregate': True, 'restartHookOn': ['ResourceExhausted']},
                            'resourceManager': {'lsf': {'statusRequestInterval': '20'}}}, {
                            'signature': {'name': 'sum', 'description': 'Adds 2 numbers',
                                          'parameters': [{'name': 'left', 'default': ''},
                                                         {'name': 'right', 'default': ''}]},
                            'command': {'executable': 'python',
                                        'arguments': '-c "print(%(left)s + %(right)s, end=\'\')"',
                                        'expandArguments': 'none'},
                            'workflowAttributes': {
                                'aggregate': True, 'restartHookOn': ['ResourceExhausted']},
                            'resourceManager': {'lsf': {'statusRequestInterval': '20'}}}]}}}

    db_secrets = apis.db.secrets.DatabaseSecrets(db_path=os.path.join(output_dir, "secrets.db"))
    db_experiments = apis.db.exp_packages.DatabaseExperiments(db_path=os.path.join(output_dir, "experiments.db"))

    with db_secrets:
        db_secrets.secret_create(
            apis.db.secrets.Secret(
                name="default-s3-secret",
                data={
                    "S3_BUCKET": "a-bucket",
                    "S3_ENDPOINT": "https://my.endpoint",
                    "S3_ACCESS_KEY_ID": "access-key-id",
                    "S3_SECRET_ACCESS_KEY": "secret-access-key",
                    "S3_REGION": "region"
                }
            )
        )

    pvep = apis.models.virtual_experiment.ParameterisedPackage(**payload["pvep"])

    pvep = apis.kernel.internal_experiments.upsert_internal_experiment(
        dsl2_definition=payload["workflow"]["dsl"],
        pvep=pvep,
        db_secrets=db_secrets,
        db_experiments=db_experiments,
        package_source="default-s3-secret",
    )

    download = apis.storage.PackagesDownloader(pvep, db_secrets=db_secrets)

    dsl = apis.kernel.experiments.api_get_experiment_dsl(
        pvep=pvep,
        packages=download
    )

    one_component = [x for x in dsl['components'] if x['signature']['name'] == "sum"]
    assert len(one_component) == 1

    one_component = one_component[0]

    assert one_component == {
        'command': {
            'arguments': '-c "print(%(left)s + %(right)s, end=\'\')"',
            'environment': None,
            'executable': 'python',
            'expandArguments': 'none',
            'interpreter': None,
            'resolvePath': True},
        'resourceManager': {
            'config': {
                'backend': 'local', 'walltime': 60.0},
            'docker': {'image': None,
                       'imagePullPolicy': 'Always',
                       'platform': None},
            'kubernetes': {'api-key-var': None,
                           'cpuUnitsPerCore': None,
                           'gracePeriod': None,
                           'host': 'http://localhost:8080',
                           'image': None,
                           'image-pull-secret': None,
                           'namespace': 'default',
                           'podSpec': None,
                           'qos': None},
            'lsf': {'dockerImage': None,
                    'dockerOptions': None,
                    'dockerProfileApp': None,
                    'queue': 'normal',
                    'reservation': None,
                    'resourceString': None,
                    'statusRequestInterval': 20}},
        'resourceRequest': {'gpus': None,
                            'memory': None,
                            'numberProcesses': 1,
                            'numberThreads': 1,
                            'ranksPerNode': 1,
                            'threadsPerCore': 1},
        'signature': {'description': 'Adds 2 numbers',
                      'name': 'sum',
                      'parameters': [{'default': '', 'name': 'left'},
                                     {'default': '', 'name': 'right'}]},
        'variables': {},
        'workflowAttributes': {
            'aggregate': True,
            'isMigratable': False,
            'isMigrated': False,
            'isRepeat': False,
            'maxRestarts': None,
            'memoization': {
                'disable': {'fuzzy': False,
                            'strong': False},
                'embeddingFunction': None},
            'optimizer': {
                'disable': False,
                'exploitChance': 0.9,
                'exploitTarget': 0.75,
                'exploitTargetHigh': 0.5,
                'exploitTargetLow': 0.25},
            'repeatInterval': None,
            'repeatRetries': 3,
            'replicate': None,
            'restartHookFile': None,
            'restartHookOn': ['ResourceExhausted'],
            'shutdownOn': [],
            'stage': None}}


def test_internal_experiment_simple(
    output_dir: str,
    simple_dsl2: typing.Dict[str, typing.Any],
    pvep_on_s3: typing.Dict[str, typing.Any],
    mock_s3_storage: apis.storage.actuators.memory.InMemoryStorage,
):
    pvep = apis.models.virtual_experiment.ParameterisedPackage(**pvep_on_s3)
    db_secrets = apis.db.secrets.DatabaseSecrets(db_path=os.path.join(output_dir, "secrets.db"))
    db_experiments = apis.db.exp_packages.DatabaseExperiments(db_path=os.path.join(output_dir, "experiments.db"))

    with db_secrets:
        db_secrets.secret_create(
            apis.db.secrets.Secret(
                name="default-s3-secret",
                data={
                    "S3_BUCKET": "a-bucket",
                    "S3_ENDPOINT": "https://my.endpoint",
                    "S3_ACCESS_KEY_ID": "access-key-id",
                    "S3_SECRET_ACCESS_KEY": "secret-access-key",
                    "S3_REGION": "region"
                }
            )
        )

    apis.kernel.internal_experiments.upsert_internal_experiment(
        dsl2_definition=simple_dsl2,
        pvep=pvep,
        db_secrets=db_secrets,
        db_experiments=db_experiments,
        package_source="default-s3-secret",
    )

    assert pvep.base.packages[0].source.s3.security.credentials.valueFrom.dict(exclude_none=True) == {
        'keyAccessKeyID': 'S3_ACCESS_KEY_ID',
        'keySecretAccessKey': 'S3_SECRET_ACCESS_KEY',
        'secretName': 'default-s3-secret'
    }

    assert pvep.base.packages[0].source.s3.location.dict(exclude_none=True) == {
        "bucket": "a-bucket",
        "endpoint": "https://my.endpoint",
        "region": "region",
    }

    uploaded = sorted(mock_s3_storage.files)
    assert uploaded == sorted([
        "/",
        # VV: The code will call apis.storage.actuators.s3.S3Storage() twice, once to upload the files
        # and then a second time to download the files from S3 during validate_and_store_pvep_in_db()
        "initialized/", "initialized/0.yaml", "initialized/1.yaml",
        # VV: The code will upload the file experiments/example/conf/flowir_package.yaml (DSL 2.0)
        "experiments/", "experiments/example/", "experiments/example/conf/",
        "experiments/example/conf/dsl.yaml",
    ])

    contents = mock_s3_storage.read("experiments/example/conf/dsl.yaml").decode()
    yaml_dsl = yaml.safe_dump(simple_dsl2, indent=2)
    assert contents == yaml_dsl

    expected_creds = {
        "endpoint": "https://my.endpoint",
        "bucket": "a-bucket",
        "access_key_id": "access-key-id",
        "secret_access_key": "secret-access-key",
        "region_name": "region",
    }

    contents = yaml.safe_load(mock_s3_storage.read("initialized/0.yaml"))
    assert contents == expected_creds

    contents = yaml.safe_load(mock_s3_storage.read("initialized/1.yaml"))
    assert contents == expected_creds

    assert pvep.metadata.registry.platforms == ["default"]
    assert len(pvep.metadata.registry.executionOptionsDefaults.variables) == 1
    assert pvep.metadata.registry.executionOptionsDefaults.variables[0].dict(exclude_none=True) == {
        "name": "foo",
        "valueFrom": [
            {
                "value": "bar",
                "platform": "default"
            }
        ]
    }


@internal_storage_s3()
def test_internal_experiment_simple_real(
    output_dir: str,
    simple_dsl2: typing.Dict[str, typing.Any],
    pvep_on_s3: typing.Dict[str, typing.Any],
):
    rand = random.Random()
    characters = string.ascii_letters + string.digits
    suffix = ''.join((rand.choice(characters) for x in range(10)))
    pvep_name = f"test_internal_experiment_simple_real-{suffix}"

    pvep = apis.models.virtual_experiment.ParameterisedPackage(**pvep_on_s3)
    pvep.metadata.package.name = pvep_name

    db_secrets = apis.db.secrets.DatabaseSecrets(db_path=os.path.join(output_dir, "secrets.db"))
    db_experiments = apis.db.exp_packages.DatabaseExperiments(db_path=os.path.join(output_dir, "experiments.db"))

    keys = ["S3_BUCKET", "S3_ENDPOINT", "S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY", "S3_REGION"]
    secret = apis.db.secrets.Secret(
        name="default-s3-secret",
        data={k: os.environ[k] for k in keys if os.environ.get(k)}
    )
    lookup = {
        "S3_BUCKET": "bucket",
        "S3_ENDPOINT": "endpoint_url",
        "S3_ACCESS_KEY_ID": "access_key_id",
        "S3_SECRET_ACCESS_KEY": "secret_access_key",
        "S3_REGION": "region_name"
    }
    args = {
        lookup[k]: secret.data.get(k) for k in lookup
    }
    s3_storage = apis.storage.actuators.s3.S3Storage(
        **args
    )

    with db_secrets:
        db_secrets.secret_create(secret)

    apis.kernel.internal_experiments.upsert_internal_experiment(
        dsl2_definition=simple_dsl2,
        pvep=pvep,
        db_secrets=db_secrets,
        db_experiments=db_experiments,
        package_source="default-s3-secret",
        dest_path=pathlib.Path("unit-tests-experiments")
    )

    assert pvep.base.packages[0].source.s3.security.credentials.valueFrom.dict(exclude_none=True) == {
        'keyAccessKeyID': 'S3_ACCESS_KEY_ID',
        'keySecretAccessKey': 'S3_SECRET_ACCESS_KEY',
        'secretName': 'default-s3-secret'
    }

    assert sorted(pvep.base.packages[0].source.s3.location.dict(exclude_none=True)) == [
        "bucket", "endpoint", "region"]
    contents = s3_storage.read(f"unit-tests-experiments/{pvep_name}/conf/dsl.yaml").decode()
    s3_storage.remove(f"unit-tests-experiments/{pvep_name}/")

    yaml_dsl = yaml.safe_dump(simple_dsl2, indent=2)
    assert contents == yaml_dsl

    assert pvep.metadata.registry.platforms == ["default"]
    assert len(pvep.metadata.registry.executionOptionsDefaults.variables) == 1
    assert pvep.metadata.registry.executionOptionsDefaults.variables[0].dict(exclude_none=True) == {
        "name": "foo",
        "valueFrom": [
            {
                "value": "bar",
                "platform": "default"
            }
        ]
    }


def test_recover_dsl_from_internal_experiment(
        output_dir: str,
        simple_dsl2: typing.Dict[str, typing.Any],
        pvep_on_s3: typing.Dict[str, typing.Any],
        mock_s3_storage: apis.storage.actuators.memory.InMemoryStorage,
):
    pvep = apis.models.virtual_experiment.ParameterisedPackage(**pvep_on_s3)
    db_secrets = apis.db.secrets.DatabaseSecrets(db_path=os.path.join(output_dir, "secrets.db"))
    db_experiments = apis.db.exp_packages.DatabaseExperiments(db_path=os.path.join(output_dir, "experiments.db"))

    with db_secrets:
        db_secrets.secret_create(
            apis.db.secrets.Secret(
                name="default-s3-secret",
                data={
                    "S3_BUCKET": "a-bucket",
                    "S3_ENDPOINT": "https://my.endpoint",
                    "S3_ACCESS_KEY_ID": "access-key-id",
                    "S3_SECRET_ACCESS_KEY": "secret-access-key",
                    "S3_REGION": "region"
                }
            )
        )

    pvep = apis.kernel.internal_experiments.upsert_internal_experiment(
        dsl2_definition=simple_dsl2,
        pvep=pvep,
        db_secrets=db_secrets,
        db_experiments=db_experiments,
        package_source="default-s3-secret",
    )

    download = apis.storage.PackagesDownloader(pvep, db_secrets=db_secrets)

    recons_dsl = apis.kernel.experiments.api_get_experiment_dsl(
        pvep=pvep,
        packages=download
    )

    orig_namespace = experiment.model.frontends.dsl.Namespace(**simple_dsl2)
    rc_namespace = experiment.model.frontends.dsl.Namespace(**recons_dsl)

    apis.kernel.experiments.update_component_defaults_in_namespace(orig_namespace)
    assert rc_namespace.dict(by_alias=True) == orig_namespace.dict(by_alias=True)


def test_recover_dsl_from_internal_experiment_with_input_params(
        output_dir: str,
        simple_dsl2_with_inputs: typing.Dict[str, typing.Any],
        pvep_on_s3: typing.Dict[str, typing.Any],
        mock_s3_storage: apis.storage.actuators.memory.InMemoryStorage,
):
    pvep = apis.models.virtual_experiment.ParameterisedPackage(**pvep_on_s3)
    db_secrets = apis.db.secrets.DatabaseSecrets(db_path=os.path.join(output_dir, "secrets.db"))
    db_experiments = apis.db.exp_packages.DatabaseExperiments(db_path=os.path.join(output_dir, "experiments.db"))

    with db_secrets:
        db_secrets.secret_create(
            apis.db.secrets.Secret(
                name="default-s3-secret",
                data={
                    "S3_BUCKET": "a-bucket",
                    "S3_ENDPOINT": "https://my.endpoint",
                    "S3_ACCESS_KEY_ID": "access-key-id",
                    "S3_SECRET_ACCESS_KEY": "secret-access-key",
                    "S3_REGION": "region"
                }
            )
        )

    pvep = apis.kernel.internal_experiments.upsert_internal_experiment(
        dsl2_definition=simple_dsl2_with_inputs,
        pvep=pvep,
        db_secrets=db_secrets,
        db_experiments=db_experiments,
        package_source="default-s3-secret",
    )

    assert "internal-experiment" in pvep.metadata.package.keywords

    download = apis.storage.PackagesDownloader(pvep, db_secrets=db_secrets)

    recons_dsl = apis.kernel.experiments.api_get_experiment_dsl(
        pvep=pvep,
        packages=download
    )

    orig_namespace = experiment.model.frontends.dsl.Namespace(**simple_dsl2_with_inputs)
    rc_namespace = experiment.model.frontends.dsl.Namespace(**recons_dsl)

    apis.kernel.experiments.update_component_defaults_in_namespace(orig_namespace)
    rc_sans_entrypoint = rc_namespace.dict(by_alias=True)
    orig_sans_entrypoint = orig_namespace.dict(by_alias=True)

    del rc_sans_entrypoint["entrypoint"]
    del orig_sans_entrypoint["entrypoint"]

    assert rc_sans_entrypoint == orig_sans_entrypoint

    assert rc_namespace.entrypoint.dict(by_alias=True) == {
        "entry-instance": "main",
        "execute": [
            {
                "target": "<entry-instance>",
                "args": {
                    "input.my-inputs.csv": "input/my-inputs.csv"
                }
            }
        ]
    }



def test_auto_pvep_for_simple(
    simple_dsl2: typing.Dict[str, typing.Any],
):
    pvep_and_changes = apis.kernel.internal_experiments.generate_pvep_for_dsl(
        dsl2_definition=simple_dsl2,
        template=None,
    )

    expected_registry = {
        'data': [],
        'executionOptionsDefaults': {
            'variables': [
                {'name': 'foo',
                 'valueFrom': [
                     {
                         # VV: In this DSL workflow, there's 1 parameter that the entrypoint sets
                         'value': 'bar',
                         'platform': 'default'
                     }
                 ]
                 }
            ]
        },
        'inputs': [],
        # VV: There's only one platform for **ALL** DSL workflows
        'platforms': ['default']
    }
    pvep = pvep_and_changes.pvep

    assert pvep.metadata.registry.dict(exclude={
        "containerImages", "createdOn", "digest", "interface", "tags", "timesExecuted"
    }) == expected_registry

    # VV: No parameterisation at all
    assert pvep.parameterisation.presets.variables == []
    assert pvep.parameterisation.presets.data == []
    assert pvep.parameterisation.executionOptions.variables == []
    assert pvep.parameterisation.executionOptions.data == []


def test_auto_update_pvep_for_simple(
    simple_dsl2: typing.Dict[str, typing.Any],
):
    definition = yaml.safe_load("""
    base:
        packages:
        - source:
            s3:
                security:
                    credentials:
                        value:
                            accessKeyID: $accessKeyID
                            secretAccessKey: $secretAccessKey
                location:
                    bucket: $bucket
                    endpoint: https://end.point
          config:
              path: /experiments/old.package
    parameterisation:
        presets:
            variables:
            - name: doesNotExistAnyMore
              value: does not matter
            data:
            - name: $data-preset-does-not-exist
        executionOptions:
            variables:
            - name:  foo
              value: keep this default value       
    metadata:
        package:
            name: old
            license: $license
            maintainer: $maintainer
        registry:
            inputs:
            - name: $input
            data:
            - name: $data-preset-does-not-exist
    """)
    template = apis.models.virtual_experiment.ParameterisedPackage(**definition)

    pvep_and_changes = apis.kernel.internal_experiments.generate_pvep_for_dsl(
        dsl2_definition=simple_dsl2,
        template=template,
    )

    expected_registry = {
        'data': [],
        'executionOptionsDefaults': {
            'variables': [
                {'name': 'foo',
                 'valueFrom': [
                     {
                         # VV: In this DSL workflow, there's 1 parameter that the entrypoint sets
                         'value': 'bar',
                         'platform': 'default'
                     }
                 ]
                 }
            ]
        },
        'inputs': [],
        # VV: There's only one platform for **ALL** DSL workflows
        'platforms': ['default']
    }
    pvep = pvep_and_changes.pvep
    changes = pvep_and_changes.changes

    assert changes == [
        {'message': 'Erased base.packages', 'location': ["base", "packages"]},
        {'message': 'Updated metadata.registry', 'location': ['metadata', 'registry']},
        {'message': 'Inherited metadata.package from template', 'location': ["metadata", "package"]},
        {'message': 'Removed presets for variable doesNotExistAnyMore',
         'location': ['parameterisation', 'presets', 'variables', 0]},
        {'location': ['parameterisation', 'executionOptions', 'variables', 0],
         'message': 'Inherited executionOptions for variable foo from template'},
        {'message': 'Removed presets for data $data-preset-does-not-exist',
         'location': ['parameterisation', 'presets', 'data', 0]}
    ]

    assert pvep.metadata.registry.dict(exclude={
        "containerImages", "createdOn", "digest", "interface", "tags", "timesExecuted"
    }) == expected_registry

    assert pvep.parameterisation.presets.variables == []
    assert pvep.parameterisation.presets.data == []
    assert pvep.parameterisation.executionOptions.data == []
    assert len(pvep.parameterisation.executionOptions.variables) == 1

    assert pvep.parameterisation.executionOptions.variables[0].dict(exclude_none=True) == {
        "name": "foo",
        "value": "keep this default value"
    }

    assert pvep.metadata.package.name == "old"

    assert "internal-experiment" in pvep.metadata.package.keywords


def test_invalid_dsl():
    dsl = yaml.safe_load("""
    entrypoint:
      entry-instance: main
      execute:
      - target: "<entry-instance>"
    """)

    with pytest.raises(apis.models.errors.InvalidModelError) as e:
        apis.kernel.internal_experiments.validate_dsl(dsl)

    exc = e.value

    assert exc.problems == [
        {
            'message': "No template with name main",
            'location': ['entrypoint', 'entry-instance']
        }
    ]


def test_validate_dsl_with_nested_workflows():
    dsl = yaml.safe_load("""
    entrypoint:
      entry-instance: test
      execute:
      - target: "<entry-instance>"
        args: {}
    workflows:
    - signature:
        name: test
        parameters:
        - name: input.a
          default: ''
        - name: b
          default: '20'
      steps:
        generator: generator
        consumer: consumer
      execute:
      - target: "<generator>"
        args:
          a: "%(input.a)s:output"
          b: "%(b)s"
      - target: "<consumer>"
        args:
          one: "<generator>"
    components:
    - signature:
        name: generator
        parameters:
        - name: a
          default: ''
        - name: b
          default: ''
      command:
        executable: a
        arguments: "%(a)s %(b)s"
    - signature:
        name: consumer
        parameters:
        - name: one
          default: ''
      command:
        executable: a
        arguments: "%(one)s:ref"
    """)

    apis.kernel.internal_experiments.validate_dsl(dsl)