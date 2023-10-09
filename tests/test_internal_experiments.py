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

import experiment.model.frontends.dsl


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
    # VV: FIXME Use DSL 2.0 here
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
    # VV: S3 deletes keys about 7 days after we ask COS to delete them - so let's use a static name
    pvep_name = f"test_internal_experiment_simple_real"

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

    assert sorted(pvep.base.packages[0].source.s3.location.dict(exclude_none=True)) == ["bucket", "endpoint", "region"]
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

    assert rc_namespace.dict(by_alias=True) == orig_namespace.dict(by_alias=True)
