# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

import os
import typing

import pytest
import contextlib

import yaml

import apis.models.virtual_experiment
import apis.storage
import apis.storage.actuators
import apis.storage.actuators.local
import apis.storage.actuators.memory
import apis.storage.actuators.s3
import apis.storage.downloader

import apis.db.secrets

import apis.kernel.internal_experiments


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

def test_experiment_from_s3(
        output_dir: str,
        pvep_on_s3: typing.Dict[str, typing.Any],
):
    in_memory = apis.storage.actuators.memory.InMemoryStorage({})
    initialized = 0

    @contextlib.contextmanager
    def mock_s3_storage():
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
            yield mock_s3
        finally:
            apis.storage.actuators.s3.S3Storage = actual_s3

    db_secrets = apis.db.secrets.DatabaseSecrets(db_path=os.path.join(output_dir, "secrets.db"))

    pvep = apis.models.virtual_experiment.ParameterisedPackage(**pvep_on_s3)

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
    # VV: FIXME Use DSL 2.0 here
    dsl = yaml.safe_load(
        """
        components:
          - name: hello
            variables:
              foo: bar
            command:
              executable: echo
              arguments: "%(foo)s"
        """
    )

    yaml_dsl = yaml.safe_dump(dsl, indent=2)

    with mock_s3_storage():
        apis.kernel.internal_experiments.validate_internal_experiment(
            dsl2_definition=dsl,
            pvep=pvep,
        )
        apis.kernel.internal_experiments.store_internal_experiment(
            dsl2_definition=dsl,
            pvep=pvep,
            db_secrets=db_secrets
        )

        uploaded = sorted(in_memory.files)
        assert uploaded == sorted([
            "/",
            # VV: The code will call apis.storage.actuators.s3.S3Storage() just once (with "raw" s3 creds)
            "initialized/", "initialized/0.yaml",
            # VV: The code will upload the file experiments/example/conf/flowir_package.yaml (DSL 2.0)
            "experiments/", "experiments/example/", "experiments/example/conf/",
            "experiments/example/conf/flowir_package.yaml",
        ])

        contents = in_memory.read("experiments/example/conf/flowir_package.yaml").decode()
        assert contents == yaml_dsl

        contents = yaml.safe_load(in_memory.read("initialized/0.yaml"))
        assert contents == {
            "endpoint": "https://my.endpoint",
            "bucket": "a-bucket",
            "access_key_id": "access-key-id",
            "secret_access_key": "secret-access-key",
            "region_name": None,
        }
