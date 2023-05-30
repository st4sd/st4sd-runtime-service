# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import apis.models.constants
import apis.k8s
import apis.db.secrets
import pytest

import utils


def test_extract_oauth_token(mock_list_namespaced_secret):
    db_secret = apis.db.secrets.KubernetesSecrets(namespace=apis.models.constants.MONITORED_NAMESPACE)
    with db_secret:
        secret = db_secret.secret_get("my-test")
    oauth_token = secret['data']['oauth-token']

    assert oauth_token == "my-token"


def test_extract_oauth_token_no_secret(mock_list_namespaced_secret):
    with pytest.raises(apis.k8s.errors.KubernetesObjectNotFound) as e:
        db_secret = apis.db.secrets.KubernetesSecrets(namespace=apis.models.constants.MONITORED_NAMESPACE)
        with db_secret:
            _ = db_secret.secret_get("not-my-test")

    assert e.value.name == "not-my-test"
    assert e.value.kind == "secret"


def test_extract_oauth_token_default(mock_list_namespaced_secret, mock_list_config_map_configuration):
    db_secret = apis.db.secrets.KubernetesSecrets(namespace=apis.models.constants.MONITORED_NAMESPACE)

    config = utils.parse_configuration(
        # VV: this is to test the Kubernetes Secret Database
        local_deployment=False,
        validate=False, from_config_map=apis.models.constants.ConfigMapWithParameters)

    assert config.gitsecretOauth == "my-test"
    with db_secret:
        secret = db_secret.secret_get(config.gitsecretOauth)
    oauth_token = secret['data']['oauth-token']
    assert oauth_token == "my-token"


def test_extract_s3_credentials_from_dataset(mock_list_dataset):
    s3 = apis.k8s.extract_s3_credentials_from_dataset("my-test")
    assert s3.bucket == "bucket"
    assert s3.accessKeyID == "accessKeyID"
    assert s3.secretAccessKey == "secretAccessKey"
    assert s3.path is None
    assert s3.region == "region"
    assert s3.endpoint == "endpoint"


def test_extract_s3_credentials_from_dataset_no_dataset(mock_list_dataset):
    with pytest.raises(apis.k8s.errors.KubernetesObjectNotFound) as e:
        apis.k8s.extract_s3_credentials_from_dataset("not-my-test")

    assert e.value.name == "not-my-test"
    assert e.value.kind == "dataset"


def test_load_configuration(mock_list_config_map_configuration):
    config = utils.parse_configuration(
        # VV: this is to test the Kubernetes Secret Database
        local_deployment=False,
        from_config_map=apis.models.constants.ConfigMapWithParameters)

    assert config.image == "res-st4sd-team-official-base-docker-local.artifactory.swg-devops.com/st4sd-runtime-core:latest"
    assert config.gitsecretOauth == "my-test"
    assert config.imagePullSecrets == ["st4sd-base-images", "st4sd-community-applications"]
    assert config.workingVolume == "workflow-instances-pvc"
    assert config.inputdatadir == "./examples"
    assert config.s3FetchFilesImage == "res-st4sd-team-official-base-docker-local.artifactory.swg-devops.com/st4sd-runtime-k8s-input-s3:latest"
    assert config.defaultArguments == [{"--executionMode": "production"}]
