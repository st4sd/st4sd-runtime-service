# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import base64
import json
import logging
from typing import Dict

import kubernetes.client
import yaml

import apis.k8s.errors
import apis.k8s.errors as errors
import apis.models.common
import apis.models.virtual_experiment
from apis.models.constants import *


def load_configuration(
        from_config_map: str | None = ConfigMapWithParameters
) -> apis.models.virtual_experiment.Configuration:
    f"""Loads the configuration settings

    Arguments:
        from_config_map(Optional[str]): Read configuration from kubernetes ConfigMap, if set to None will instead read
            configuration from the config.json file that ${CONFIG_JSON_PATH} returns.

    Returns:
        apis.models.virtual_experiment.Configuration
    """
    if CONFIG_JSON_PATH is not None:
        with open(CONFIG_JSON_PATH) as f:
            configuration = json.load(f)
    else:
        configuration = extract_configmap_values(from_config_map)

    return apis.models.virtual_experiment.Configuration.parse_obj(configuration)


def extract_configmap_values(name: str, namespace: str = MONITORED_NAMESPACE) -> Dict[str, str]:
    name = name
    api = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient())
    cm_list: kubernetes.client.V1ConfigMapList = api.list_namespaced_config_map(
        namespace, field_selector=f'metadata.name={name}')

    if len(cm_list.items) != 1:
        raise apis.k8s.errors.KubernetesObjectNotFound('configmap', k8s_name=name)

    configmap: kubernetes.client.V1ConfigMap = cm_list.items[0]
    data = yaml.load(configmap.data['config.json'], Loader=yaml.FullLoader)
    return data


def extract_git_oauth_token_default() -> str:
    config = load_configuration()
    if config.gitsecretOauth is None:
        logging.getLogger().warning("There is no default gitSecretOauth for cloning https:// URLs")
        return None

    return extract_git_oauth_token(config.gitsecretOauth)


def extract_git_oauth_token(name: str, key: str | None = None, namespace: str = MONITORED_NAMESPACE) -> str:
    if key is None:
        key = "oauth-token"

    api = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient())

    secrets: kubernetes.client.V1SecretList = api.list_namespaced_secret(
        namespace, field_selector=f'metadata.name={name}')

    if len(secrets.items) == 0:
        raise apis.k8s.errors.KubernetesObjectNotFound('secret', k8s_name=name)

    secret: kubernetes.client.V1Secret = secrets.items[0]
    return base64.b64decode(secret.data[key]).decode()


def extract_s3_credentials_from_dataset(
        name: str, namespace: str = MONITORED_NAMESPACE
) -> apis.models.common.OptionFromS3Values:
    api_instance = kubernetes.client.CustomObjectsApi(kubernetes.client.ApiClient())

    try:
        api_response = api_instance.list_namespaced_custom_object(
            group=K8S_DATASET_GROUP,
            version=K8S_DATASET_VERSION,
            namespace=namespace,
            plural=K8S_DATASET_PLURAL,
            field_selector=f"metadata.name={name}")
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 403:
            # VV: if we're forbidden to list Datasets that can mean 2 things:
            # 1. The RBAC is incorrect
            # 2. There is no such thing as a Dataset - let's assume RBAC is correct
            raise apis.k8s.errors.DatashimNotInstalledError()
        raise e from e

    if len(api_response['items']) != 1:
        raise apis.k8s.errors.KubernetesObjectNotFound('dataset', k8s_name=name)

    obj_dataset = api_response['items'][0]

    local = obj_dataset['spec']['local']
    if local['type'] not in ['S3', 'COS']:
        raise ValueError("Unsupported Dataset type: %s" % local['type'])

    return apis.models.common.OptionFromS3Values(
        accessKeyID=local['accessKeyID'],
        secretAccessKey=local['secretAccessKey'],
        endpoint=local['endpoint'],
        bucket=local['bucket'],
        region=local.get('region'))
