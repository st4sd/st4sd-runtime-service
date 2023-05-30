# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


import base64
import difflib
import json
import logging
import os.path

import traceback
from typing import Dict, Any, cast, Optional

import experiment.model.frontends.flowir as FlowIR
import kubernetes
import six

import apis.models.errors
import apis.models.virtual_experiment
import apis.db.exp_packages
import apis.db.relationships
import apis.db.secrets
import apis.k8s

from apis.models.constants import *


def _generate_path_to_storage_file(local_deployment: bool, filename: str) -> str:
    configuration = setup_config(local_deployment=local_deployment)
    return os.path.join(configuration['inputdatadir'], filename)


def database_relationships_open(local_deployment: bool) -> apis.db.relationships.DatabaseRelationships:
    # VV: FIXME This is a bad place for this method, need to figure out a better way to decide where to store db
    path = _generate_path_to_storage_file(local_deployment, "relationships.json")
    return apis.db.relationships.DatabaseRelationships(path)


def database_experiments_open(local_deployment: bool) -> apis.db.exp_packages.DatabaseExperiments:
    # VV: FIXME This is a bad place for this method, need to figure out a better way to decide where to store db
    path = _generate_path_to_storage_file(local_deployment, "experiments.json")
    return apis.db.exp_packages.DatabaseExperiments(path)


def secrets_git_open(local_deployment: bool) -> apis.db.secrets.SecretsStorageTemplate:
    if local_deployment is False:
        return apis.db.secrets.KubernetesSecrets(namespace=MONITORED_NAMESPACE)
    else:
        path = _generate_path_to_storage_file(local_deployment, "git-secrets.json")
        return apis.db.secrets.DatabaseSecrets(db_path=path)


class KubernetesObjectNotFound(Exception):
    def __init__(self, k8s_kind, k8s_name):
        # type: (str, str) -> None
        self.kind = k8s_kind
        self.name = k8s_name
        self.message = 'Kubernetes object %s/%s does not exist' % (k8s_kind, k8s_name)

        super(KubernetesObjectNotFound, self).__init__()
        
    def __str__(self):
        return self.message
    

def apply_k8s_object(k8s_kind, k8s_name, k8s_object):
    namespace = MONITORED_NAMESPACE
    api = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient())

    apply = {
        'secret': api.patch_namespaced_secret,
        'configmap': api.patch_namespaced_config_map
    }[k8s_kind]

    return apply(name=k8s_name, namespace=namespace, body=k8s_object, pretty=True)


def create_k8s_object(k8s_kind, k8s_object):
    namespace = MONITORED_NAMESPACE
    api = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient())

    apply = {
        'secret': api.create_namespaced_secret,
    }[k8s_kind]

    return apply(namespace=namespace, body=k8s_object)


def get_k8s_object(k8s_kind, k8s_name):
    namespace = MONITORED_NAMESPACE
    api = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient())

    get = {
        'secret': api.list_namespaced_secret,
        'pvc': api.list_namespaced_persistent_volume_claim,
        'configmap': api.list_namespaced_config_map,
    }[k8s_kind]

    response = get(field_selector='metadata.name=%s' % k8s_name, namespace=namespace)
    if len(response.items) == 0:
        raise KubernetesObjectNotFound(k8s_kind, k8s_name)

    return response.items[0]


def validate_secret(name, keys):
    """Retrieve Secret kubernetes object and make sure that it contains the expected @keys

    Returns:
        None on success

    Raises:
        ValueError: if there's an issue with the object
        KubernetesObjectNotFound: if the object does not exist
    """
    try:
        secret = get_k8s_object('secret', name)  # type: kubernetes.client.V1Secret
        if not secret.data:
            raise ValueError("does not contain fields")

        missing = [k for k in keys if k not in secret.data]
        if missing:
            raise ValueError("missing field(s) %s" ', '.join(missing))
    except KubernetesObjectNotFound:
        raise
    except Exception as e:
        raise ValueError("unexpected exception %s" % e)


def validate_secret_git_ssh(name):
    return validate_secret(name, ['ssh', 'known_hosts'])


def validate_secret_git_oauth(name):
    return validate_secret(name, ['oauth-token'])

def decode_secret_git_oauth(name):
    secret = get_k8s_object('secret', name)
    return base64.b64decode(secret.data['oauth-token']).decode()

def validate_secret_imagepull(name):
    return validate_secret(name, ['.dockerconfigjson'])


def validate_pvc(name):
    get_k8s_object('pvc', name)


def validate_config(configuration):
    # type: (Dict[str, Any]) -> None
    """Validates contents of a configuration dictionary (typically found in /etc/config.json)

    Keys in @configuration which are not part of Schema are considered errors.

    `gitsecret` and `gitsecret-oauth` are optional but at least one of them must exist

    Returns:
        None: On success returns None

    Raises:
        ValueError: when the configuration does not match the expected schema OR some of the objects it references
          are not properly configured and/or do not exist

    Schema::

        workflow-monitoring-image: "container image:str"
        image: "container image:str"
        s3-fetch-files-image: "container image:str"
        gitsecret: "Name of Secret object which contains the keys: `ssh` and `known_hosts`"
        gitsecret-oauth: "Name of Secret object which contains the key `oauth-token`"
        imagePullSecrets:
          - "Name of Secret object which contains the key `.dockerconfigjson`"
        inputdatadir: "string"
        workingVolume: "name of a PVC"
    """
    errors = []

    expected = ['image', 's3-fetch-files-image',
                'imagePullSecrets', 'inputdatadir', 'workingVolume']
    optional = ['gitsecret', 'gitsecret-oauth', 'workflow-monitoring-image', 'default-arguments']

    missing = [k for k in expected if k not in configuration]
    unknown = [k for k in configuration if k not in expected + optional]
    logger = logging.getLogger('validate')

    if 'gitsecret' not in configuration and 'gitsecret-oauth' not in configuration:
        logger.warning("no git secrets in configuration - no default secrets for git clone")
    elif 'gitsecret' not in configuration:
        logger.warning("gitsecret not in configuration - no default SSH key for git@ clone urls")
    elif 'gitsecret-oauth' not in configuration:
        logger.warning("gitsecret-oauth not in configuration - no default OAuth Token for https:// clone urls ")

    for k in missing:
        errors.append('Missing key %s' % k)

    for k in unknown:
        possibilities = difflib.get_close_matches(k, expected+optional, cutoff=0.8)
        if possibilities:
            errors.append('Unknown key "%s" did you mean "%s" ?' % (k, possibilities[0]))
        else:
            errors.append('Unknown key "%s", valid keys are %s' % (k, ', '.join(missing)))

    if 'gitsecret' in configuration:
        try:
            validate_secret_git_ssh(configuration['gitsecret'])
        except Exception as e:
            errors.append('Invalid GitSecretSSH object %s: %s' % (configuration['gitsecret'], e))

    if 'gitsecret-oauth' in configuration:
        try:
            validate_secret_git_oauth(configuration['gitsecret-oauth'])
        except Exception as e:
            errors.append('Invalid GitSecretOAuth object %s: %s' % (configuration['gitsecret-oauth'], e))

    pull_secrets = configuration.get('imagePullSecrets', [])
    for name in pull_secrets:
        try:
            validate_secret_imagepull(name)
        except Exception as e:
            errors.append('Invalid docker pull object %s: %s' % (name, e))

    # VV: Default command line arguments to the orchestrator of virtual experiments.
    # The format is a list of dictionaries. A key of a nested dictionary is the name of a parameter.
    # For example, the arguments `-m author:mary -m project:surrogate-models --registerWorkflow=y` would
    # be encoded as: [{"-m": "author:mary"}, {"-m": "project:surrogate-models", "--registerWorkflow": "y"}].
    # The above enables you to provide the same argument multiple times ("-m" for user-metadata key-value).
    default_arguments = configuration.get('default-arguments', [])
    schema_arguments = FlowIR.ValidateMany({six.string_types: FlowIR.PrimitiveTypes})
    try:
        errors.extend(FlowIR.validate_object_schema(default_arguments, schema_arguments, "default-command-line"))
    except Exception as e:
        msg = f"Unable to validate default-command-line due to {e}"
        logger.warning(msg)
        logger.warning(f"Traceback: {traceback.format_exc()}")
        errors.append(ValueError(msg))

    if errors:
        raise ValueError('\n'.join(errors))


def get_config_map_data(name):
    # type: (str) -> Dict[str, str]
    """Returns the data field of a ConfigMap"""
    obj = get_k8s_object('configmap', name)
    cf = cast("kubernetes.client.models.V1ConfigMap", obj)
    return cf.data


def configuration_from_configmap(name):
    # type: (str) -> Dict[str, Any]
    """Extracts configuration straight from the Kubernetes ConfigMap"""
    return json.loads(get_config_map_data(name)['config.json'])


def get_config_json_path():
    if CONFIG_JSON_PATH:
        return os.path.isfile(CONFIG_JSON_PATH)

    paths = ['/etc/consumable/config.json', '/etc/config.json', 'config.json']
    for k in paths:
        if os.path.isfile(k):
            return k


def setup_config(
        local_deployment: bool,
        validate: bool = False,
        from_config_map: Optional[str] = ...
) -> Dict[str, Any]:
    """Loads the Consumable Computing configuration, may validate it before returning its contents.

    If apis.models.constants.LOCAL_DEPLOYMENT is True this method returns an empty dictionary

    Arguments:
        local_deployment: Whether the API is running in LOCAL_DEPLOYMENT mode (i.e. not inside
            Kubernetes)
        validate(bool): Set to True to validate contents of configuration file
        from_config_map(Optional[str]): Read configuration from kubernetes ConfigMap, if unset will instead read
            configuration from the config.json file that @get_config_json_path() returns.

    Returns:
        The dictionary
            {
                "image": "container image:str for st4sd-runtime-core"
                "s3-fetch-files-image": "container image:str for the s3-fetch-files image"
                "gitsecret" (optional): "Name of Secret object which contains the keys: `ssh` and `known_hosts`"
                "gitsecret-oauth" (optional): "Name of Secret object which contains the key `oauth-token`"
                "imagePullSecret":
                  - "Name of Secret object which contains the key `.dockerconfigjson`"
                "inputdatadir": "the directory that runtime service uses to store metadata files
                   (e.g. experiments.json, relationships.json, etc)"
                "workingVolume": "name of the PVC that workflow instances will use to store their outputs",
                "default-arguments": [
                    {
                        "--some-parameter": "the value",
                        "-o": "other value"
                    }
                ]
            }
    """
    logger = logging.getLogger(__name__)

    if from_config_map is ...:
        if local_deployment is False:
            from_config_map = ConfigMapWithParameters

    if from_config_map is ...:
        path = get_config_json_path()

        if path is not None:
            with open(path) as f:
                configuration = json.load(f)
        elif local_deployment is False:
            raise apis.models.errors.ApiError("Could not load configuration file")
        else:
            local_storage = LOCAL_STORAGE or os.getcwd()
            ret = {'inputdatadir': local_storage}
            logger.info(f"API is in LOCAL_DEPLOYMENT and CONFIG_JSON_PATH is unset - configuration={ret}")

            return ret
    else:
        configuration = configuration_from_configmap(from_config_map)

    logger.info(f"Loaded configuration {json.dumps(configuration)}")

    if validate:
        validate_config(configuration)

    return configuration


def parse_configuration(
        local_deployment: bool,
        validate: bool = False,
        from_config_map: Optional[str] = ...
) -> apis.models.virtual_experiment.Configuration:
    f"""Loads the configuration settings

    Arguments:
        local_deployment: Whether API is running in LOCAL_DEPLOYMENT mode
        validate(bool): Set to True to validate contents of configuration file
        from_config_map(Optional[str]): Read configuration from kubernetes ConfigMap, if unset will instead read
            configuration from the config.json file that @get_config_json_path() returns.

    Returns:
        apis.models.virtual_experiment.Configuration
    """
    configuration = setup_config(local_deployment=local_deployment, validate=validate,
                                 from_config_map=from_config_map)

    return apis.models.virtual_experiment.Configuration.parse_obj(configuration)
