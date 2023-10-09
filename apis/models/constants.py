# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis
#   Theo Kanakis

import os
from typing import Mapping, Text, Optional


TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

class EnvVar:

    def __init__(
            self,
            key: Text,
            default: Text = None,
            env: Optional[Mapping[Text, Text]] = None,
    ):
        if env is None:
            env = os.environ.copy()

        self.key = key
        self.default = default
        self._env = env

    def __str__(self) -> Text:
        return self.get()
    
    def get(self) -> Text:
        return self._env.get(self.key, self.default)
    
    @property
    def value(self) -> Text:
        return self.get()
    
    @property
    def defined(self) -> bool:
        return self.key in self._env
    
    @property
    def enabled(self) -> bool:
        return (self.value or "").lower() in ["true","yes","enabled","1"]


# VV: This is defined by https://github.ibm.com/st4sd/st4sd-deployment
ConfigMapWithParameters = EnvVar('CONFIGMAP_NAME', 'st4sd-runtime-service').value
WORKING_VOLUME_MOUNT = '/tmp/workdir'

# VV: st4sd-runtime-service propagates these to `st4sd-runtime-core` via:
# '--flowConfigPath=/config/st4sd-runtime-core/config.yaml'
# The 2 env-variables below are defined in https://github.ibm.com/st4sd/st4sd-deployment
DATASTORE_MONGODB_PROXY_ENDPOINT = EnvVar(
    'DATASTORE_MONGODB_PROXY_ENDPOINT', '${DATASTORE_MONGODB_PROXY_ENDPOINT} is unset').value
DATASTORE_GATEWAY_REGISTRY = EnvVar(
    'DATASTORE_GATEWAY_REGISTRY', '${DATASTORE_GATEWAY_REGISTRY} is unset').value

# VV: This is expected to be set using the Kubernetes Downward api.
# It instructs the st4sd-runtime-service to monitor a single namespace.
# In the future we could consider monitoring multiple namespaces e.g.
# to make the experiment catalog available to many groups that are using
# the same shared cluster.
MONITORED_NAMESPACE = EnvVar('MONITORED_NAMESPACE', "${MONITORED_NAMESPACE} is unset").value

# VV: This is for https://github.ibm.com/st4sd/st4sd-runtime-k8s
# it used to be hpsys.ie.ibm.com/v1alpha1
K8S_WORKFLOW_GROUP = EnvVar("K8S_WORKFLOW_GROUP", "st4sd.ibm.com").value
K8S_WORKFLOW_VERSION = EnvVar("K8S_WORKFLOW_VERSION", "v1alpha1").value
K8S_WORKFLOW_PLURAL = EnvVar("K8S_WORKFLOW_PLURAL", "workflows").value

# VV: This is for https://github.com/datashim-io/datashim
K8S_DATASET_GROUP = EnvVar("K8S_DATASET_GROUP", "com.ie.ibm.hpsys").value
K8S_DATASET_VERSION = EnvVar("K8S_DATASET_VERSION", "v1alpha1").value
K8S_DATASET_PLURAL = EnvVar("K8S_DATASET_PLURAL", "datasets").value

# VV: Use a JSON file instead of a ConfigMap object
CONFIG_JSON_PATH = EnvVar("ST4SD_CONFIG_JSON_PATH").value

# VV: This is the ONLY place we are allowed to store "derived" packages
ROOT_STORE_DERIVED_PACKAGES = EnvVar("ST4SD_ROOT_STORE_DERIVED_PACKAGES", "/tmp/workdir/derived").value

PATH_TO_RUNTIME_SERVICE_API_KEY = EnvVar("ST4SD_PATH_TO_RUNTIME_SERVICE_BEARER_KEY",
                                         "/var/run/secrets/kubernetes.io/serviceaccount/token").value
URL_RUNTIME_SERVICE = EnvVar('ST4SD_URL_RUNTIME_SERVICE', "https://st4sd-authentication:8888").value

# VV: Most of the runtime-service APIs can function without the need for a Kubernetes environment.
# Exporting `LOCAL_DEPLOYMENT=True` causes the runtime-service APIs to run in `local` mode.
# Some of its functionality will be disabled
LOCAL_DEPLOYMENT = EnvVar("LOCAL_DEPLOYMENT", "False").enabled

# VV: Points to the directory which will contain the files that the runtime service uses
# to store metadata (databases, etc)
LOCAL_STORAGE = EnvVar("LOCAL_STORAGE", os.getcwd()).value

# VV: Contains the name of a secret which includes the keys:
# S3_ACCESS_KEY_ID
# S3_SECRET_ACCESS_KEY
# S3_ENDPOINT
# S3_BUCKET
# S3_PREFIX
# When set, it switches on the `/internal-experiments/` APIs
S3_CONFIG_SECRET_NAME = EnvVar("S3_CONFIG_SECRET_NAME", "").value

# VV: The suffix under which to store the internal experiments under the internal storage (e.g. S3)
S3_ROOT_INTERNAL_EXPERIMENTS = EnvVar("S3_ROOT_INTERNAL_EXPERIMENTS", "experiments").value
