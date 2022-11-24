# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


import os

TIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

# VV: This is defined by https://github.ibm.com/st4sd/st4sd-deployment
ConfigMapWithParameters = os.environ.get('CONFIGMAP_NAME', 'st4sd-runtime-service')
WORKING_VOLUME_MOUNT = '/tmp/workdir'

# VV: st4sd-runtime-service propagates these to `st4sd-runtime-core` via:
# '--flowConfigPath=/config/st4sd-runtime-core/config.yaml'
# The 2 env-variables below are defined in https://github.ibm.com/st4sd/st4sd-deployment
DATASTORE_MONGODB_PROXY_ENDPOINT = os.environ.get(
    'DATASTORE_MONGODB_PROXY_ENDPOINT', '${DATASTORE_MONGODB_PROXY_ENDPOINT} is unset')
DATASTORE_GATEWAY_REGISTRY = os.environ.get(
    'DATASTORE_GATEWAY_REGISTRY', '${DATASTORE_GATEWAY_REGISTRY} is unset')

# VV: This is expected to be set using the Kubernetes Downward api.
# It instructs the st4sd-runtime-service to monitor a single namespace.
# In the future we could consider monitoring multiple namespaces e.g.
# to make the experiment catalog available to many groups that are using
# the same shared cluster.
MONITORED_NAMESPACE = os.environ.get('MONITORED_NAMESPACE', "${MONITORED_NAMESPACE} is unset")

# VV: This is for https://github.ibm.com/st4sd/st4sd-runtime-k8s
# it used to be hpsys.ie.ibm.com/v1alpha1
K8S_WORKFLOW_GROUP = os.environ.get("K8S_WORKFLOW_GROUP", "st4sd.ibm.com")
K8S_WORKFLOW_VERSION = os.environ.get("K8S_WORKFLOW_VERSION", "v1alpha1")
K8S_WORKFLOW_PLURAL = os.environ.get("K8S_WORKFLOW_PLURAL", "workflows")

# VV: This is for https://github.com/datashim-io/datashim
K8S_DATASET_GROUP = os.environ.get("K8S_DATASET_GROUP", "com.ie.ibm.hpsys")
K8S_DATASET_VERSION = os.environ.get("K8S_DATASET_VERSION", "v1alpha1")
K8S_DATASET_PLURAL = os.environ.get("K8S_DATASET_PLURAL", "datasets")

# VV: Use a JSON file instead of a ConfigMap object
CONFIG_JSON_PATH = os.environ.get("ST4SD_CONFIG_JSON_PATH")

# VV: This is the ONLY place we are allowed to store "derived" packages
ROOT_STORE_DERIVED_PACKAGES = os.environ.get("ST4SD_ROOT_STORE_DERIVED_PACKAGES", "/tmp/workdir/derived")

PATH_TO_RUNTIME_SERVICE_API_KEY = os.environ.get("ST4SD_PATH_TO_RUNTIME_SERVICE_BEARER_KEY",
                                                 "/var/run/secrets/kubernetes.io/serviceaccount/token")
URL_RUNTIME_SERVICE = os.environ.get('ST4SD_URL_RUNTIME_SERVICE', "https://st4sd-authentication:8888")
