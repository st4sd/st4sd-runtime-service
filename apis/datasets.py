# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


import json
import traceback
from typing import cast, Dict

import kubernetes.client
import kubernetes.client.rest
from flask import request, current_app
from flask_restx import Resource

import apis.models
import utils

api = apis.models.api_datasets
s3_model = apis.models.s3_model


def pvc_list(api_instance=None, namespace=None):
    # type: (kubernetes.client.CoreV1Api, str) -> Dict[str, kubernetes.client.V1PersistentVolumeClaim]
    if namespace is None:
        namespace = utils.MONITORED_NAMESPACE

    if api_instance is None:
        api_instance = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient())

    pvcs = api_instance.list_namespaced_persistent_volume_claim(namespace=namespace)
    pvcs = cast("kubernetes.client.V1PersistentVolumeClaimList", pvcs)

    return {item.metadata.name: item for item in pvcs.items}


def configmap_list(api_instance=None, namespace=None):
    # type: (kubernetes.client.CoreV1Api, str) -> Dict[str, kubernetes.client.V1ConfigMap]
    if namespace is None:
        namespace = utils.MONITORED_NAMESPACE

    if api_instance is None:
        api_instance = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient())

    configmaps = api_instance.list_namespaced_config_map(namespace=namespace)
    configmaps = cast("kubernetes.client.V1ConfigMapList", configmaps)

    return {item.metadata.name: item for item in configmaps.items}


def dataset_list(api_instance=None, namespace=None,
                 k8s_group=utils.K8S_DATASET_GROUP, k8s_version=utils.K8S_DATASET_VERSION,
                 k8s_plural=utils.K8S_DATASET_PLURAL,):
    if namespace is None:
        namespace = utils.MONITORED_NAMESPACE

    if api_instance is None:
        api_instance = kubernetes.client.CustomObjectsApi(kubernetes.client.ApiClient())

    api_response = api_instance.list_namespaced_custom_object(k8s_group, k8s_version, namespace, k8s_plural)
    return {item['metadata']['name']: item for item in api_response.get("items", [])}


def dataset_create(name, accessKeyID, secretAccessKey, bucket,
                   endpoint, region="", api_instance=None, namespace=None,
                   k8s_group=utils.K8S_DATASET_GROUP,
                   k8s_version=utils.K8S_DATASET_VERSION, k8s_plural=utils.K8S_DATASET_PLURAL,):
    if namespace is None:
        namespace = utils.MONITORED_NAMESPACE

    if api_instance is None:
        api_instance = kubernetes.client.CustomObjectsApi(kubernetes.client.ApiClient())

    body = {
        'apiVersion': '%s/%s' % (k8s_group, k8s_version),
        'kind': 'Dataset',
        'metadata': {
            'name': name,
        },
        'spec': {
            'local': {
                'type': 'COS',
                'accessKeyID': accessKeyID,
                'secretAccessKey': secretAccessKey,
                'endpoint': endpoint,
                'bucket': bucket,
                'region': region
            }
        }
    }

    return api_instance.create_namespaced_custom_object(
        k8s_group, k8s_version, namespace, k8s_plural, body)

@api.route('/')
@api.response(400, 'Unable to list Dataset resource - ensure that Dataset Lifecycle Framework is installed')
class DatasetList(Resource):
    def get(self):
        try:
            datasets = [x for x in dataset_list()]
        except Exception as e:
            current_app.logger.warning(traceback.format_exc())
            current_app.logger.warning("Failed to list datasets: %s" % e)
            api.abort(400, "Unable to list datasets because of %s" % e)
        else:
            current_app.logger.info("Available datasets: %s" % datasets)
            return datasets


@api.route('/s3/<id>')
@api.param('id', 'The Dataset identifier')
@api.response(400, 'Unable to create dataset - ensure that the Dataset Lifecycle Framework is installed')
@api.response(409, 'Dataset already exists')
class Dataset(Resource):
    @api.expect(s3_model)
    def post(self, id):
        s3 = request.json

        try:
            dataset_create(id, **s3)
        except Exception as e:
            if isinstance(e, kubernetes.client.rest.ApiException):
                try:
                    body = json.loads(e.body)
                except Exception:
                    body = {}
                if isinstance(body, dict):
                    message = (body or {}).get('message', "Kubernetes raised exception %s" % e.status)
                else:
                    message = "Kubernetes raised exception"
                api.abort(e.status, message)
            current_app.logger.warning(traceback.format_exc())
            current_app.logger.warning("Failed to create dataset: %s" % e)
            api.abort(400, "Unable to create dataset because of %s" % e)
        current_app.logger.info("Created dataset %s" % id)
        return "OK"
