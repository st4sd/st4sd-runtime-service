# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

import base64
import json
import threading
import traceback
from typing import List, Optional, Dict, Any

import kubernetes
import kubernetes.client.rest
from flask import current_app, request
from flask_restx import Resource

import apis.models
import apis.models.constants
import utils

api = apis.models.api_image_pull_secret

example_imagePullSecret_get_all = apis.models.example_imagePullSecret_get_all
example_imagePullSecret_get = apis.models.example_imagePullSecret_get
model_imagePullSecret_full = apis.models.model_imagePullSecret_full


class SecretObjectAlreadyExists(Exception):
    def __init__(self, secret):
        self.secret = secret
        self.message = "Secret object %s already exists" % self.secret
        super(SecretObjectAlreadyExists, self).__init__()

    def __str__(self):
        return self.message


class ImagePullSecretAlreadyExists(SecretObjectAlreadyExists):
    def __init__(self, secret):
        super(ImagePullSecretAlreadyExists, self).__init__(secret)
        self.message = "imagePullSecret %s already exists" % self.secret

    def __str__(self):
        return self.message


def b64_decode(which):
    # type: (str) -> str
    return base64.standard_b64decode(which.encode('utf-8')).decode('utf-8')


def b64_encode(which):
    # type: (str) -> str
    return base64.standard_b64encode(which.encode('utf-8')).decode('utf-8')


def get_all_image_pull_secrets(which=None, out_config=None):
    # type: (Optional[List[str]], Optional[Dict[str, Any]]) -> Dict[str, List[str]]
    """List all imagePullSecrets, returns a Dictionary whose keys are names of Secrets and values lists of docker
       registry urls.

    May optionally retrieve information of a selection of imagePullSecrets instead of all known ones

    May optionally return loaded out_config if out_config is not set to None
    """
    configuration = utils.parse_configuration(local_deployment=apis.models.constants.LOCAL_DEPLOYMENT)

    if out_config is not None:
        out_config.update(configuration.model_dump().copy())

    secrets_names = configuration.imagePullSecrets

    if which is not None:
        secrets_names = [x for x in secrets_names if x in which]

    # VV: A kubernetes.io/dockerconfigjson secret contains a `.dockerconfigjson` field which contains a base64
    # encoded string, which when decoded is a valid JSON dictionary with the keys:
    # { "auths": {
    #    "<docker registry url>" : {
    #       "auth": "base64(%s:%s) % (username, password)",
    #       "email": <an e-mail">,
    #       "username": "<the username that the secret uses to authenticate to the docker registry url>",
    #       "password": "<the password that the secret uses to authenticate to the docker registry url>",
    #     }
    # }
    lbl = '.dockerconfigjson'
    ret = {}
    for name in secrets_names:
        try:
            secret = utils.get_k8s_object('secret', name)  # type: kubernetes.client.models.V1Secret

            dockerconfigjson = secret.data[lbl]
            dockerconfigjson = b64_decode(dockerconfigjson)
            dockerconfigjson = json.loads(dockerconfigjson)

            for auth in dockerconfigjson.get('auths', {}):
                if name not in ret:
                    ret[name] = []
                ret[name].append(auth)
        except Exception as e:
            current_app.logger.warning("Traceback: %s\nException: %s for %s" % (traceback.format_exc(), e, id))
            current_app.logger.warning("Cannot extract docker registries of %s due to %s - will continue" % (name, e))

    return ret


@api.route('/')
@api.response(200, "Success", example_imagePullSecret_get_all)
class ImagePullSecretsList(Resource):
    def get(self):
        """List all imagePullSecrets, returns a Dictionary whose keys are names of Secrets and values lists of docker
        registry urls.
        """
        try:
            return get_all_image_pull_secrets()
        except Exception as e:
            current_app.logger.warning("Traceback: %s\nException: %s for %s" % (traceback.format_exc(), e, id))
            api.abort(500, "Exception when listing imagePullSecrets - %s" % e)


@api.route('/<id>/', doc=False)
@api.route('/<id>')
@api.param('id', 'The imagePullSecret identifier')
@api.response(404, 'imagePullSecret not found')
@api.response(500, 'Internal error while retrieving imagePullSecret details')
class ImagePullSecret(Resource):
    mutex = threading.RLock()
    @api.response(200, "Success", example_imagePullSecret_get)
    def get(self, id):
        """Get imagePullSecret, returns a list of strings, each of which is a docker registry that the specified
        imagePullSecret has pull access to
        """
        try:
            image_pull_secrets = get_all_image_pull_secrets([id])

            if id in image_pull_secrets:
                return image_pull_secrets[id]
            else:
                api.abort(404, "Unknown imagePullSecret %s" % id)
        except Exception as e:
            current_app.logger.warning("Traceback: %s\nException: %s for %s" % (traceback.format_exc(), e, id))
            api.abort(500, "Exception when listing imagePullSecret %s - %s" % (id, e))

    @classmethod
    def _build_imagePullSecret_data(cls, username: str, password: str, registry_url: str) \
            -> Dict[str, Any]:
        """Helper function to build the data field of a kubernetes.client.models.V1Secret"""
        lbl = '.dockerconfigjson'  # VV: Name of the field under data that k8s expends to find the docker credentials
        dockerconfigjson = {
            'auths': {registry_url: {
                'auth': b64_encode('%s:%s' % (username, password)),
                'username': username,
                'password': password,
        }}}
        dockerconfigjson = json.dumps(dockerconfigjson)
        dockerconfigjson = b64_encode(dockerconfigjson)
        data = {lbl: dockerconfigjson}
        return data

    @classmethod
    def build_imagePullSecret(cls, secret_name: str, username: str, password: str, registry_url: str) \
            -> kubernetes.client.models.V1Secret:
        """Build a kubernetes.client.models.V1Secret to hold docker-registry credentials """
        data = cls._build_imagePullSecret_data(username=username, password=password, registry_url=registry_url)

        return kubernetes.client.models.V1Secret(
            data=data,
            type='kubernetes.io/dockerconfigjson',
            metadata=kubernetes.client.models.V1ObjectMeta(name=secret_name,
                                                           labels={'creator': 'st4sd-runtime-service'})
        )

    def _try_create_imagePullSecret(self, secret_name: str, username: str, password: str,
                                    registry_url: str, update: bool):
        """Create a new imagePullSecret, will be available for use, typically, within 2 minutes

        if the imagePullSecret with `id` already exists return False if update is set to False, otherwise continue

        if the imagePullSecret is not defined in the configuration BUT the associated kubernetes object exists:

        - if update is True: this method will override the contents of the Secret object with the contents of the
            request and return True.
        - if update is False: the method raises SecretObjectAlreadyExists

        Arguments:
            secret_name(bool): Name of the secret Kubernete object that will be created/updated
            username(bool): Username credentials to docker-registry
            password(bool): Password credentials to docker-registry
            registry_url(bool): The url of the docker registry
            update(bool): When False method will raise exceptions if an imagePullSecret entry in utils.setup_config()
                for the Secret already exists or the Secret Object already exists. Setting this to True will update the
                kubernetes object and also make sure that there is an imagePullSecret entry in utils.setup_config()

        Notes:
            utils.setup_config() returns the data/config.json contents of a ConfigMap (by default
            st4sd-runtime-service).
            The Consumable Computing REST-API records in this ConfigMap "configuration" information, including:
                imagePullSecrets which workflow instances can forward to pods they spawn so that the latter can
                 pull images.
        Returns:
            bool: False if method creates a new Secret object, True if it updates an existing Secret object

        Raises:
            ImagePullSecretAlreadyExists: If imagePullSecret already exists and update is set to False
            SecretObjectAlreadyExists: If imagePullSecret does not exist in the configuration but a kubernetes Secret
              object with the same name already exists
        """
        cf_map_lbl = 'config.json'

        configuration = utils.setup_config(local_deployment=apis.models.constants.LOCAL_DEPLOYMENT)
        updated = secret_name in configuration['imagePullSecrets']

        def update_object():
            secret = utils.get_k8s_object('secret', secret_name)  # type: kubernetes.client.models.V1Secret
            data = secret.data or {}
            data.update(self._build_imagePullSecret_data(username=username,
                                                         password=password, registry_url=registry_url))
            current_app.logger.info("Updating imagePullSecret object %s" % secret_name)

            # VV: This will completely overwrite the object
            utils.apply_k8s_object('secret', secret_name, secret)

        def create_object():
            secret = self.build_imagePullSecret(secret_name=secret_name, username=username, password=password,
                                                registry_url=registry_url)
            utils.create_k8s_object('secret', secret)

        if updated and update is False:
            raise ImagePullSecretAlreadyExists(secret_name)
        elif updated:
            # VV: There's no need to update the configmap (or the json file that's mounted for it) as it already
            #     contains the id of the kubernetes secret
            update_object()

            return True
        else:
            # VV: There's no entry of the secret in the configMap but there might still be an object, in that case
            #     we have to assume that there's something terribly wrong and do not attempt to create/update the secret
            try:
                _ = utils.get_k8s_object('secret', secret_name)
            except utils.KubernetesObjectNotFound as e:
                # VV: this is what we're hoping to get
                secret_object_exists = False
            else:
                secret_object_exists = True
                if update is False:
                    current_app.logger.warning("Kubernetes secret %s already exists but there's no entry for it in "
                                               "the configuration: %s - abort" % (secret_name, configuration))
                    raise SecretObjectAlreadyExists(secret_name)

            if secret_object_exists:
                update_object()
            else:
                create_object()

            if secret_name in configuration['imagePullSecrets']:
                if update is False:
                    # VV: This is a very weird race-condition; WE were the ones to create the secret, but somehow the
                    #     secret was already listed in he ConfigMap ...
                    current_app.logger.warning("Created imagePullSecret %s but found it already exists in "
                                               "configMap: %s" % (secret_name, configuration))
                    raise ImagePullSecretAlreadyExists(secret_name)
                else:
                    # VV: Nothing else to do, the imagePullSecret entry is already there
                    return not secret_object_exists

            configuration['imagePullSecrets'].append(secret_name)
            json_config = json.dumps(configuration, indent=2)

            # VV: this will only overwrite data.<cf_map_lbl>
            utils.apply_k8s_object('configmap', utils.ConfigMapWithParameters, {'data': {cf_map_lbl: json_config}})

            return False

    def serve_upsertImagePullSecret(self, secret_name: str, username: str, password: str,
                                    registry_url: str, update: bool):
        with self.mutex:
            try:
                self._try_create_imagePullSecret(secret_name=secret_name, username=username, password=password,
                                                 registry_url=registry_url, update=update)
                return
            except ImagePullSecretAlreadyExists:
                if id in utils.setup_config(local_deployment=apis.models.constants.LOCAL_DEPLOYMENT)[
                    'imagePullSecrets']:
                    msg = "imagePullSecret already exists"
                else:
                    msg = "imagePullSecret already exists but has not become available yet"
                current_app.logger.warning("msg")
                api.abort(409, msg)
            except SecretObjectAlreadyExists:
                msg = 'imagePullSecret %s conflicts with existing Secret object with the same name' % id
                current_app.logger.warning(msg)
                api.abort(422, msg)
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
                msg = "Unable to create imagePullSecret %s because of %s" % (id, e)
                current_app.logger.warning('Traceback:%s\n%s' % (traceback.format_exc(), msg))
                api.abort(500, msg)

    @api.expect(model_imagePullSecret_full)
    @api.response(200, "Success")
    @api.response(409, 'imagePullSecret already exists')
    @api.response(422, 'imagePullSecret conflicts with existing Secret object with the same name')
    def post(self, id):
        """Create a new imagePullSecret.

        This method does not check whether the contents of the Secret object are correct. In the remainder of this
        text @configuration IS the contents of the `config.json` key of the `st4sd-runtime-service` ConfigMap
        (or the ConfigMap object that the environment variable `CONSUMABLE_COMPUTING_CONFIGMAP_NAME` points to)

        if imagePullSecret is already defined in in the @configuration this method returns 422
        (imagePullSecret already exists).

        if the imagePullSecret is not defined in the @configuration BUT the associated kubernetes object exists
        this method will NOT override the contents of the Secret object with the contents of the request. It will
        return 409 (imagePullSecret conflict with existing Secret object with the same name)

        if the imagePullSecret is successfully created it will be available to use within a couple of minutes after
        Kubernetes asynchronously refreshes the file whose contents are the @configuration.
        """
        # VV: Make sure that exactly 1 thread at a time can create an imagePullSecret
        #     it's OK to get a list of secrets while a writer is modifying it
        data = request.json
        username = data['username']
        password = data['password']
        registry_url = data['server']

        current_app.logger.info("Creating imagePullSecret %s for registry %s" % (id, registry_url))
        self.serve_upsertImagePullSecret(secret_name=id, username=username, password=password,
                                         registry_url=registry_url, update=False)
        return "Success"

    @api.expect(model_imagePullSecret_full)
    @api.response(200, "Success")
    def put(self, id):
        """Update an existing imagePullSecret.

        This method does not check whether the contents of the Secret object are correct. In the remainder of this
        text @configuration IS the contents of the `config.json` key of the `st4sd-runtime-service` ConfigMap
        (or the ConfigMap object that the environment variable `CONSUMABLE_COMPUTING_CONFIGMAP_NAME` points to)

        if imagePullSecret is already defined in in the @configuration this method returns 422
        (imagePullSecret already exists).

        if the imagePullSecret is not defined in the @configuration BUT the associated kubernetes object exists
        this method will NOT override the contents of the Secret object with the contents of the request. It will
        return 409 (imagePullSecret conflict with existing Secret object with the same name)

        if the imagePullSecret is successfully created it will be available to use within a couple of minutes after
        Kubernetes asynchronously refreshes the file whose contents are the @configuration.
        """
        # VV: Make sure that exactly 1 thread at a time can create an imagePullSecret
        #     it's OK to get a list of secrets while a writer is modifying it
        data = request.json
        username = data['username']
        password = data['password']
        registry_url = data['server']

        current_app.logger.info("Creating imagePullSecret %s for registry %s" % (id, registry_url))
        self.serve_upsertImagePullSecret(secret_name=id, username=username, password=password,
                                         registry_url=registry_url, update=True)
        return "Success"
