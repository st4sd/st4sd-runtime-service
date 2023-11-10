# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis
#   Alessandro Pomponio
#   Yiannis Gkoufas

from __future__ import annotations

import base64
import binascii
import copy
import datetime
import difflib
import json
import os
import pprint
import random
import string
import traceback
import typing
from typing import List, Dict, Optional, Any, Tuple

import experiment.model.errors
import experiment.model.frontends.flowir
import experiment.model.graph
import experiment.model.storage
import pydantic
import werkzeug.exceptions
import yaml
from flask import request, current_app
from flask_restx import Resource, reqparse
from kubernetes import client
from kubernetes.client.rest import ApiException

import apis.datasets
import apis.db.secrets
import apis.image_pull_secrets
import apis.instances
import apis.k8s
import apis.kernel.flask_utils
import apis.kernel.experiments
import apis.models
import apis.models.common
import apis.models.constants
import apis.models.errors
import apis.models.from_core
import apis.models.virtual_experiment
import apis.policy
import apis.policy.prefer_surrogate
import apis.policy.random_canary_surrogate
import apis.runtime.package
import apis.runtime.package_derived
import apis.storage
import apis.url_map
import apis.url_map
import utils
# from dummy_data import populate_from, append_to, update, delete_identifier, query_for_identifier
from utils import setup_config, get_k8s_object, KubernetesObjectNotFound

parser_formatting_dsl = apis.kernel.flask_utils.parser_formatting_dsl
parser_formatting_parameterised_package = apis.kernel.flask_utils.parser_formatting_parameterised_package

api = apis.models.api_experiments

mVirtualExperiment = apis.models.mVirtualExperiment
mPackageHistory = apis.models.mPackageHistory

mLambdaExperimentStart = apis.models.mLambdaExperimentStart
mExperimentStart = apis.models.mExperimentStart
mS3Store = apis.models.mS3Store


def do_format_parameterised_package(
        package: apis.models.virtual_experiment.ParameterisedPackage | Dict[str, Any],
        parser: reqparse.RequestParser
) -> Any:
    args = parser.parse_args()
    if isinstance(package, apis.models.virtual_experiment.ParameterisedPackage):
        what = package.dict(exclude_none=args.hideNone == "y")
    else:
        what = copy.deepcopy(package)

    if args.hideMetadataRegistry == "y":
        del what['metadata']['registry']

    if args.hideBeta == "y":
        if 'base' in what:
            for x in ['connections', 'includePaths', 'output', 'interface']:
                if x in what['base']:
                    del what['base'][x]

            if 'packages' in what['base']:
                many = what['base']['packages']
                for bp in many:
                    for x in ['graphs']:
                        if x in bp:
                            del bp[x]

    if args.outputFormat == "python":
        what = str(what)
    elif args.outputFormat == "python-pretty":
        what = pprint.pformat(what, width=120)

    return what



class InvalidExperimentDefinition(Exception):
    def __init__(self, exp_def: Dict[str, Any], underlying_exception: Exception):
        super(InvalidExperimentDefinition, self).__init__()
        self.underlying_exception = underlying_exception
        self.exp_def = exp_def
        self.message = f"Invalid experiment definition {pprint.pformat(exp_def)}\n" \
                       f"The error is {str(underlying_exception)}"

    def __str__(self):
        return self.message


class InvalidKeyInExperimentDefinition(InvalidExperimentDefinition):
    def __init__(self, exp_def: Dict[str, Any], root_route: str, invalid_field: str, valid_fields: List[str]):
        if root_route:
            invalid_field_route = '.'.join((root_route, invalid_field))
            valid_field_routes = ['.'.join((root_route, field)) for field in valid_fields]
        else:
            invalid_field_route = invalid_field
            valid_field_routes = valid_fields

        problem = ValueError(f"Experiment definition field {invalid_field_route} is invalid - "
                             f"there can only be {', '.join(valid_field_routes)} fields")

        if valid_fields:
            # VV: Try to guess what the user may have meant to type
            possibilities = difflib.get_close_matches(invalid_field, valid_fields)
            if len(possibilities):
                problem = ValueError(f"Experiment definition field {invalid_field_route} is invalid, "
                                     f"did you mean {possibilities[0]} ? - "
                                     f"there can only be {', '.join(valid_field_routes)} fields")

        super(InvalidKeyInExperimentDefinition, self).__init__(exp_def, problem)


class S3StoreInfo:
    def __init__(
            self, access_key_id: str, secret_access_key: str,
            end_point: str, bucket: str, bucket_path: str | None
    ):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.end_point = end_point
        self.bucket = bucket
        if bucket_path and bucket_path.startswith('/'):
            bucket_path = bucket_path[1:]
        self.bucket_path = bucket_path

    def to_bearer64(self):
        dictionary = {
            'S3_ACCESS_KEY_ID': self.access_key_id,
            'S3_SECRET_ACCESS_KEY': self.secret_access_key,
            'S3_END_POINT': self.end_point
        }
        json_str = json.dumps(dictionary)
        return base64.b64encode(json_str.encode('utf-8')).decode('utf-8')


class InaccessibleContainerRegistries(Exception):
    def __init__(self, container_registries: List[str]):
        super(InaccessibleContainerRegistries, self).__init__(
            "There is no known imagePullSecret entry for referenced "
            f"container registries {container_registries}")
        self.container_registries = container_registries


def make_pydantic_errors_jsonable(exc: pydantic.ValidationError) -> typing.List[typing.Dict[str, typing.Any]]:
    errors = exc.errors()

    for err in errors:
        if 'ctx' in err:
            del err['ctx']
        if 'url' in err:
            del err['url']

    return errors


class ExperimentFactory:
    def __init__(self, configuration, namespace, exp_conf=None, inputs=None, data=None,
                 additionalOptions=None, orchestrator_resources=None, metadata=None, variables=None,
                 s3=None, s3Store=None, datasetStoreURI=None, volumes=None, environmentVariables=None,
                 wf_group=utils.K8S_WORKFLOW_GROUP,
                 wf_version=utils.K8S_WORKFLOW_VERSION, wf_plural=utils.K8S_WORKFLOW_PLURAL,
                 dataset_group=utils.K8S_DATASET_GROUP, dataset_version=utils.K8S_DATASET_VERSION,
                 dataset_plural=utils.K8S_DATASET_PLURAL):
        environmentVariables = environmentVariables or {}
        self.env_variables = {str(x): str(environmentVariables[x]) for x in environmentVariables}

        volumes = volumes or []  # type: List[Dict[str, str]]
        exp_conf = exp_conf or {}
        inputs = inputs or []
        data = data or []
        additionalOptions = additionalOptions or []
        orchestrator_resources = orchestrator_resources or {}
        metadata = metadata or {}
        variables = variables or {}
        self.exp_conf = exp_conf

        self.s3_store = s3Store  # type: mS3Store
        self.dataset_store_uri = datasetStoreURI  # type: Optional[str]

        self.wf_group = wf_group
        self.wf_version = wf_version
        self.wf_plural = wf_plural

        self.dataset_group = dataset_group
        self.dataset_version = dataset_version
        self.dataset_plural = dataset_plural

        self.s3_credentials = (s3 or {}).copy()

        self.variables = variables

        self.data_files = [d['filename'] for d in data]
        self.input_files = [d['filename'] for d in inputs]

        self.data_entries = {}

        if self.variables:
            variables_content = ["[GLOBAL]"]
            for key in self.variables:
                variables_content.append(key + "=" + str(self.variables[key]))
            self.data_entries['variables.conf'] = '\n'.join(variables_content)

        if s3 is None:
            # VV: Potential conflicts between data and inputs
            self.data_entries.update({d['filename']: d['content'] for d in (data or []) + (inputs or [])})

        self.additionalOptions = list(additionalOptions or [])  # type: (List[str])
        self.orchestrator_resources = orchestrator_resources
        self.metadata = metadata

        self.configuration = configuration

        self.namespace = namespace

        rand = binascii.b2a_hex(os.urandom(3))
        self._rand_id = None

        # VV: Pick a name for the instance directory, the file on the disk will be: `<id>-<timestamp>.instance`
        #     this will be different from the name of the shadow-dir but that's fine because the monitor container
        #     uses the env var INSTANCE_NAME to search for the shadow-dir
        self.timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H%M%S.%f")

        self.package_name = self.workflow_id
        self.input_volume_id = f"input-{self.workflow_id}"
        self.secret_env_vars_id = f"envvars-{self.workflow_id}"

        # VV: Entries of volumes are compatible with pod.spec.volumes
        self.volumes = []  # type: List[Dict[str, str]]
        # VV: Entries of volumeMounts are compatible with pod.spec.containers.volumeMounts
        self.volumeMounts = []  # type: List[Dict[str, str]]

        # VV: Populate volumes now so that if this package definition mounts a dataset we can make sure that we
        # do not create 2 volume entries that point to the same volume
        self.parse_volumes(volumes)

        # VV: Populated in parse_spec_package()
        self.package = {}
        self.package_type = None  # options are ['git', 'dataset']
        self.parse_spec_package()

        # VV: Mount flowConfig and ask elaunch to load it - it contains arguments which are automatically built
        # using the configuration of the workflow stack deployment
        self.volumes.append({"name": f"st4sd-config", "configMap": {"name": f"flowconfig-{self.workflow_id}"}})
        self.volumeMounts.append({'name': 'st4sd-config', 'mountPath': '/config/st4sd-runtime-core/'})
        self.additionalOptions.append('--flowConfigPath=/config/st4sd-runtime-core/config.yaml')

        # VV: Parse experiment definition, and decide package name
        package = self.exp_conf.get('package', {})
        package_type = package.get('type', {})

        # VV: format of package.type.git is {'url': git-repository-url, 'branch': name-of-branch,
        # 'oath-token-secret': kubernetes secret name }
        package_git = package_type.get('git', {})
        package_dataset = package.get('dataset')  # "dataset://<datasetName>/optional/path"

        config = package.get('config', {})
        self._package_platform = config.get('platform')

        # VV: List of container registries that workflow references - each entry must be matched by at least 1
        # imagePullSecret
        container_registries = [x for x in config.get('containerRegistries', [])]  # type: List[str]

        self.validate_containerRegistries(container_registries)

        # VV: Experiment definitions may come with a collection of elaunch options
        parser = apis.models.common.parser_important_elaunch_arguments()

        try:
            opt_instance, _ = parser.parse_known_args(self.additionalOptions)
        except BaseException as e:
            raise ValueError("Instance payload contains invalid additionalOptions: %s" % e)

        if config.get('additionalOptions'):
            exp_options = [str(x) for x in config['additionalOptions']]

            try:
                opt_exp, _ = parser.parse_known_args(exp_options)
            except BaseException as e:
                raise ValueError("Experiment definition contains invalid additionalOptions: %s" % e)

            for k in vars(opt_exp):
                if ((opt_exp.__getattribute__(k) is not None and opt_instance.__getattribute__(k) is not None) and
                        (opt_exp.__getattribute__(k) != opt_instance.__getattribute__(k))):
                    raise ValueError(f"Instance cannot override additionalOption {k} which is part of the experiment "
                                     f"definition")

            exp_options.extend(self.additionalOptions)
            self.additionalOptions = exp_options

        from_path = config.get('fromPath')  # type: str

        if from_path:
            # VV: When package.config.fromPath is specified use that to decide the package name
            from_path = from_path.rstrip('/')
            self.package_name, _ = os.path.splitext(from_path.rsplit('/', 1)[-1])
        elif package_git:
            # VV: If package.git is provied use the name of the github repo to decide the package name
            try:
                # VV: git-url is expected to be git@github.ibm.com:<organization>/<package-name>.git
                package_name = os.path.split(package_git.get('url'))[1]
                self.package_name = package_name.rsplit('.', 1)[0]
            except Exception:
                current_app.logger.warning("Could not generate pretty experiment name from %s" % package_git)
        elif package_dataset:
            if package_dataset.startswith('dataset://') is False:
                raise ValueError(f"package.type.dataset must start with dataset:// but it's {package_dataset}")
            package_dataset = package_dataset.rstrip('/')
            self.package_name, _ = os.path.splitext(package_dataset.rsplit('/', 1)[-1])

        experiment_name = '-'.join([str(self.package_name), str(self.timestamp)])  # type: str
        self.experiment_name = experiment_name.replace(' ', '_')

        self.user_metadata = {
            'workflow': self.workflow_id,
            'package-name': self.package_name,
            'rest-uid': self.workflow_id
        }
        if 'id' in self.exp_conf:
            self.user_metadata['experiment-id'] = self.exp_conf['id']

        # VV: If user hasn't already provided a -l option default to 15
        for opt in self.additionalOptions:
            if opt.startswith('-l'):
                try:
                    _ = int(opt[2:])
                except TypeError:
                    continue
                else:
                    break
        else:
            self.additionalOptions.append('-l15')

        if self._package_platform:
            self.additionalOptions.append('--platform=%s' % self._package_platform)

        self.additionalOptions.append('--instanceName=%s' % self.experiment_name)
        self.additionalOptions.append('--nostamp')

        self.git_url = package_git.get('url')
        self.git_secret_name = None
        git_secret_type, self.git_url_type = self.extract_git_secret_name(self.git_url)
        if package_git:
            secret_in_package = package_git.get('oauth-token-object')
            if secret_in_package:
                self.git_secret_name = secret_in_package

                if self.git_url_type != 'https://':
                    raise ValueError("Specifying package.type.git.oauth-token-object in your experiment definition "
                                     "also implies that package.type.git.url is a https:// url but it is %s instad"
                                     % self.git_url)
                try:
                    _ = get_k8s_object('secret', secret_in_package)
                except KubernetesObjectNotFound as e:
                    raise ValueError("Cannot instantiate a workflow with clone url %s because there is no Secret "
                                     "object %s" % (self.git_url, secret_in_package))
            else:
                if git_secret_type and git_secret_type not in configuration:
                    raise ValueError("Cannot instantiate a workflow with clone url %s because there is no %s key in "
                                     "the configuration %s" % (self.git_url, git_secret_type, configuration))
                self.git_secret_name = self.get_configuration(git_secret_type)

        try:
            for lbl in metadata:
                self.user_metadata[str(lbl)] = str(metadata[lbl])
        except Exception as e:
            raise ValueError("Could not generate metadata %s because of %s" % (metadata, e))
        self.user_metadata['rest-uid'] = self.workflow_id

        # VV: The one user-metadata we refuse to promote to a k8s label is `workflowStack` because we use that
        #     label to tag Kubernetes/OpenShift objects that are part of the workflowStack and experiment instances
        #     are not considered part of the stack
        self.k8s_labels = {str(k): str(self.user_metadata[k]) for k in self.user_metadata
                           if k != "workflowStack" and
                           apis.models.common.valid_k8s_label(str(k), str(self.user_metadata[k]))}

        for lbl in self.user_metadata:
            self.additionalOptions.extend(['-m', '%s:%s' % (lbl, self.user_metadata[lbl])])

        self.s3_store_info = None  # type: Optional[S3StoreInfo]

        # VV: If a dataset is referenced, ensure that it actually exists; fetch list of known Dataset objects
        if self.s3_credentials.get('dataset') or self.dataset_store_uri or package_dataset:
            dataset_objs = apis.datasets.dataset_list()
        else:
            dataset_objs = None

        if self.s3_credentials.get('dataset'):
            if self.s3_credentials['dataset'] not in dataset_objs:
                raise ValueError("Unknown Dataset \"%s\" referenced by s3.dataset" % self.s3_credentials['dataset'])

        if self.s3_store and self.dataset_store_uri:
            raise ValueError("datasetStoreURI and s3Store are mutually exclusive")
        elif self.dataset_store_uri:
            dataset_name, bucket_path = self.partition_dataset_uri(self.dataset_store_uri)

            if dataset_name not in dataset_objs:
                raise ValueError(
                    "Unknown Dataset \"%s\" referenced by datasetStoreURI %s" % (dataset_name, datasetStoreURI))
            try:
                self.s3_store_info = self._extract_s3_credentials_from_dataset(dataset_objs[dataset_name])
            except Exception as e:
                raise ValueError("Could not extract S3 credentials for Dataset %s due to %s" % (dataset_name, e))
            else:
                self.s3_store_info.bucket_path = bucket_path
        elif self.s3_store:
            self.s3_store_info = S3StoreInfo(
                self.s3_store['credentials']['accessKeyID'],
                self.s3_store['credentials']['secretAccessKey'],
                self.s3_store['credentials']['endpoint'],
                self.s3_store['credentials']['bucket'],
                self.s3_store['bucketPath']
            )

        if self.s3_store_info:
            self.additionalOptions.extend((
                "--s3StoreToURI", "s3://%s/%s" % (self.s3_store_info.bucket, self.s3_store_info.bucket_path)))
            self.additionalOptions.extend(('--s3AuthBearer64', self.s3_store_info.to_bearer64()))

    @property
    def workflow_id(self):
        def generate_id(rand_str):
            return '-'.join((self.exp_conf.get('id', 'lambda'), str(rand_str)))

        if self._rand_id is None:
            characters = string.digits + string.ascii_lowercase
            max_tries = 5
            rand = random.SystemRandom()
            for i in range(max_tries):
                rand_str = ''.join((rand.choice(characters) for x in range(6)))
                wf_id = generate_id(rand_str)

                existing_instance = apis.instances.get_instance(wf_id)
                if existing_instance is None:
                    self._rand_id = rand_str
                    current_app.logger.info(f"Generated unique name for workflow instance {wf_id}")
                    return wf_id

                current_app.logger.info(
                    f"There's already a workflow named {wf_id} - remaining tries {max_tries - i - 1}")

            raise ValueError(f"Unable to generate unique name for instance of workflow "
                             f"{self.exp_conf.get('id', 'lambda')}")

        return generate_id(self._rand_id)

    @classmethod
    def rewrite_dlf_uri_in_payload(cls, payload: Dict[str, Any]):
        """Rewrites the dlf:// URI of dlfStoreURI into a dataset:// URI and stores it under datasetStoreURI (in place).

        Args:
            payload: a dictionary containing which may contain a dlfStoreURI dlf:// URI (is updated)
        """
        try:
            dlfstoreuri: str = payload['dlfStoreURI']
            del payload['dlfStoreURI']
            if dlfstoreuri.startswith("dlf://"):
                dlfstoreuri = f"dataset://{dlfstoreuri[6:]}"
            payload['datasetStoreURI'] = dlfstoreuri
        except KeyError:
            pass

    @classmethod
    def partition_dataset_uri(cls, uri: str, protocol='dataset') -> Tuple[str, str]:
        """Partitions a dataset URI <protocol>://<dataset-name>[/optional/path] into dataset name and path

        Arguments:
            uri: A string URI
            protocol: the expected URI protocol (e.g "dataset", etc)

        Returns
            A tuple with 2 strings, the dataset name followed by an optional Path ('' if no path is given,
            also paths are stripped of leading '/')
        """

        if uri.startswith(f'{protocol}://') is False:
            raise ValueError(f"{uri} is not a valid dataset#{protocol} URI - it should begin with {protocol}://")

        _, url = uri.split('://', 1)

        if '/' in url:
            return url.split('/', 1)

        return url, ''

    def validate_containerRegistries(self, container_registries):
        if not container_registries:
            return

        img_pull_secrets = apis.image_pull_secrets.get_all_image_pull_secrets()

        def trim_protocol_from_url(url: str) -> str:
            """Removes leading protocol from a url
            """
            protocol, delim, url = url.partition('://')
            if delim:
                return url
            return protocol

        pending_container_registries = [trim_protocol_from_url(x) for x in container_registries]

        for secret in img_pull_secrets:
            secret_urls = [trim_protocol_from_url(x) for x in img_pull_secrets[secret]]
            for x in list(pending_container_registries):
                if x in secret_urls:
                    pending_container_registries.remove(x)
            if not pending_container_registries:
                break

        if pending_container_registries:
            raise InaccessibleContainerRegistries(pending_container_registries)

    def parse_spec_package(self):
        package = self.exp_conf.get('package', {})
        package_type = package.get('type', {})
        package_git = package_type.get('git', {})  # {'url': git-repository-url, 'branch': name-of-branch}
        package_dataset = package_type.get('dataset')  # "dataset://<datasetName>/optional/path"
        config = package.get('config', {})
        package_fromPath = config.get('fromPath')
        package_withManifest = config.get('withManifest')

        if package_git and package_dataset:
            raise ValueError(f"Must define ONE of package.type.git and package.type.dataset - package: {package}")

        package = {}

        if package_git:
            self.package_type = 'git'
            package = {'url': package_git.get('url', None), 'branch': package_git.get('branch', None)}
        elif package_dataset:
            self.package_type = 'dataset'
            name_dataset, rel_path = self.partition_dataset_uri(package_dataset, 'dataset')

            dataset_objs = apis.datasets.dataset_list()
            if name_dataset not in dataset_objs:
                raise ValueError(f"Dataset object {name_dataset} does not exist")

            # VV: Dataset objects are expected to create a PVC object with the same name
            pvc_objs = apis.datasets.pvc_list()
            if name_dataset not in pvc_objs:
                raise ValueError(f"PersistentVolumeClaim object associated with Dataset object {name_dataset} "
                                 "does not exist")

            for vol in self.volumes:
                if vol.get('persistentVolumeClaim', {}).get('claimName', None) == name_dataset:
                    volume_name = vol['name']
                    break
            else:
                volume_name = 'dataset-workflow-definition'
                self.volumes.append({'name': volume_name,
                                     'persistentVolumeClaim': {'claimName': name_dataset}})

            self.volumeMounts.append({'name': volume_name, 'mountPath': '/workflow-definition'})

            if package_fromPath:
                if os.path.isabs(package_fromPath) is False:
                    package_fromPath = os.path.join('/workflow-definition', rel_path, package_fromPath)
            else:
                package_fromPath = os.path.join('/workflow-definition', rel_path)
        elif not package_fromPath:
            raise ValueError(f"Invalid package type {self.exp_conf}")

        if package_fromPath:
            package['fromPath'] = package_fromPath

        if package_withManifest:
            package['withManifest'] = package_withManifest

        self.package = package

    def parse_volumes(self, volumes):
        # type: (List[Dict[str, Any]]) -> None
        """Parses a @volume model and populates self.volumes and self.volumeMounts

        Args:
            volumes(List[Dict[str, Any]]): A list of dictionaries, each of which is the result of parsing @model_volume

        Raises
            ValueError with a descriptive message for issues that are detected with the volume/volumeMount definition
        """

        # VV: May be populated to contain a list of available pvcs/configmap/dataset objects on the namespace
        pvc_objs = None
        configmap_objs = None
        dataset_objs = None

        # VV: Maintain a mapping of volumes to their volume entries and make sure that we only have 1
        # entry per volume even if a volume is mounted multiple times
        volume_uid_to_volume = {}  # type: Dict[str, Dict[str, Any]]

        for idx, volume in enumerate(volumes):
            volume_name = 'volume%d' % idx
            # VV: Filter out empty entries
            volume_type = volume['type']
            for vtype in ['persistentVolumeClaim', 'configMap', 'dataset', 'secret']:
                if vtype in volume_type and not volume_type[vtype]:
                    del volume_type[vtype]

            # VV: Ensure that there's exactly 1 volume-type definition
            name_pvc = volume_type.get('persistentVolumeClaim')
            name_config = volume_type.get('configMap')
            name_dataset = volume_type.get('dataset')
            name_secret = volume_type.get('secret')
            if sum([1 for x in [name_pvc, name_config, name_dataset, name_secret] if x]) != 1:
                raise ValueError("Volume %s must use exactly 1 of the fields "
                                 "persistentVolumeClaim, configMap, dataset, secret" % volume)

            mountpath = volume.get(
                'mountPath', os.path.join('/input-volumes/', name_pvc or name_config or name_dataset or name_secret))
            volume['mountPath'] = mountpath

            app_dep = volume.get('applicationDependency')
            if app_dep:
                self.additionalOptions.extend(['--applicationDependencySource', ':'.join((app_dep, mountpath))])

            volume_entry = {'name': volume_name}

            if name_pvc:
                volume_uid = ':'.join(('persistentVolumeClaim', name_pvc))
                if name_pvc == self.get_configuration("workingVolume"):
                    raise ValueError("Volume \"%s\" attempts to mount working-volume as a PVC, this is not permitted" %
                                     name_pvc)
                if pvc_objs is None:
                    pvc_objs = apis.datasets.pvc_list()
                if name_pvc not in pvc_objs:
                    raise ValueError("PersistentVolumeClaim object \"%s\" does not exist" % name_pvc)
                volume_entry['persistentVolumeClaim'] = {'claimName': name_pvc}
            elif name_config:
                volume_uid = ':'.join(('configMap', name_config))
                if configmap_objs is None:
                    configmap_objs = apis.datasets.configmap_list()

                if name_config not in configmap_objs:
                    raise ValueError("ConfigMap object \"%s\" does not exist" % name_config)
                volume_entry['configMap'] = {'name': name_config}
            elif name_dataset:
                volume_uid = ':'.join(('persistentVolumeClaim', name_dataset))
                if dataset_objs is None:
                    dataset_objs = apis.datasets.dataset_list()
                if name_dataset not in dataset_objs:
                    raise ValueError("Dataset object \"%s\" does not exist" % name_dataset)

                # VV: Dataset objects are expected to create a PVC object with the same name
                if pvc_objs is None:
                    pvc_objs = apis.datasets.pvc_list()
                if name_dataset not in pvc_objs:
                    raise ValueError("PersistentVolumeClaim object associated with Dataset object \"%s\" "
                                     "does not exist" % name_dataset)
                volume_entry['persistentVolumeClaim'] = {'claimName': name_dataset}
            elif name_secret:
                volume_uid = ':'.join(('secret', name_secret))
                # try to fetch the Secret object, if the call below fails it will raise an Exception
                _ = utils.get_k8s_object('secret', name_secret)
                volume_entry['secret'] = {'secretName': name_secret}
            else:
                raise ValueError("InputVolume \"%s\" defines an unknown volume type" % volume)

            # VV: if a volume is defined multiple times keep just one reference
            if volume_uid not in volume_uid_to_volume:
                self.volumes.append(volume_entry)
                volume_uid_to_volume[volume_uid] = volume_entry
            else:
                volume_entry = volume_uid_to_volume[volume_uid]
                volume_name = volume_entry['name']

            if ':' in mountpath:
                raise ValueError("mountPath \"%s\" of %s contains the illegal character ':'" % (mountpath, volume))

            volumemount_entry = {'name': volume_name, 'mountPath': mountpath, 'readOnly': True}
            for key in ['subPath', 'readOnly']:
                if key in volume:
                    volumemount_entry[key] = volume[key]
            self.volumeMounts.append(volumemount_entry)

    @classmethod
    def _extract_s3_credentials_from_dataset(cls, obj_dataset):
        local = obj_dataset['spec']['local']
        if local['type'] not in ['S3', 'COS']:
            raise ValueError("Unsupported Dataset type: %s" % local['type'])

        return S3StoreInfo(local['accessKeyID'], local['secretAccessKey'],
                           local['endpoint'], local['bucket'], bucket_path=None)

    @classmethod
    def extract_git_secret_name(cls, git_url):
        gitsecret_name = None
        git_url_type = None
        git_url = git_url or ''

        if git_url.startswith('git@'):
            git_url_type = 'git@'
            gitsecret_name = 'gitsecret'
        elif git_url.startswith('https://'):
            git_url_type = 'https://'
            gitsecret_name = 'gitsecret-oauth'
        return gitsecret_name, git_url_type

    def generate_flow_config_configmap_yaml_template(self, k8s_workflow_uuid):
        # VV: TODO Currently we use the Service IP/PORT of mongodb-proxy and cdb-gateway-registry to
        # memoize. This enables the st4sd-runtime-core to not have to provide credentials when connecting to these 2
        # services. At the same time it means that we are *not* able to perform remote-memoization
        # because we do not have a way to provide credentials to the st4sd-runtime-core for it to use when
        # contacting remote Datastore services.

        # VV: Default command line arguments to the orchestrator of virtual experiments.
        # The format is a list of dictionaries. A key of a nested dictionary is the name of a parameter.
        # For example, the arguments `-m author:mary -m project:surrogate-models --registerWorkflow=y` would
        # be encoded as: [{"-m": "author:mary"}, {"-m": "project:surrogate-models", "--registerWorkflow": "y"}].
        # The above enables you to provide the same argument multiple times ("-m" for user-metadata key-value).
        default_arguments: List[Dict[str, Any]] = self.configuration.get('default-arguments', [])

        to_inject = {
            "--discovererMonitorDir": "/tmp/workdir/pod-reporter/update-files",
            "--mongoEndpoint": utils.DATASTORE_MONGODB_PROXY_ENDPOINT,
            "--cdbRegistry": utils.DATASTORE_GATEWAY_REGISTRY,
        }

        defaults = []

        # VV: now merge default_arguments and to_inject into `defaults`

        for collection in default_arguments:
            new_col = {}
            for (key, value) in collection.items():
                if key in to_inject:
                    to_inject[key] = value
                else:
                    new_col[key] = value
            if new_col:
                defaults.append(new_col)

        defaults.append(to_inject)

        defaults = {
            "default-arguments": defaults
        }

        config_map_body = {
            'metadata': {
                'name': f"flowconfig-{self.workflow_id}",
                'ownerReferences': self.generate_ownerref_to_workflow(k8s_workflow_uuid)
            },
            # VV: This is mounted under /config/st4sd-runtime-core/
            'data': {
                'config.yaml': yaml.dump(defaults)
            }
        }
        return config_map_body

    def generate_env_vars_secret(self, k8s_workflow_uuid):
        secret_body = {
            'metadata': {
                'name': self.secret_env_vars_id,
                'labels': {
                    'workflow': self.workflow_id
                },
                'ownerReferences': self.generate_ownerref_to_workflow(k8s_workflow_uuid)
            },
            'data': {str(x): base64.b64encode(str(self.env_variables[x]).encode('utf-8'))
            .decode('utf-8') for x in self.env_variables}
        }
        return secret_body

    def generate_data_entries_configmap_yaml_template(self, k8s_workflow_uuid):
        config_map_body = {
            'metadata': {
                'name': self.input_volume_id,
                'ownerReferences': self.generate_ownerref_to_workflow(k8s_workflow_uuid)
            },
            'data': self.data_entries
        }
        return config_map_body

    def get_configuration(self, field):
        try:
            return self.configuration[field]
        except KeyError:
            raise ValueError("Field %s does not exist in configuration %s" % (field, self.configuration))

    def generate_workflow_yaml_template(self, mount_configmap_data_input=True):
        """Generates the yaml for the Workflow kubernetes object

        Arguments:
            mount_configmap_data_input(bool): Whether to also mount the automatically generated configmap for input
                and data files (default is True)
        """
        volumes = list(self.volumes)
        volume_mounts = list(self.volumeMounts)

        body = {
            "apiVersion": '/'.join((self.wf_group, self.wf_version)),
            "kind": "Workflow",
            "metadata": {
                "name": self.workflow_id,
                "labels": self.k8s_labels,
            },
            "spec": {
                "image": self.get_configuration("image"),
                "package": self.package,
                "imagePullSecrets": self.get_configuration("imagePullSecrets"),
                "env": [{'name': 'INSTANCE_DIR_NAME', 'value': '%s.instance' % self.experiment_name}],
                "workingVolume": {
                    "name": "working-volume",
                    "persistentVolumeClaim": {
                        "claimName": self.get_configuration("workingVolume")
                    }
                },
                "inputs": self.input_files,
                "data": self.data_files,
                "variables": [],
                "volumes": volumes,
                "volumeMounts": volume_mounts,
                "additionalOptions": self.additionalOptions
            }
        }

        if self.env_variables:
            env = body['spec']['env']
            for x in self.env_variables:
                env.append({'name': x, 'valueFrom': {'secretKeyRef': {'key': x, 'name': self.secret_env_vars_id}}})

        if mount_configmap_data_input is True and self.data_entries:
            volumes.append({"name": "input-volume", "configMap": {"name": self.input_volume_id}})
            volume_mounts.append({'name': 'input-volume', 'mountPath': '/tmp/inputdir'})

        if (self.input_files or self.data_files) and self.s3_credentials:
            if self.s3_credentials.get('dataset', '') == '':
                body['spec']['s3BucketInput'] = {key: {'value': self.s3_credentials[key]} for key in
                                                 ['accessKeyID', 'secretAccessKey', 'bucket', 'endpoint']}
            else:
                body['spec']['s3BucketInput'] = {'dataset': self.s3_credentials['dataset']}

            body['spec']['s3FetchFilesImage'] = self.get_configuration('s3-fetch-files-image')

        if self.variables:
            body['spec']['variables'] = ['variables.conf']

        if self.orchestrator_resources:
            # VV: Ensure that cpu and memory fields are both represented as strings
            body['spec']['resources'] = {'elaunchPrimary': {
                x: "%s" % self.orchestrator_resources[x] for x in self.orchestrator_resources
            }}

        if self.git_secret_name:
            body['spec']['package']['gitsecret'] = self.git_secret_name

        return body

    def create_envvars_secret(self, k8s_workflow_uuid):
        if not self.env_variables:
            current_app.logger.info("No need to create an EnvVars secret")
            return None

        current_app.logger.info("Creating EnvVars Secret for %s" % self.experiment_name)

        secret_body = self.generate_env_vars_secret(k8s_workflow_uuid)
        api_instance_core = client.CoreV1Api(client.ApiClient())

        response_Secret = api_instance_core.create_namespaced_secret(
            namespace=utils.MONITORED_NAMESPACE, body=secret_body)
        current_app.logger.log(19, "EnvVars Secret create response: %s" % response_Secret)

        return response_Secret

    def create_input_volume(self, k8s_workflow_uuid):
        if not self.data_entries:
            current_app.logger.info("No need to create a DataEntries ConfigMap")
            return None

        current_app.logger.info("Creating DataEntries ConfigMap for %s" % self.experiment_name)

        config_map_body = self.generate_data_entries_configmap_yaml_template(k8s_workflow_uuid)
        api_instance_core = client.CoreV1Api(client.ApiClient())

        response_cfmap = api_instance_core.create_namespaced_config_map(
            namespace=utils.MONITORED_NAMESPACE, body=config_map_body)
        current_app.logger.log(19, "ConfigMap create response: %s" % response_cfmap)

        return response_cfmap

    def create_flowconfig_config_map(self, k8s_workflow_uuid):
        """This configMap contains default-arguments to the st4sd-runtime-core
        (e.g. --discovererMonitorDir and --mongoEndpoint
        """
        current_app.logger.info("Creating flowConfig ConfigMap for %s" % self.experiment_name)

        config_map_body = self.generate_flow_config_configmap_yaml_template(k8s_workflow_uuid)
        api_instance_core = client.CoreV1Api(client.ApiClient())

        response_cfmap = api_instance_core.create_namespaced_config_map(
            namespace=utils.MONITORED_NAMESPACE, body=config_map_body)
        current_app.logger.log(19, "ConfigMap create response: %s" % response_cfmap)

        return response_cfmap

    def generate_ownerref_to_workflow(self, k8s_workflow_uuid):
        return [
            {'apiVersion': self.wf_group + '/' + self.wf_version,
             'blockOwnerDeletion': True,
             'controller': True,
             'kind': 'Workflow',
             'name': self.workflow_id,
             'uid': k8s_workflow_uuid
             }]

    def create_workflow_object(self):
        api_instance = client.CustomObjectsApi(client.ApiClient())
        body = self.generate_workflow_yaml_template()
        return api_instance.create_namespaced_custom_object(
            self.wf_group, self.wf_version, self.namespace, self.wf_plural, body)


class LambdaExperimentFactory(ExperimentFactory):
    def __init__(self, configuration, namespace, lambdaFlowIR, validate_flowir=True, data=None, scripts=None,
                 additionalOptions=None, orchestrator_resources=None, metadata=None, variables=None,
                 s3=None, s3Store=None, datasetStoreURI=None, volumes=None, environmentVariables=None,
                 wf_group=utils.K8S_WORKFLOW_GROUP, wf_version=utils.K8S_WORKFLOW_VERSION,
                 wf_plural=utils.K8S_WORKFLOW_PLURAL):
        data = data or []
        additionalOptions = additionalOptions or []
        orchestrator_resources = orchestrator_resources or {}
        metadata = metadata or {}
        variables = variables or {}

        super().__init__(exp_conf={}, inputs=[], data=data, additionalOptions=additionalOptions,
                         orchestrator_resources=orchestrator_resources, metadata=metadata,
                         variables=variables, configuration=configuration, namespace=namespace,
                         s3=s3, s3Store=s3Store, datasetStoreURI=datasetStoreURI, volumes=volumes,
                         environmentVariables=environmentVariables)

        # VV: Do not mount an input_volume, the workflow-operator will automatically mount the flowirFromConfig one
        self.input_volume_id = None

        rand = binascii.b2a_hex(os.urandom(5))
        self.bin_entries = {d['filename']: d['content'] for d in (scripts or [])}
        self.flowir_configmap_name = '-'.join(('flowir', self.workflow_id, rand.decode('utf-8')))
        self.lambdaFlowIR = lambdaFlowIR

        if validate_flowir:
            try:
                concrete = experiment.model.frontends.flowir.FlowIRConcrete(lambdaFlowIR, None, {})
                flowir_errors = concrete.validate()
            except Exception as e:
                flowir_errors = [e]
            if flowir_errors:
                msg = '\n'.join([str(x) for x in flowir_errors])

                msg = 'Found %d errors:\n%s' % (len(flowir_errors), msg)
                raise experiment.model.errors.FlowIRInconsistency(
                    reason=msg, flowir=lambdaFlowIR, exception=None, extra=flowir_errors)

    def parse_spec_package(self):
        # VV: Override method so that it doesn't raise an error for an invalid package type definition,
        # lambda experiments do not have an external package definition!
        self.package = {}

    def generate_workflow_yaml_template(self):
        body = super().generate_workflow_yaml_template(mount_configmap_data_input=False)

        # VV: Data files are bundled in the spec.package.fromConfigMap configMap, st4sd-runtime-core must not
        # attempt to override them (ExperimentFactory assumes taht the data entries are placed
        # in a separate volume)
        if 'data' in body['spec']:
            del body['spec']['data']

        field_package = body['spec'].get('package', {})
        field_package.update({'fromConfigMap': self.flowir_configmap_name})
        body['spec']['package'] = field_package
        return body

    def generate_flowir_configmap_yaml_template(self, k8s_workflow_uuid):
        data = {
            # VV: We support both json/yaml because we use a YAML loader to parse the package.json file
            'conf/flowir_package.yaml': yaml.dump(self.lambdaFlowIR, Dumper=yaml.SafeDumper),
        }

        if self.s3_credentials:
            # VV: When s3 is specified, we have to generate SOME file under the `data` dir so that the
            #     st4sd-runtime-core can copy the one hosted on s3, onto the existing dummy one inside `data`.
            #     it's important that we use os.path.split here so that we extract the filename out of the
            #     relative path! Otherwise, the st4sd-runtime-core will complain because it will not be able to find
            #     the file inside its `data` folder
            for d in self.data_files:
                data[os.path.join('data', os.path.split(d)[1])] = ''
        else:
            for d in self.data_files:
                data[os.path.join('data', d)] = self.data_entries[d]

        for d in self.bin_entries:
            data[os.path.join('bin', d)] = self.bin_entries[d]

        data = {'files': data}

        config_map_body = {
            'metadata': {
                'name': self.flowir_configmap_name,
                'labels': {
                    'workflow': self.workflow_id
                },
                'ownerReferences': self.generate_ownerref_to_workflow(k8s_workflow_uuid)
            },
            'data': {
                'package.json': yaml.dump(data, Dumper=yaml.SafeDumper)
            }
        }
        return config_map_body

    def create_input_volume(self, k8s_workflow_uuid):
        current_app.logger.info("Creating DataEntries configmap for %s" % self.experiment_name)

        config_map_body = self.generate_flowir_configmap_yaml_template(k8s_workflow_uuid)
        api_instance_core = client.CoreV1Api(client.ApiClient())

        response_cfmap = api_instance_core.create_namespaced_config_map(
            namespace=utils.MONITORED_NAMESPACE, body=config_map_body)
        current_app.logger.log(19, "Create ConfigMap response: %s" % response_cfmap)

        return response_cfmap


@api.route('/')
class ExperimentList(Resource):
    _my_parser = parser_formatting_parameterised_package()

    @api.expect(_my_parser)
    def get(self):
        '''List all experiments'''
        with utils.database_experiments_open(apis.models.constants.LOCAL_DEPLOYMENT) as db:

            entries = []
            problems = []
            for doc in db.query():
                try:
                    obj = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(doc)
                except pydantic.ValidationError as exc:
                    package_name = doc.get('metadata', {}).get('package', {}).get('name', '**unknown**')
                    digest = doc.get('metadata', {}).get('registry', {}).get('digest', '**unknown**')
                    identifier = '@'.join((package_name, digest))

                    errors = make_pydantic_errors_jsonable(exc)

                    problems.append({
                        'identifier': identifier,
                        'problems': errors
                    })
                    obj = doc

                entries.append(do_format_parameterised_package(obj, self._my_parser))

        return {
            'entries': entries,
            'problems': problems,
        }

    @api.expect(mVirtualExperiment)
    @api.response(400, "Experiment ID is invalid")
    @api.response(409, "Experiment ID already exists")
    @api.response(200, "OK")
    def post(self):
        '''Add an experiment'''
        try:
            experiment_inp = request.get_json()
            try:
                ve = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(experiment_inp)
            except pydantic.ValidationError as e:
                current_app.logger.warning(f"Invalid parameterised package {e}. Traceback: {traceback.format_exc()}")
                api.abort(400, message="Invalid parameterised package", invalidVirtualExperimentDefinition=str(e))
                raise  # keep linter happy

            current_app.logger.info(f"Creating experiment definition")
            download = apis.storage.PackagesDownloader(ve, db_secrets=utils.secrets_git_open(
                local_deployment=apis.models.constants.LOCAL_DEPLOYMENT))
            db = utils.database_experiments_open(apis.models.constants.LOCAL_DEPLOYMENT)
            apis.kernel.experiments.validate_and_store_pvep_in_db(download, ve, db)

            return {"result": ve.dict()}
        except werkzeug.exceptions.HTTPException:
            raise
        except apis.models.errors.ApiError as e:
            current_app.logger.warning(f"Run into {e} while pushing new parameterised package. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, f"Invalid payload, reason: {str(e)}", problem=str(e))
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while pushing new parameterised package. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(500, f"Internal error pushing new parameterised package")


@api.route('/<identifier>/dsl/', doc=False)
@api.route('/<identifier>/dsl')
@api.param('identifier', 'The package identifier. It must contain a $packageName and may include either '
                         'a tag suffix (`:${tag}`) or a digest suffix (`@${digest}`). If both suffixes are missing '
                         'then the identifier implies the `:latest` tag suffix.')
@api.response(404, 'Unknown experiment')
class ExperimentDSL(Resource):
    _my_parser = parser_formatting_dsl()

    @api.expect(_my_parser)
    def get(self, identifier: str):
        """Fetch the DSL of an experiment given its identifier"""

        try:
            # VV: If identifier has neither @ or : it rewrites it to "${identifier}:latest}
            identifier = apis.models.common.PackageIdentifier(identifier).identifier

            with utils.database_experiments_open(apis.models.constants.LOCAL_DEPLOYMENT) as db:
                docs = db.query_identifier(identifier)

                if len(docs) == 0:
                    api.abort(404, message=f"There is no entry in the experiment registry "
                                           f"that matches {identifier}")

                try:
                    ve = apis.models.virtual_experiment.ParameterisedPackageDropUnknown \
                        .parse_obj(docs[0])
                except pydantic.error_wrappers.ValidationError as e:
                    return {'problems': make_pydantic_errors_jsonable(e)}

            if len(ve.base.packages) == 1:
                download = apis.storage.PackagesDownloader(ve, db_secrets=utils.secrets_git_open(
                    local_deployment=apis.models.constants.LOCAL_DEPLOYMENT))
            else:
                download = None

            dsl = apis.kernel.experiments.api_get_experiment_dsl(
                pvep=ve,
                packages=download,
                derived_packages_root=apis.models.constants.ROOT_STORE_DERIVED_PACKAGES,
            )

            args = self._my_parser.parse_args()
            if args.outputFormat == "yaml":
                dsl = experiment.model.frontends.flowir.yaml_dump(dsl)

            return {
                "dsl": dsl,
                "problems": [],
            }

        except werkzeug.exceptions.HTTPException:
            raise
        except apis.models.errors.ApiError as e:
            current_app.logger.warning(f"Run into {e} while generating the DSL of {identifier}. "
                                       f"Traceback: {traceback.format_exc()}")
            return {
                "dsl": None,
                "problems": [str(e)]
            }
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while generating the DSL of {identifier}. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(500, f"Run into internal error while querying for {identifier}")
            return {
                'dsl': None,
                "problems": [str(e)]
            }


@api.route('/<identifier>/package-inheritance/', doc=False)
@api.route('/<identifier>/package-inheritance')
@api.param('identifier', 'The package identifier. It must contain a $packageName and may include either '
                         'a tag suffix (`:${tag}`) or a digest suffix (`@${digest}`). If both suffixes are missing '
                         'then the identifier implies the `:latest` tag suffix.')
@api.response(404, 'Unknown experiment')
class ExperimentExplain(Resource):
    def get(self, identifier: str):
        """Explain how variables of multi-base-package experiments receive their values

        It returns a Dictionary with the format ::

            {
                "result": {
                    "variables": {
                        "<variable name>": {
                            "fromBasePackage": "name of base package from which variable receives its value",
                            "values": [
                                {
                                    "value": "the value of the variable in the platform",
                                    "platform": "the platform name",

                                    "overrides": [
                                        # Optional - explains which values in other basePackages this value overrides
                                        {
                                            "fromBasePackage": "the other base package which is overridden",
                                            "value": "the value of this variable, in the other base package
                                                      for this platform",
                                        }
                                    [
                                },
                            ]
                        }
                    }
                }
            }

        """

        try:
            # VV: If identifier has neither @ or : it rewrites it to "${identifier}:latest}
            identifier = apis.models.common.PackageIdentifier(identifier).identifier

            with utils.database_experiments_open(apis.models.constants.LOCAL_DEPLOYMENT) as db:
                docs = db.query_identifier(identifier)

                if len(docs) == 0:
                    api.abort(404, message=f"There is no entry in the experiment registry "
                                           f"that matches {identifier}")

                try:
                    ve = apis.models.virtual_experiment.ParameterisedPackageDropUnknown \
                        .parse_obj(docs[0])
                except pydantic.error_wrappers.ValidationError as e:
                    errors = make_pydantic_errors_jsonable(e)
                    raise apis.models.errors.ApiError(f"Invalid experiment. Underlying problems {errors}")

            if len(ve.base.packages) == 1:
                api.abort(400, "Experiment has a single base package, there is no inheritance to explain")
                raise NotImplementedError()  # keep linter happy
            elif len(ve.base.packages) > 1:

                downloader = apis.storage.PackagesDownloader(ve=ve, db_secrets=utils.secrets_git_open(
                    local_deployment=apis.models.constants.LOCAL_DEPLOYMENT))

                with downloader:
                    explanation = apis.runtime.package_derived.explain_choices_in_derived(ve, packages=downloader)

                return {"result": explanation.dict()}
            else:
                api.abort(400, "Parameterised virtual experiment package does not contain any base packages")
                raise NotImplementedError()  # keep linter happy

        except werkzeug.exceptions.HTTPException:
            raise
        except apis.models.errors.ApiError as e:
            current_app.logger.warning(f"Run into {e} while explaining package-inheritance in {identifier}. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, message=f"Unable to explain package inheritance. Underlying error: {e}")
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while explaining package-inheritance in {identifier}. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(500, f"Run into internal error while explaining package-inheritance in {identifier}")


@api.route('/<identifier>/', doc=False)
@api.route('/<identifier>')
@api.param('identifier', 'The package identifier. It must contain a $packageName and may include either '
                         'a tag suffix (`:${tag}`) or a digest suffix (`@${digest}`). If both suffixes are missing '
                         'then the identifier implies the `:latest` tag suffix.')
@api.response(404, 'Unknown experiment')
class Experiment(Resource):
    _my_parser = parser_formatting_parameterised_package()

    @api.expect(_my_parser)
    def get(self, identifier: str):
        '''Fetch an experiment given its identifier'''

        try:
            ret = apis.kernel.experiments.api_get_experiment(identifier, utils.database_experiments_open(
                apis.models.constants.LOCAL_DEPLOYMENT))

            return {
                'entry': do_format_parameterised_package(ret.experiment, self._my_parser),
                'problems': ret.problems,
            }
        except werkzeug.exceptions.HTTPException:
            raise
        except apis.models.errors.ParameterisedPackageNotFoundError as e:
            api.abort(404, e.message, packageNotFound=identifier)
        except apis.models.errors.InvalidModelError as e:
            current_app.logger.warning(f"Run into {e} while getting {identifier}. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, e.message, problems=e.problems)
        except apis.models.errors.ApiError as e:
            current_app.logger.warning(f"Run into {e} while getting {identifier}. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, e.message)
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while getting {identifier}. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(500, f"Run into internal error while querying for {identifier}")

    @api.response(500, "Internal error")
    @api.response(404, "Unknown experiment")
    @api.response(200, "Success")
    def delete(self, identifier: str):
        '''Delete an experiment'''
        deleted = False
        try:
            with utils.database_experiments_open(apis.models.constants.LOCAL_DEPLOYMENT) as db:
                deleted = db.delete_identifier(identifier)
        except Exception as e:
            current_app.logger.warning(traceback.format_exc())
            current_app.logger.warning("Could not delete experiment due to internal error %s" % e)
            api.abort(500, "Could not delete experiment due to internal error")

        if deleted > 0:
            current_app.logger.info("Deleted experiment %s" % identifier)
            return {"deleted": deleted}
        else:
            api.abort(404, "Experiment %s is unknown" % identifier, unknownExperiment=identifier)


@api.route('/lambda/start')
@api.response(200, '<id_of_new_experiment_instance>')
@api.response(400, 'Malformed experiment creation request')
@api.response(500, 'Internal error while creating experiment')
class LambdaExperimentStart(Resource):
    @api.expect(mLambdaExperimentStart)
    def post(self):
        '''Start a lambda experiment'''
        try:
            configuration = setup_config(local_deployment=apis.models.constants.LOCAL_DEPLOYMENT)

            experiment_start_obj = request.json

            current_app.logger.info("Creating lambda workflow")

            # VV: Try to rewrite dlfStoreURI into a datasetStoreURI here
            ExperimentFactory.rewrite_dlf_uri_in_payload(experiment_start_obj)

            try:
                exp_obj = LambdaExperimentFactory(
                    configuration=configuration, namespace=utils.MONITORED_NAMESPACE,
                    wf_group=utils.K8S_WORKFLOW_GROUP, wf_version=utils.K8S_WORKFLOW_VERSION,
                    wf_plural=utils.K8S_WORKFLOW_PLURAL, **experiment_start_obj)
            except experiment.model.errors.FlowIRInconsistency as e:
                current_app.logger.warning(f"Invalid lambda FlowIR: {e.message}")
                api.abort(400, "Invalid lambda FlowIR", flowirErrors=e.message)
                raise  # VV: keeps linter happy
            except Exception as e:
                current_app.logger.warning(traceback.format_exc())
                current_app.logger.warning("Could not instantiate experiment object because of %s" % e)
                raise

            if exp_obj.git_url_type is not None:
                api.abort(400, "Lambda Workflows do not git clone experiments")

            data_input_conflicts = set(exp_obj.input_files).intersection(exp_obj.data_files)
            if data_input_conflicts:
                api.abort(400, "Data files and input files cannot have the same filename. Conflicts: %s"
                          % list(data_input_conflicts), dataInputConflicts=list(data_input_conflicts))

            current_app.logger.info("Creating new experiment %s" % exp_obj.experiment_name)

            k8s_uid = None  # VV: keep linter happy
            try:
                api_response = exp_obj.create_workflow_object()
                k8s_uid = api_response["metadata"]["uid"]
            except ApiException as e:
                current_app.logger.warning(traceback.format_exc())
                current_app.logger.warning("Exception when calling CustomObjectsApi->"
                                           "create_namespaced_custom_object: %s\n" % e)
                api.abort(500, "Exception when calling CustomObjectsApi->create_namespaced_custom_object: %s\n" % e)

            try:
                exp_obj.create_flowconfig_config_map(k8s_uid)
            except Exception as e:
                current_app.logger.warning(traceback.format_exc())
                current_app.logger.error("Exception when creating flowConfig configMap: %s\n" % e)
                api.abort(500, "Exception when creating flowConfig configMap: %s\n" % e)

            try:
                exp_obj.create_envvars_secret(k8s_uid)
            except Exception as e:
                current_app.logger.warning(traceback.format_exc())
                current_app.logger.error("Exception when creating env-vars secret: %s\n" % e)
                api.abort(500, "Exception when creating env-vars secret: %s\n" % e)

            try:
                exp_obj.create_input_volume(k8s_uid)
            except Exception as e:
                current_app.logger.warning(traceback.format_exc())
                current_app.logger.error("Exception when calling CustomObjectsApi->create_namespaced_config_map for "
                                         "lambdaFlowir: %s\n" % e)
                api.abort(500, "Exception when calling CustomObjectsApi->create_namespaced_custom_object: %s\n" % e)

            return exp_obj.workflow_id
        except werkzeug.exceptions.HTTPException:
            raise
        except Exception as e:
            current_app.logger.warning("Traceback: %s\nException: %s for %s" % (traceback.format_exc(), e, id))
            api.abort(500, "Exception when creating lambda instance - %s" % e)


@api.route('/<package_name>/history/', doc=False)
@api.route('/<package_name>/history')
@api.param('package_name', 'The package name. It must not include a tag (:${tag}) or a digest (@${digest})')
@api.response(404, 'No matching experiments found')
@api.response(400, 'Malformed experiment creation request')
@api.response(500, 'Internal error while creating experiment')
class ExperimentPackageHistory(Resource):

    @api.doc(model=mPackageHistory)
    def get(self, package_name: str):
        '''Returns the history of tags and digests for this package'''
        try:
            with utils.database_experiments_open(apis.models.constants.LOCAL_DEPLOYMENT) as db:
                history = db.trace_history(package_name=package_name)

            return history.to_dict()
        except werkzeug.exceptions as e:
            raise


@api.route('/<identifier>/tag')
@api.param('identifier', 'The package identifier. It must contain a $packageName and may include either '
                         'a tag suffix (`:${tag}`) or a digest suffix (`@${digest}`). If both suffixes are missing '
                         'then the identifier implies the `:latest` tag suffix.')
@api.response(404, 'No matching parameterised package')
@api.response(500, 'Internal error while updating tags of parameterised package')
class ExperimentPackageTag(Resource):
    _my_parser = reqparse.RequestParser()
    _my_parser.add_argument(
        # VV: For some unknown reason I cannot set the name of the parameter to "tags". When I do that, the
        # Swagger form does NOT send the request and simply prints the text:
        # Failed to fetch.
        # Possible Reasons:
        #
        # - CORS
        # - Network Failure
        # - URL scheme must be "http" or "https" for CORS request.
        # I HAVE ABSOLUTELY NO IDEA WHY "newTags" works but "tags" doesn't.
        # Good luck.
        "newTags",
        type=str,
        help='Comma separated tags to replace those associated with the $identifier. Each tag must contain '
             'lowercase letters, numbers, and -. characters. Each tag must start and end with a lowercase letter '
             'or a number. '
             'The service will untag other parameterised packages with the same `metadata.package.name` to ensure that '
             'the $newTags are only associated with the parameterised package $identifier. '
             'If the definition of the $identifier contains the `latest` tag then $newTags must also contain '
             'the `latest` tag. ',
        required=True,
        location=("args", "values"))

    @api.expect(_my_parser)
    def put(self, identifier):
        """Updates the tags associated with the parameterised package that matches the identifier"""
        try:
            args = self._my_parser.parse_args()
            tags = args.newTags.split(',')
        except Exception as e:
            current_app.logger.warning(f"Exception while parsing args {e}. Traceback: {traceback.format_exc()}")
            api.abort(400, f"Invalid tags")
            raise  # VV: keep linter happy

        try:
            with utils.database_experiments_open(apis.models.constants.LOCAL_DEPLOYMENT) as db:
                db.tag_update(identifier, tags)
        except apis.models.errors.CannotRemoveLatestTagError:
            api.abort(400, f"Cannot untag the latest tag from {identifier}", cannotUntagLatestFrom=identifier)
        except apis.models.errors.ParameterisedPackageNotFoundError:
            # VV: FIXME other places of the RestAPI are using "Experiment" instead of "ParameterisedPackage"
            api.abort(404, f"Unknown parameterised package {identifier}", unknownExperiment=identifier)
        except Exception as e:
            current_app.logger.warning(f"Exception while updating tags {tags} of {identifier} = {e}. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(500, f"Internal error while updating database - please inform your ST4SD admins")

        return "OK"


def create_local_client() -> apis.models.from_core.BetaExperimentRestAPI:
    token = open(apis.models.constants.PATH_TO_RUNTIME_SERVICE_API_KEY, 'r').read().rstrip()
    return apis.models.from_core.BetaExperimentRestAPI(apis.models.constants.URL_RUNTIME_SERVICE, cc_bearer_key=token)


@api.route('/<identifier>/start')
@api.param('identifier', 'The package identifier. It must contain a $packageName and may include either '
                         'a tag suffix (`:${tag}`) or a digest suffix (`@${digest}`). If both suffixes are missing '
                         'then the identifier implies the `:latest` tag suffix.')
@api.response(404, 'Experiment not found')
@api.response(200, '<rest_uid_of_new_experiment_instance>')
@api.response(400, 'Malformed experiment creation request')
@api.response(500, 'Internal error while creating experiment')
class ExperimentStart(Resource):
    @api.expect(mExperimentStart)
    def post(self, identifier: str):
        '''Start an experiment given its identifier'''
        try:
            # current_app.logger.info("ExperimentStart %s" % id)
            # if valid_k8s_label('id', id) is False:
            #     # VV: We use the suffix `-XXXXXX` to make workflow ids unique
            #     api.abort(400, "Invalid experiment id \"%s\". Valid names must match the regular expression "
            #                    "(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]) and be less than %d "
            #                    "characters long" % (id, 64 - (1 + 6)))

            # VV: If identifier has neither @ or : it rewrites it to "${identifier}:latest}
            identifier = apis.models.common.PackageIdentifier(identifier).identifier

            with utils.database_experiments_open(apis.models.constants.LOCAL_DEPLOYMENT) as db:
                docs = db.query_identifier(identifier)

            if len(docs) == 0:
                api.abort(404, message=f"No parameterised package {identifier}", unknownExperiment=identifier)
            elif len(docs) > 1:
                api.abort(400, message=f"Found too many parameterised packages {len(docs)} for your query {identifier}")

            try:
                ve = apis.models.virtual_experiment.ParameterisedPackageDropUnknown.parse_obj(docs[0])
            except pydantic.ValidationError as e:
                errors = make_pydantic_errors_jsonable(e)
                raise apis.models.errors.InvalidModelError(
                    "The parameterised virtual experiment is invalid. Please update it before trying to execute it.",
                    problems=errors
                )

            experiment_start_obj = request.json
            try:
                old = apis.models.virtual_experiment.DeprecatedExperimentStartPayload.parse_obj(experiment_start_obj)
                payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.from_old_payload(old)
            except pydantic.ValidationError as e:
                current_app.logger.warning(f"Invalid start payload {e}. Traceback: {traceback.format_exc()}")
                api.abort(400, message="Invalid start payload", invalidStartPayload=str(e))
                raise  # keep linter happy
            except Exception as e:
                current_app.logger.info(f"Traceback while decoding payload {traceback.format_exc()}")
                api.abort(400, f"Invalid payload: {e}", reason=str(e))
                raise  # keep linter happy

            if payload_config.runtimePolicy:
                known_policies = ['prefer-surrogate', 'random-canary-surrogate']
                if payload_config.runtimePolicy.name == "prefer-surrogate":
                    policy = apis.policy.prefer_surrogate.PolicyPreferSurrogate(create_local_client())
                elif payload_config.runtimePolicy.name == "random-canary-surrogate":
                    policy = apis.policy.random_canary_surrogate.PolicyRandomCanarySurrogate(create_local_client())
                else:
                    api.abort(404, f"Unknown policy name \"{payload_config.runtimePolicy.name}\". "
                                   f"Known policies {known_policies}",
                              unknownRuntimePolicyName=payload_config.runtimePolicy)
                    raise ValueError("Satisfy the linter")

                plan: Dict[str, Any] = policy.policy_based_run_create(
                    pvep_identifier=identifier,
                    payload_start=experiment_start_obj,
                    policy_config=payload_config.runtimePolicy.config, dry_run=False)
                try:
                    plan: apis.policy.PolicyBasedExperimentRun = apis.policy.PolicyBasedExperimentRun.parse_obj(
                        plan)
                except pydantic.error_wrappers.ValidationError as e:
                    api.abort(400, f"PolicyBasedExperimentRun has invalid schema, problems: {e.json(indent=2)}")

                return plan.uid

            s3_out = payload_config.s3Output.my_contents
            if isinstance(s3_out, apis.models.common.OptionFromDatasetRef):
                # VV: apis.runtime.NamedPackage does not know how to deal with OptionFromDatasetRef, it only understands
                # OptionsFromS3Values. Extract the dataset credentials from S3 and synthesize OptionFromS3Values
                s3_security = apis.k8s.extract_s3_credentials_from_dataset(s3_out.name, utils.MONITORED_NAMESPACE)
                payload_config.configure_output_s3(s3_out.path, s3_security)

            configuration = setup_config(local_deployment=apis.models.constants.LOCAL_DEPLOYMENT)
            extra_options = apis.runtime.package.PackageExtraOptions.from_configuration(configuration)
            namespace_presets = apis.models.virtual_experiment.NamespacePresets.from_configuration(configuration)

            namespace_presets.runtime.args += [
                "--discovererMonitorDir=/tmp/workdir/pod-reporter/update-files",
                f"--mongoEndpoint={utils.DATASTORE_MONGODB_PROXY_ENDPOINT}",
                f"--cdbRegistry={utils.DATASTORE_GATEWAY_REGISTRY}",
            ]

            # VV: TODO AT this point we should resolve variables, input files, data files, environment variables
            # i.e. the associated apis.models.common.Option should look like {'name': the name, 'value': the value}
            current_app.logger.info(f"Execute {ve.metadata.package.name}@{ve.metadata.registry.digest} "
                                    f"(digest: {ve.metadata.registry.digest})")

            try:
                package = apis.runtime.package.NamedPackage(ve, namespace_presets, payload_config, extra_options)
            except apis.models.errors.ApiError:
                raise
            except Exception as e:
                current_app.logger.warning(f"Invalid payload with anonymous error {e}. Traceback: "
                                           f"{traceback.format_exc()}")
                api.abort(400, f"{e} - experiment start payload is invalid", reason=str(e))
                raise e from e  # VV: Keep linter happy

            # VV: first create the instance of the workflow CRD to extract its uuid and setup the ownerreferences
            # of any child k8s objects (e.g. ConfigMaps, Secrets, etc) this enables automatic cleanup of child objects

            api_instance = client.CustomObjectsApi(client.ApiClient())
            body = package.construct_k8s_workflow()

            current_app.logger.info(f"Creating new experiment {package.experiment_name}")

            if len(ve.base.packages) > 1:
                # VV: For the time being we're storing the standalone st4sd package under a certain path in persistent
                # storage. Double check that the files are there before asking elaunch.py to run the experiment
                if os.path.exists(package.get_path_to_multi_package_pvep()) is False:
                    raise apis.models.errors.ApiError(
                        "Unable to locate the package files - try recreating the package (e.g. by using the "
                        "/relationships/<identifier>/synthesize/ API)")

            try:
                api_response = api_instance.create_namespaced_custom_object(
                    utils.K8S_WORKFLOW_GROUP,
                    utils.K8S_WORKFLOW_VERSION,
                    utils.MONITORED_NAMESPACE,
                    utils.K8S_WORKFLOW_PLURAL,
                    body)
                k8s_workflow_uuid = api_response["metadata"]["uid"]
            except ApiException as e:
                current_app.logger.warning(traceback.format_exc())
                current_app.logger.warning("Exception when calling CustomObjectsApi->"
                                           "create_namespaced_custom_object: %s\n" % e)
                current_app.logger.warning(f"Could not create workflow:\n{yaml.dump(body)}")
                api.abort(500, "Exception when calling CustomObjectsApi->create_namespaced_custom_object: %s\n" % e)
                raise  # VV: keep linter happy

            api_instance_core = None
            try:
                body = package.construct_k8s_secret_env_vars(k8s_workflow_uuid)
                if body is not None:
                    api_instance_core = client.CoreV1Api(client.ApiClient())
                    api_instance_core.create_namespaced_secret(namespace=utils.MONITORED_NAMESPACE, body=body)
            except Exception as e:
                current_app.logger.warning(traceback.format_exc())
                current_app.logger.error("Exception when creating env-vars secret: %s\n" % e)
                api.abort(500, "Exception when creating env-vars secret: %s\n" % e)

            try:
                body = package.construct_k8s_configmap_embedded_files(k8s_workflow_uuid)
                if body is not None:
                    if api_instance_core is None:
                        api_instance_core = client.CoreV1Api(client.ApiClient())
                    api_instance_core.create_namespaced_config_map(namespace=utils.MONITORED_NAMESPACE, body=body)
            except Exception as e:
                current_app.logger.warning(traceback.format_exc())
                current_app.logger.error("Exception when creating input volume: %s\n" % e)
                api.abort(500, "Exception when creating input volume: %s\n" % e)
            ve.metadata.registry.timesExecuted += 1

            with utils.database_experiments_open(apis.models.constants.LOCAL_DEPLOYMENT) as db:
                db.upsert(ve.dict(exclude_none=True), ql=db.construct_query(
                    package_name=ve.metadata.package.name,
                    registry_digest=ve.metadata.registry.digest))

            return package.rest_uid
        except apis.models.errors.InvalidInputsError as e:
            current_app.logger.warning("Traceback: %s\nException: %s for %s" % (traceback.format_exc(), e, identifier))
            kargs = {}
            if e.missing_inputs:
                kargs['missingInputs'] = e.missing_inputs
            if e.extra_inputs:
                kargs['extraInputs'] = e.extra_inputs
            api.abort(400, f"{e}", **kargs)
        except apis.models.errors.ApiError as e:
            current_app.logger.warning("Traceback: %s\nException: %s for %s" % (traceback.format_exc(), e, identifier))
            api.abort(400, f"Invalid request. {e}")
        except werkzeug.exceptions.HTTPException:
            raise
        except Exception as e:
            current_app.logger.warning("Traceback: %s\nException: %s for %s" % (traceback.format_exc(), e, identifier))
            api.abort(500, message="Exception when creating instance %s - %s" % (identifier, e))



@api.route('/<identifier>/start/payload/')
@api.param('identifier', 'The package identifier. It must contain a $packageName and may include either '
                         'a tag suffix (`:${tag}`) or a digest suffix (`@${digest}`). If both suffixes are missing '
                         'then the identifier implies the `:latest` tag suffix.')
@api.response(404, 'Experiment not found')
@api.response(500, 'Internal error while generating the experiment start payload')
@api.response(200, "The payload skeleton with magicValues explaining it")
class GetPayloadToStart(Resource):
    def get(self, identifier: str):
        """Returns a skeleton payload to /experiments/<identifier>/start for an experiment"""
        try:
            identifier = apis.models.common.PackageIdentifier(identifier).identifier

            with utils.database_experiments_open(apis.models.constants.LOCAL_DEPLOYMENT) as db:
                docs = db.query_identifier(identifier)

            if len(docs) == 0:
                api.abort(404, message=f"No parameterised package {identifier}", unknownExperiment=identifier)
            elif len(docs) > 1:
                api.abort(400, message=f"Found too many parameterised packages {len(docs)} for your query {identifier}")

            ve = apis.models.virtual_experiment.ParameterisedPackageDropUnknown.parse_obj(docs[0])

            skeleton = apis.kernel.experiments.generate_experiment_start_skeleton_payload(ve=ve)

            return {
                "payload": skeleton.payload,
                "magicValues": skeleton.magicValues,
                "problems": skeleton.problems,
                "message": "Read the skeleton payload in .payload and consult the .magicValues for instructions on "
                           "how to construct the payload that you will submit to start the experiment.",
            }
        except pydantic.ValidationError as e:
            api.abort(400, "The experiment stored in the database is invalid", problems=e.errors())
        except apis.models.errors.ApiError as e:
            current_app.logger.warning("Traceback: %s\nException: %s for %s" % (traceback.format_exc(), e, identifier))
            api.abort(400, f"Invalid request. {e}")
        except werkzeug.exceptions.HTTPException:
            raise
        except Exception as e:
            current_app.logger.warning("Traceback: %s\nException: %s for %s" % (traceback.format_exc(), e, identifier))
            api.abort(500, message="Internal error while generating the skeleton payload of %s - %s" % (identifier, e))
