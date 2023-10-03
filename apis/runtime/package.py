# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import argparse
import copy
import datetime
import logging
import os
import random
import string
import traceback
from typing import Any
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Tuple

import experiment.model.errors
import experiment.model.frontends.flowir
import six
import yaml

import apis.db.exp_packages
import apis.db.secrets
import apis.k8s
import apis.models.common
import apis.models.constants
import apis.models.errors
import apis.models.virtual_experiment
import apis.runtime.errors
import apis.runtime.package_derived
import apis.storage
import binascii
import base64


ROOT_VOLUME_MOUNTS = '/tmp/st4sd-volumes/'
ROOT_DATASET_WORKFLOW_DEFINITIONS = "/tmp/st4sd-workflow-definitions/"
ROOT_EMBEDDED_FILES = "/tmp/st4sd-embedded/"

# VV: DO NOT change this - st4sd-runtime-k8s currently has a hard-coded check we'll address this in the future
ROOT_S3_FILES = "/tmp/s3-root-dir"

logger = logging.getLogger("pkg")


class Volume(NamedTuple):
    # VV: this is not actually the name
    name: str
    config: Dict[str, Any]


class VolumeMount(NamedTuple):
    volume_name: str
    config: Dict[str, Any]


class VolumesVolumeMountsArgs(NamedTuple):
    volumes: Dict[str, Volume]
    volume_mounts: List[VolumeMount]
    args: List[str]


# VV: We should put in here "global" things so that we don't provide them in single arguments to NamedPackage
# this makes it easier to unit-test as well as pass around a large amount of "parameterisation"
class PackageExtraOptions:
    @classmethod
    def from_configuration(cls, configuration: Dict[str, Any]) -> PackageExtraOptions:
        """Parses what you would find in the data/config.json field inside the st4sd-runtime-service ConfigMap
        The current schema is (more in utils.setup_config)::

            {
                "image": "container image:str for st4sd-runtime-core"
                "s3-fetch-files-image": "container image:str for the s3-fetch-files image"
                "gitsecret" (optional): "Name of Secret object which contains the keys: `ssh` and `known_hosts`"
                "gitsecret-oauth" (optional): "Name of Secret object which contains the key `oauth-token`"
                "imagePullSecrets":
                  - "Name of Secret object which contains the key `.dockerconfigjson`"
                "inputdatadir": "the directory that the workflow experiments description is stored (experiments.json)"
                "workingVolume": "name of the PVC that workflow instances will use to store their outputs",
                "default-arguments": [
                    {
                        "--some-parameter": "the value",
                        "-o": "other value"
                    }
                ]
            }
        """

        # VV: Update the field in this weird way so that we do not override default values unless they are in the
        # config AND if we ever refactor the code we automatically pick up the changes
        obj = cls()

        if configuration.get('image'):
            obj.image_st4sd_runtime_core = configuration['image']

        if configuration.get('s3-fetch-files-image'):
            obj.image_st4sd_runtime_k8s_input_s3 = configuration.get('s3-fetch-files-image')

        if configuration.get('imagePullSecrets'):
            obj.extra_image_pull_secret_names = configuration['imagePullSecrets']

        if configuration.get('workingVolume'):
            obj.pvc_working_volume = configuration['workingVolume']

        return obj

    def __init__(
            self,
            pvc_working_volume: str = "workflow-instances-pvc",
            extra_image_pull_secret_names: List[str] | None = None,
            image_st4sd_runtime_core: str =
            "res-st4sd-team-official-base-docker-local.artifactory.swg-devops.com/st4sd-runtime-core",
            image_st4sd_runtime_k8s_input_s3: str =
            "res-st4sd-team-official-base-docker-local.artifactory.swg-devops.com/st4sd-runtime-k8s-input-s3",
            image_st4sd_runtime_k8s_monitoring: str =
            "res-st4sd-team-official-base-docker-local.artifactory.swg-devops.com/st4sd-runtime-k8s-monitoring",
    ):
        self.pvc_working_volume = pvc_working_volume
        self.extra_image_pull_secret_names = extra_image_pull_secret_names or []
        self.image_st4sd_runtime_core = image_st4sd_runtime_core
        self.image_st4sd_runtime_k8s_input_s3 = image_st4sd_runtime_k8s_input_s3
        self.image_st4sd_runtime_k8s_monitoring = image_st4sd_runtime_k8s_monitoring


# VV: The intention is for this class to not "do" anything i.e. it shouldn't contact kubernetes.
# It should just prepare instructions consistent with what the payload, namespace, and preset as it to do.
# This enables implementing a test suite that can run without requiring access to kubernetes or even the internet.
class NamedPackage:
    def __init__(
            self,
            ve: apis.models.virtual_experiment.ParameterisedPackage,
            namespace_presets: apis.models.virtual_experiment.NamespacePresets,
            payload_config: apis.models.virtual_experiment.PayloadExecutionOptions,
            extra_options: PackageExtraOptions | None = None
    ):
        self._extra_options = extra_options or PackageExtraOptions()
        self._log = logging.getLogger("Package")
        self._ve = ve
        self._namespace_presets = namespace_presets
        # VV: Digest format is "${digest algorithm}x", find the first x (delimiter) and keep 6 chars after that
        digest = self._ve.metadata.registry.digest.split('x', 1)[1][:6]
        self._experiment_name = '-'.join((ve.metadata.package.name, digest))
        self._experiment_name = self._experiment_name.replace(' ', '-')
        self._experiment_name = self._experiment_name.replace('_', '-')

        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H%M%S.%f")
        self._instance_name = f'{self._experiment_name}-{timestamp}'

        self._payload_config = payload_config
        # VV: Caller cares about CRD names collisions
        self._rest_uid = self.generate_new_rest_uid()

        aggregate_args = self._aggregate_runtime_args()

        # VV: Figure out if we need to store variables as embedded files - use this in _aggregate_embedded_files()
        self._workflow_variables: Dict[str, str] = self._extract_workflow_variables()

        self._check_overriding_data_files()

        # VV: These are files that we place inside a configMap
        self._embedded_files: Dict[str, str] = self._aggregate_embedded_files()

        # VV: Here look at all volumes we need to mount - inspect payload_config but NOT _embedded_files
        # due to name conflicts of Kubernetes objects we must generate the name of this configmap when we
        volume_mount_args = self._discover_volumes_volume_mounts_args()

        self._volumes: Dict[str, Volume] = volume_mount_args.volumes
        self._volume_mounts: List[VolumeMount] = volume_mount_args.volume_mounts

        # VV: It is NOT safe to read this ivar outside the constructor. use the self.runtime_args property
        # the property injects some runtime args which HAVE to be generated dynamically
        self._runtime_args: List[str] = ['-l15', '--nostamp', '--instanceName', self._instance_name]

        self._platform = self._extract_platform()
        if self._platform:
            self._runtime_args.append(f'--platform={self._platform}')

        self._runtime_args.extend(aggregate_args)
        self._runtime_args.extend(volume_mount_args.args)

        for um in self._payload_config.userMetadata:
            self._runtime_args.append(f'-m{um.name}:{um.value}')

        # VV: Expect that variables are fully resolved
        env_vars = {x.name: x.my_contents for x in self._ve.parameterisation.presets.environmentVariables}
        payload_env_vars = {x.name: x.my_contents for x in self._payload_config.environmentVariables}
        overriding = set(payload_env_vars).intersection(env_vars)
        if overriding:
            raise ValueError(f"Payload overrides environment variables of parameterisation.presets: "
                             f"{', '.join(overriding)}")

        env_vars.update(payload_env_vars)

        self._environment_variables: Dict[str, str] = env_vars

        self._validate_and_extract_runtime_args_and_env_vars_from_s3_credentials_output()
        self._validate_s3_credentials_input()

        # VV: trigger error checking of orchestrator resources
        _ = self.orchestrator_resources

        # VV: Finally validate the inputs
        provided_inputs = {x.name for x in payload_config.inputs}
        expected_inputs = {x.name for x in self._ve.metadata.registry.inputs}
        missing_inputs = sorted(expected_inputs.difference(provided_inputs))
        extra_inputs = sorted(provided_inputs.difference(expected_inputs))

        if missing_inputs or extra_inputs:
            raise apis.models.errors.InvalidInputsError(missing_inputs=missing_inputs, extra_inputs=extra_inputs)

    def _validate_and_extract_runtime_args_and_env_vars_from_s3_credentials_output(self):
        """Invoke this AFTER setting environment variables

        Method checks whether payload will end up overriding env vars that users specified due to storing outputs to S3
        """

        # VV: Search for instructions to upload key-outputs to s3/datasets and convert them into runtime_args.
        s3_out = self._payload_config.s3Output.my_contents
        if s3_out is not None:
            if isinstance(s3_out, apis.models.common.OptionFromS3Values):
                security = self._payload_config.security.s3Output.my_contents
                if isinstance(security, apis.models.common.OptionFromS3Values) is False:
                    raise ValueError("PayloadExecutionOptions.s3Store stores output to S3 but does not contain "
                                     f"PayloadExecutionOptions.security.s3Output.s3Ref instead it has {type(security)}")

                # VV: We do not actually need the bucket here but given that we ask for it for s3Input we should
                # also ask for it for output for consistency
                expecting = ['accessKeyID', 'secretAccessKey', 'endpoint', 'bucket']
                missing = set(expecting).difference(security.dict())

                if missing:
                    raise ValueError(f"PayloadExecutionOptions.security.s3Output.s3Ref is missing "
                                     f"{', '.join(missing)}")

                # VV: elaunch.py inspects these 3 env-vars when we set --s3AuthWithEnvVars
                inject_env_vars = ['S3_ACCESS_KEY_ID', 'S3_END_POINT', 'S3_SECRET_ACCESS_KEY']
                overlapping = set(inject_env_vars).intersection(self._environment_variables)

                if overlapping:
                    raise ValueError(f"PayloadExecutionOptions.security.s3Output.s3Ref injects env vars "
                                     f"{', '.join(inject_env_vars)} but they would override existing "
                                     f"environment variables")

                # VV: construct_k8s_secret_env_vars() will create the env-vars with credentials for storing files on S3
                self._runtime_args.append("--s3AuthWithEnvVars")

                s3_uri = f"s3://{security.bucket}"
                if s3_out.path:
                    path = s3_out.path if s3_out.path.startswith('/') is False else s3_out.path[1:]
                    if path:
                        s3_uri = '/'.join((s3_uri, path))

                self._runtime_args.append(f"--s3StoreToURI={s3_uri}")
            else:
                try:
                    what = f"{type(s3_out)} -> {s3_out.dict()}"
                except Exception:
                    what = s3_out
                raise NotImplementedError("PayloadExecutionOptions.s3Output must be resolved to OptionFromS3Values - "
                                          f"NamedPackage constructor received {what}")

    def _validate_s3_credentials_input(self):
        s3_in = self._payload_config.security.s3Input.my_contents

        if isinstance(s3_in, apis.models.common.OptionFromS3Values):
            # VV: We currently support using the S3 credentials to fetch data from a SINGLE bucket
            expecting = ['accessKeyID', 'secretAccessKey', 'endpoint', 'bucket']
            missing = set(expecting).difference(s3_in.dict())
            if missing:
                raise ValueError(f"PayloadExecutionOptions.security.s3Input.s3Ref is missing "
                                 f"{', '.join(missing)}")

    @property
    def instance_name(self) -> str:
        return self._instance_name

    @property
    def experiment_name(self) -> str:
        return self._experiment_name

    @property
    def pvc_working_volume(self) -> str:
        return self._extra_options.pvc_working_volume

    @property
    def embedded_files(self) -> Dict[str, str]:
        return copy.deepcopy(self._embedded_files)

    @property
    def orchestrator_resources(self) -> Dict[str, str]:
        namespace = self._namespace_presets.runtime.resources
        presets = self._ve.parameterisation.presets.runtime.resources
        execution_options = self._ve.parameterisation.executionOptions.runtime.resources
        payload = self._payload_config.runtime.resources

        named = [
            ('namespace.runtime.resources', namespace),
            ('parameterisation.presets.runtime.resources', presets),
            ('parameterisation.executionOptions.runtime.resources', execution_options),
            ('payload.orchestrator_resources', payload)
        ]

        for i, (label, me) in enumerate(named):
            for higher_label, higher in named[:i]:
                if me.cpu is not None and higher.cpu is not None and me.cpu != higher.cpu:
                    raise apis.models.errors.OverrideResourcesError(f"{label}.cpu", f"{higher_label}.cpu")

                if me.memory is not None and higher.memory is not None and me.memory != higher.memory:
                    raise apis.models.errors.OverrideResourcesError(f"{label}.memory", f"{higher_label}.memory")

        cpu = namespace.cpu or presets.cpu or execution_options.cpu or payload.cpu or "1"
        memory = namespace.memory or presets.memory or execution_options.memory or payload.memory or "1Gi"
        return {'cpu': str(cpu), 'memory': str(memory)}

    def _check_overriding_data_files(self):
        """Checks whether payload_config is valid in terms of overriding files
        """
        payload_data = {x.name: x.my_contents for x in self._payload_config.data}
        payload_input = {x.name: x.my_contents for x in self._payload_config.inputs}

        overlapping = set(payload_data).intersection(payload_input)
        if overlapping:
            raise ValueError(f"Input and Data files with conflicting names: {list(overlapping)}")

        preset_data = {x.name: x.my_contents for x in self._ve.parameterisation.presets.data}
        may_override_data_names = [x.name for x in self._ve.parameterisation.executionOptions.data]

        overlapping = set(preset_data).intersection(payload_data)
        if overlapping:
            raise apis.models.errors.OverrideDataFilesError(
                list(overlapping),
                "Payload overrides data files specified in parameterisation.presets.data")

        cannot_override = set(payload_data).difference(may_override_data_names)
        if cannot_override:
            raise apis.models.errors.OverrideDataFilesError(
                list(cannot_override),
                "Payload overrides data files which are not specified in parameterisation.executionOptions.data")

    def _aggregate_embedded_files(self) -> Dict[str, str]:
        """This returns files that we wish to provide as input, data, etc AND must be embedded i.e.

        Embedded files are provided in the form of:

        {"name": the name of the file, "value": the contents of the file}

        It assumes that all parametrisation_presets.data and payload_config.data have been "resolved"
        i.e. calling data[index].my_contents returns the string representation of the contents of the file

        It also assumes that we have already discovered the variables for this virtual experiment because
        these go into the "input/st4sd-variables.yaml" file.
        """
        preset_data = {os.path.join('data', x.name): x.my_contents for x in self._ve.parameterisation.presets.data
                       if isinstance(x.my_contents, six.string_types)}
        payload_data = {os.path.join('data', x.name): x.my_contents for x in self._payload_config.data
                        if isinstance(x.my_contents, six.string_types)}
        payload_input = {os.path.join('input', x.name): x.my_contents for x in self._payload_config.inputs
                         if isinstance(x.my_contents, six.string_types)}

        aggregate = preset_data
        aggregate.update(payload_data)
        aggregate.update(payload_input)

        if self._workflow_variables:
            # VV: If we want to add any workflow variables we just need to have one extra embedded file
            dict_variables = {
                'global': self._workflow_variables
            }
            str_variables = yaml.dump(dict_variables)
            aggregate['input/st4sd-variables.yaml'] = str_variables

        return aggregate

    def _aggregate_runtime_args(self) -> List[str]:
        def is_argument_overriding(param_name: str, base: argparse.Namespace, extend: argparse.Namespace) -> bool:
            if ((extend.__getattribute__(param_name) is not None and base.__getattribute__(param_name) is not None) and
                    (extend.__getattribute__(param_name) != base.__getattribute__(param_name))):
                return True
            return False

        parser = apis.models.common.parser_important_elaunch_arguments()
        namespace_args = self._namespace_presets.runtime.args or []
        try:
            opt_namespace, _ = parser.parse_known_args(namespace_args)
        except BaseException as e:
            raise ValueError(f"Invalid namespace arguments {e} - "
                             f"check the contents of the ConfigMap named st4sd-runtime-service")

        parser = apis.models.common.parser_important_elaunch_arguments()
        package_presets = self._ve.parameterisation.presets
        package_preset_args = package_presets.runtime.args
        package_execution_options_args = self._ve.parameterisation.executionOptions.runtime.args

        try:
            opt_presets, _ = parser.parse_known_args(package_preset_args)
        except BaseException as e:
            raise apis.models.errors.ApiError(f"Invalid parameterisation.presets.runtime.args: {e}")

        try:
            opt_exec_options, _ = parser.parse_known_args(package_execution_options_args)
        except BaseException as e:
            raise apis.models.errors.ApiError(f"Invalid parameterisation.executionOptions.runtime.args: {e}")

        for arg_name in vars(opt_presets):
            if is_argument_overriding(arg_name, opt_presets, opt_namespace):
                raise apis.models.errors.InvalidElaunchParameterChoices(
                    arg_name,
                    opt_presets.__getattribute__(arg_name),
                    [opt_namespace.__getattribute__(arg_name)],
                    f"parameterisation.presets.runtime.args override commandline argument "
                    f"--{arg_name}={opt_namespace.__getattribute__(arg_name)} which is part of "
                    f"the experiment namespace presets"
                )

        for arg_name in vars(opt_exec_options):
            if is_argument_overriding(arg_name, opt_exec_options, opt_namespace):
                raise apis.models.errors.InvalidElaunchParameterChoices(
                    arg_name,
                    opt_exec_options.__getattribute__(arg_name),
                    [opt_namespace.__getattribute__(arg_name)],
                    f"parameterisation.executionOptions.runtime.args override commandline argument "
                    f"--{arg_name}={opt_namespace.__getattribute__(arg_name)} which is part of "
                    f"the experiment namespace presets"
                )

            if is_argument_overriding(arg_name, opt_exec_options, opt_presets):
                raise apis.models.errors.InvalidElaunchParameterChoices(
                    arg_name,
                    opt_exec_options.__getattribute__(arg_name),
                    [opt_namespace.__getattribute__(arg_name)],
                    f"parameterisation.executionOptions.runtime.args override commandline argument "
                    f"--{arg_name}={opt_namespace.__getattribute__(arg_name)} which is part of "
                    f"parameterisation.executionOptions.runtime.args"
                )

        parser = apis.models.common.parser_important_elaunch_arguments()
        payload_args = self._payload_config.runtime.args
        try:
            opt_payload, _ = parser.parse_known_args(payload_args)
        except BaseException as e:
            raise ValueError(f"Payload contains invalid runtime.args: {e}")

        for arg_name in vars(opt_payload):
            if is_argument_overriding(arg_name, opt_payload, opt_presets):
                raise apis.models.errors.InvalidElaunchParameterChoices(
                    arg_name,
                    opt_payload.__getattribute__(arg_name),
                    [opt_presets.__getattribute__(arg_name)],
                    f"The payload runtime.args override commandline argument override commandline argument "
                    f"--{arg_name}={opt_presets.__getattribute__(arg_name)} which is part of "
                    f"parameterisation.presets.runtime.args"
                )
            if is_argument_overriding(arg_name, opt_payload, opt_exec_options):
                raise apis.models.errors.InvalidElaunchParameterChoices(
                    arg_name,
                    opt_payload.__getattribute__(arg_name),
                    [opt_exec_options.__getattribute__(arg_name)],
                    f"The payload runtime.args override commandline arguments override commandline argument "
                    f"--{arg_name}={opt_presets.__getattribute__(arg_name)} which is part of "
                    f"parameterisation.executionOptions.runtime.args"
                )

            if is_argument_overriding(arg_name, opt_payload, opt_namespace):
                raise apis.models.errors.InvalidElaunchParameterChoices(
                    arg_name,
                    opt_payload.__getattribute__(arg_name),
                    [opt_namespace.__getattribute__(arg_name)],
                    f"The payload runtime.args override commandline argument override commandline argument "
                    f"--{arg_name}={opt_namespace.__getattribute__(arg_name)} which is part of the experiment "
                    f"namespace presets"
                )

        return namespace_args + package_preset_args + package_execution_options_args + payload_args

    def _extract_workflow_variables(self) -> Dict[str, str]:
        ve_vars = {x.name: x.my_contents for x in self._ve.parameterisation.presets.variables}
        payload_vars = {x.name: x.my_contents for x in self._payload_config.variables}
        overriding = set(ve_vars).intersection(payload_vars)
        if overriding:
            raise ValueError(f"Payload overrides workflow variables of parameterisation.presets: "
                             f"{', '.join(overriding)}")

        modifiable_variables = self._ve.parameterisation.executionOptions.variables
        for name in payload_vars:
            for allow_var in modifiable_variables:
                if allow_var.name == name:
                    if not allow_var.my_contents:
                        # VV: Virtual Experiment allows setting this value to anything
                        break

                    if allow_var.value is not None:
                        # VV: The value could be anything
                        break
                    elif allow_var.valueFrom and payload_vars[name] in [x.value for x in allow_var.valueFrom]:
                        # VV: The value *MUST* be one of the values defined in  valueFrom
                        break

                    raise apis.models.errors.OverrideVariableError(
                        name, payload_vars[name],
                        f"Payload variable {name} = {payload_vars[name]} does not have an allowed "
                        f"value. Valid choices are {allow_var.my_contents}")
            else:
                raise apis.models.errors.OverrideVariableError(
                    name, payload_vars[name],
                    f"Payload sets variable {name} = {payload_vars[name]} for which "
                    f"parameterisation.executionOptions.variables has no rule.")

        ve_vars.update(payload_vars)

        # VV: finally add in any default variables from `modifiable_variables` - these are variables which COULD be
        # overridden but for which the user decided not to provide a value
        for allow_var in modifiable_variables:
            if allow_var.value is None and not allow_var.valueFrom:
                # VV: The developer doesn't care what value users put in here. If they do not specify a value
                # then the value is whatever the platform specifies
                continue
            if allow_var.name not in payload_vars:
                # VV: The default value is the 1st in the entry
                if allow_var.value is not None:
                    ve_vars[allow_var.name] = allow_var.value
                elif allow_var.valueFrom:
                    ve_vars[allow_var.name] = allow_var.valueFrom[0].value

        return ve_vars

    @property
    def volume_mounts(self) -> List[VolumeMount]:
        return copy.deepcopy(sorted(self._volume_mounts, key=lambda x: x.volume_name))

    @property
    def volumes(self) -> Dict[str, Volume]:
        return copy.deepcopy({x: self._volumes[x] for x in sorted(self._volumes)})

    def _generate_ownerref_to_workflow(self, k8s_workflow_uuid: str) -> List[Dict[str, Any]]:
        return [{
            'apiVersion': apis.models.constants.K8S_WORKFLOW_GROUP + '/' + apis.models.constants.K8S_WORKFLOW_VERSION,
            'blockOwnerDeletion': True,
            'controller': True,
            'kind': 'Workflow',
            'name': self.rest_uid,
            'uid': k8s_workflow_uuid}]

    @property
    def workflow_variables(self) -> Dict[str, str]:
        return copy.deepcopy(self._workflow_variables)

    @property
    def environment_variables_raw(self) -> Dict[str, str]:
        return copy.deepcopy(self._environment_variables)

    def _discover_volumes_volume_mounts_args(
            self,
    ) -> VolumesVolumeMountsArgs:
        ret = VolumesVolumeMountsArgs({}, [], [])

        volumes = self._payload_config.volumes

        for idx, volume in enumerate(volumes):
            volume_name = 'volume%d' % idx

            # VV: Filter out empty entries
            volume_raw = volume.type.dict()

            # VV: Ensure that there's exactly 1 volume-type definition
            name_pvc = volume_raw.get('persistentVolumeClaim', {}).get('claimName')
            name_config = volume_raw.get('configMap', {}).get('name')
            name_dataset = volume_raw.get('dataset', {}).get('name')
            name_secret = volume_raw.get('secret', {}).get('name')
            if sum([1 for x in [name_pvc, name_config, name_dataset, name_secret] if x]) != 1:
                raise ValueError("Volume %s must use exactly 1 of the fields "
                                 "persistentVolumeClaim, configMap, dataset, secret" % volume_raw)
            underlying_name = name_pvc or name_config or name_dataset or name_secret
            mountpath = volume_raw.get('mountPath', os.path.join(ROOT_VOLUME_MOUNTS, underlying_name))

            volume_raw['mountPath'] = mountpath

            app_dep = volume.applicationDependency
            if app_dep:
                ret.args.append(f'--applicationDependencySource={":".join((app_dep, mountpath))}')

            volume_entry = {'name': volume_name}

            # VV: Populate this with whatever the user put in the payload
            volume_mount_entry = {}

            if name_pvc:
                volume_uid = ':'.join(('persistentVolumeClaim', name_pvc))
                if name_pvc == self.pvc_working_volume:
                    raise ValueError("Volume \"%s\" attempts to mount working-volume as a PVC, this is not permitted" %
                                     name_pvc)
                volume_entry['persistentVolumeClaim'] = {'claimName': name_pvc}
                volume_mount_entry = volume_raw['persistentVolumeClaim']
            elif name_config:
                volume_uid = ':'.join(('configMap', name_config))
                volume_entry['configMap'] = {'name': name_config}
                volume_mount_entry = volume_raw['configMap']
            elif name_dataset:
                volume_uid = ':'.join(('persistentVolumeClaim', name_dataset))
                volume_entry['persistentVolumeClaim'] = {'claimName': name_dataset}
                volume_mount_entry = volume_raw['dataset']
            elif name_secret:
                volume_uid = ':'.join(('secret', name_secret))
                volume_entry['secret'] = {'secretName': name_secret}
                volume_mount_entry = volume_raw['secret']
            else:
                raise ValueError("InputVolume \"%s\" defines an unknown volume type" % volume)

            # VV: if a volume is defined multiple times keep just one reference
            if volume_uid not in ret.volumes:
                ret.volumes[volume_uid] = Volume(volume_name, volume_entry)
            else:
                volume_entry = ret.volumes[volume_uid]
                volume_name = volume_entry.name

            if ':' in mountpath:
                raise ValueError("mountPath \"%s\" of %s contains the illegal character ':'" % (mountpath, volume))

            # VV: Start with some defaults that make sense
            vm_config = {'name': volume_name, 'mountPath': mountpath, 'readOnly': True}

            # VV: Then accept whatever the user put in the payload
            vm_config.update({x: volume_mount_entry[x] for x in volume_mount_entry if x in ['subPath', 'readOnly']})

            ret.volume_mounts.append(VolumeMount(volume_name, vm_config))

        # VV: Finally, look at the definition of the base packages we'll need to add volumes and volume mounts for
        # base packages that live on `datasets`.
        # Mount each dataset under ${ROOT_DATASET_WORKFLOW_DEFINITIONS}/${name of base package}
        for package in self._ve.base.packages:
            source = package.source
            if source.dataset is None:
                continue
            source = source.dataset

            dataset = source.location.dataset

            for v in ret.volumes:
                if ret.volumes[v].config.get('persistentVolumeClaim', {}).get('claimName') != dataset:
                    continue
                dataset_volume_uid = v
                break
            else:
                dataset_volume_uid = f"base:{package.name}"
                ret.volumes[dataset_volume_uid] = Volume(
                    f"base-{package.name}", {
                        'name': f"base-{package.name}",
                        'persistentVolumeClaim': {'claimName': dataset}})

            volume = ret.volumes[dataset_volume_uid]
            ret.volume_mounts.append(VolumeMount(volume.name, {
                'name': volume.name,
                'mountPath': os.path.join(ROOT_DATASET_WORKFLOW_DEFINITIONS, package.name)}))

        return ret

    @property
    def runtime_args(self) -> List[str]:
        own = [x for x in self._runtime_args]
        # VV: we're also generating some userMetadata values whose value depends on rest_uid which can "change" when
        # there's a conflict. Therefore we must dynamically generate these runtime args
        generated = [f"-m{name}:{value}" for name, value in self.k8s_labels.items()]

        return own + generated

    def generate_new_rest_uid(self) -> str:
        """Generates, and stores, a new rest-uid for this virtual experiment instance

        This could be useful if there is a conflict with workflow instance names on the namespace
        """
        rand = random.SystemRandom()
        characters = string.digits + string.ascii_lowercase
        rand_str = ''.join((rand.choice(characters) for _ in range(8)))
        self._rest_uid = '-'.join((self._experiment_name, rand_str))
        return self._rest_uid

    @property
    def rest_uid(self) -> str:
        return self._rest_uid

    @property
    def platform(self) -> str | None:
        return self._platform

    def _extract_platform(self) -> str:
        preset_platform = self._ve.parameterisation.presets.platform
        execoptions_platforms = self._ve.parameterisation.executionOptions.platform
        payload_platform = self._payload_config.platform

        if (preset_platform is None) and (not execoptions_platforms):
            # VV: nothing in presets or executionOptions - the user must be able to do whatever they want
            return payload_platform

        if (preset_platform is not None) and (payload_platform is not None) and (payload_platform != preset_platform):
            # VV: Preset specified a platform user provided something that's not what the preset says
            raise apis.models.errors.OverridePlatformError(
                payload_platform, f"Payload overrides parameterisation.presets.platform={preset_platform}")

        if preset_platform:
            return preset_platform
        elif payload_platform is None:
            if execoptions_platforms:
                return execoptions_platforms[0]
        else:
            if execoptions_platforms and payload_platform not in execoptions_platforms:
                raise apis.models.errors.OverridePlatformError(
                    payload_platform, f"Payload overrides parameterisation.executionOptions"
                                      f".platform={execoptions_platforms}")
            return payload_platform

    @property
    def k8s_labels(self) -> Dict[str, str]:
        logging.getLogger("NamedPackage").warning("This method is not fully implemented - not injecting all k8s labels")

        # VV: Kubernetes labels can hold up to 64 characters - which is the length of sha256 therefore we cannot store
        # the entire string "sha256xYYYY" where YYY is the sha256 hash

        experiment_id = apis.models.common.PackageIdentifier.from_parts(
            package_name=self._ve.metadata.package.name,
            tag=None,
            digest=self._ve.metadata.registry.digest).identifier

        injected = {
            'rest-uid': self.rest_uid,
            'workflow': self.rest_uid,
            # VV: This can be too long to fit as a k8s label
            'experiment-id': experiment_id,
            # VV: CANNOT use dots as delimiters here.
            # We want to inject these labels as userMetadata tog and mongoDb does not like "dots" in field names.
            'st4sd-package-name': self._ve.metadata.package.name,
            'st4sd-package-digest': self._ve.metadata.registry.digest,
        }
        return injected

    def get_path_to_multi_package_pvep(self) -> str:
        return os.path.join(apis.models.constants.ROOT_STORE_DERIVED_PACKAGES,
                            self._ve.metadata.package.name,
                            self._ve.get_packages_identifier())

    @property
    def workflow_spec_package(self) -> Dict[str, str]:
        base = self._ve.base.packages

        if len(base) > 1:
            self._log.warning(f"HACK: Virtual experiment {self._ve.metadata.package.name} "
                              f"consists of multiple packages - assume it exists in "
                              f"{apis.models.constants.ROOT_STORE_DERIVED_PACKAGES}")
            return {
                'fromPath': self.get_path_to_multi_package_pvep()
            }

        if len(base) != 1:
            raise NotImplementedError(f"We currently support virtual experiments with just 1 base package this one "
                                      f"has {len(base)}")
        base = base[0]
        package = {}

        source = base.source
        if source.git is not None:
            git_location = source.git.location

            branch = git_location.branch or git_location.tag
            package['url'] = git_location.url

            if git_location.commit:
                package['commitId'] = git_location.commit
            elif branch:
                package['branch'] = branch
            git_sec = source.git.security

            if git_sec and git_sec.oauth:
                if isinstance(git_sec.oauth, apis.models.virtual_experiment.SourceGitSecurityOAuth):
                    value = git_sec.oauth.my_contents
                    if isinstance(value, apis.models.common.OptionFromSecretKeyRef):
                        if value.name is not None:
                            package['gitsecret'] = value.name
                    else:
                        raise NotImplementedError(
                            f"We do not know how to generate a Workflow.spec.package for "
                            f"Git source with security.oauth {type(value)}")
                else:
                    raise NotImplementedError(
                        f"We do not know how to generate a Workflow.spec.package for "
                        f"Git source with security.oauth")
            elif git_sec is None:
                pass
            else:
                raise NotImplementedError(
                    f"We do not know how to generate a Workflow.spec.package for "
                    f"Git source with security {type(git_sec)}")
            package['fromPath'] = base.config.path
            package['withManifest'] = base.config.manifestPath
        elif source.dataset is not None:
            if source.dataset.location.dataset is None:
                raise NotImplementedError(
                    f"We do not know how to generate a Workflow.spec.package for {type(source).__name__} "
                    f"source.location with keys {list(source.dataset.location.dict(exclude_none=True))}")

            manifest = base.config.manifestPath
            if manifest and os.path.isabs(manifest) is False:
                manifest = os.path.join(ROOT_DATASET_WORKFLOW_DEFINITIONS, base.name, manifest)

            if manifest:
                manifest = os.path.normpath(manifest)

            path = base.config.path or ''
            if os.path.isabs(path) is False:
                path = os.path.join(ROOT_DATASET_WORKFLOW_DEFINITIONS, base.name, path)

            package['fromPath'] = os.path.normpath(path)
            package['withManifest'] = manifest
        else:
            raise NotImplementedError(
                f"We do not know how to generate a Workflow.spec.package for {type(source).__name__} source with keys "
                f"{list(source.dict(exclude_none=True))}")

        return package

    @classmethod
    def _to_filename(cls, x: apis.models.common.Option, file_type: str) -> str:
        value = x.my_contents
        from_s3 = (apis.models.common.OptionFromS3Values, apis.models.common.OptionFromDatasetRef)

        if isinstance(value, six.string_types):
            return os.path.join(ROOT_EMBEDDED_FILES, file_type, x.name)
        elif value is None or isinstance(value, from_s3):
            # VV: If it has no value then it definitely cannot be embedded therefore it has to be S3
            path = value.path or x.name if value is not None else x.name
            if path.startswith(os.path.sep):
                # VV: joining ("hello" with "/hi there") produces "/hi there"
                path = path[1:]

            if value.rename:
                path = ':'.join((path, value.rename))

            return os.path.join(ROOT_S3_FILES, file_type, path)
        else:
            raise NotImplementedError(f"Cannot construct filename of data {x.dict()}")

    @property
    def input_file_names(self) -> List[str]:
        # VV: TODO I think I'm missing something here, how do we handle input A on S3 and input B in "embedded_files" ?
        # VV: st4sd-runtime-k8s EXPECTS paths to s3 input files to start with `/tmp/s3-root-dir/input`
        return [self._to_filename(x, 'input') for x in self._payload_config.inputs]

    @property
    def data_file_names(self) -> List[str]:
        # VV: st4sd-runtime-k8s EXPECTS paths to s3 data files to start with `/tmp/s3-root-dir/data`
        preset_data = [self._to_filename(x, 'data') for x in self._ve.parameterisation.presets.data]
        payload_data = [self._to_filename(x, 'data') for x in self._payload_config.data]

        return preset_data + payload_data

    @property
    def image_pull_secrets(self) -> List[str]:
        secrets = self._extra_options.extra_image_pull_secret_names
        for package in self._ve.base.packages:
            for dimr in package.dependencies.imageRegistries:
                value = dimr.security.my_contents
                if isinstance(value, apis.models.common.OptionFromSecretKeyRef) is False:
                    raise ValueError(f"base[{package.name}].dependencies.imageRegistries={dimr.dict()} "
                                     f"does not contain security.valueFrom.secretKeyRef")
                if value.name is None:
                    raise ValueError(f"base[{package.name}].dependencies.imageRegistries={dimr.dict()} "
                                     f"does not contain security.name")
                if value.name not in secrets:
                    secrets.append(value.name)

        return secrets

    @property
    def _spec_s3_bucket_input(self) -> Dict[str, str] | None:
        """ This returns what should go into Workflow.spec.s3BucketInput
            if self.s3_credentials.get('dataset', '') == '':
                body['spec']['s3BucketInput'] = {key: {'value': self.s3_credentials[key]} for key in
                                                 ['accessKeyID', 'secretAccessKey', 'bucket', 'endpoint']}
            else:
                body['spec']['s3BucketInput'] = {'dataset': self.s3_credentials['dataset']}

            body['spec']['s3FetchFilesImage'] = self._extra_options.image_st4sd_runtime_k8s_input_s3
        """
        s3 = self._payload_config.security.s3Input.my_contents
        if s3 is None:
            return None
        elif isinstance(s3, apis.models.common.OptionFromS3Values):
            # VV: Again, this can only be fully resolved
            return {
                x: {
                    # VV: We are injecting these environment variables in construct_k8s_secret_env_vars()
                    'valueFrom': {
                        'secretKeyRef': {
                            'name': f'env-{self.rest_uid}',
                            'key': f"ST4SD_S3_IN_{x.upper()}",
                        }
                    }
                    # VV: notice that we do not use the value of x, it's in the Secret object
                    # also notice that we use the BUCKET name - i.e. we can download from a SINGLE bucket only
                    # this is a limitation of current implementation of s3-runtime-k8s-input-s3 (can be addressed)
                } for x in ['bucket', 'endpoint', 'accessKeyID', 'secretAccessKey']
            }
        elif isinstance(s3, apis.models.common.OptionFromDatasetRef):
            return {'dataset': s3.name}
        else:
            raise NotImplementedError(f"Unknown s3 credentials {s3}")

    def construct_k8s_workflow(self) -> Dict[str, str]:
        variable_files = []
        if self._workflow_variables:
            variable_files = [os.path.join(ROOT_EMBEDDED_FILES, 'input', 'st4sd-variables.yaml')]

        volumes = [self._volumes[x].config for x in self._volumes]
        volume_mounts = [x.config for x in self._volume_mounts]

        # VV: the name of the ConfigMap for embedded_files depends on the rest-uid which is the name of this object.
        # Therefore, we should generate the `volumes` and `volume_mounds` entries just-in-time

        # VV: IF there are embedded files which we do NOT expect to find on S3 - create volume and volumeMount entries
        if self._embedded_files:
            cm_items = []
            for name in self._embedded_files:
                if os.path.sep in name:
                    cm_items.append({
                        'key': os.path.basename(name),
                        'path': name,
                    })
                else:
                    cm_items.append({'key': name})

            volumes.append({
                'name': 'embedded-files',
                'configMap': {
                    'name': f'files-{self.rest_uid}',
                    'items': cm_items
                }
            })

            volume_mounts.append({
                'name': 'embedded-files',
                'mountPath': ROOT_EMBEDDED_FILES,
            })

        env_vars = [{'name': 'INSTANCE_DIR_NAME', 'value': '%s.instance' % self._instance_name}]

        s3_creds = self._payload_config.security.s3Output.my_contents
        if isinstance(s3_creds, apis.models.common.OptionFromS3Values):
            # VV: Notice that there's no `bucket` here - this is by design, we also don't need the values of these
            # env vars, this is something that self.construct_k8s_secret_env_vars() handles for us
            secret_name = f'env-{self.rest_uid}'
            env_vars.extend([{
                'name': what,
                'valueFrom': {
                    'secretKeyRef': {
                        'name': secret_name,
                        'key': what,
                    }
                }
            } for what in sorted(['S3_ACCESS_KEY_ID', 'S3_END_POINT', 'S3_SECRET_ACCESS_KEY'])])

        k8s_labels = self.k8s_labels

        body = {
            'apiVersion': apis.models.constants.K8S_WORKFLOW_GROUP + '/' + apis.models.constants.K8S_WORKFLOW_VERSION,
            "kind": "Workflow",
            "metadata": {
                "name": self.rest_uid,
                # VV: Only create k8s labels that are actually valid.
                "labels": {x: k8s_labels[x] for x in k8s_labels if apis.models.common.valid_k8s_label(x, k8s_labels[x])}
            },
            "spec": {
                "image": self._extra_options.image_st4sd_runtime_core,
                "package": self.workflow_spec_package,
                "imagePullSecrets": self.image_pull_secrets,
                "env": env_vars,
                "workingVolume": {
                    "name": "working-volume",
                    "persistentVolumeClaim": {
                        "claimName": self.pvc_working_volume
                    }
                },
                "inputs": self.input_file_names,
                "data": self.data_file_names,
                "variables": variable_files,
                "volumes": volumes,
                "volumeMounts": volume_mounts,
                "additionalOptions": self.runtime_args,
                "resources": {
                    'elaunchPrimary': self.orchestrator_resources
                }
            }
        }

        # VV: We will be storing the values of these environment variables in a Secret
        if self._environment_variables:
            obj_name = f"env-{self.rest_uid}"
            env = body['spec']['env']
            for x in self._environment_variables:
                env.append({'name': x, 'valueFrom': {'secretKeyRef': {'key': x, 'name': obj_name}}})

        s3_bucket_input = self._spec_s3_bucket_input
        if s3_bucket_input:
            body['spec']['s3BucketInput'] = s3_bucket_input
            body['spec']['s3FetchFilesImage'] = self._extra_options.image_st4sd_runtime_k8s_input_s3

        return body

    def construct_k8s_configmap_embedded_files(self, k8s_workflow_uuid: str) -> Dict[str, Any] | None:
        """

        Args:
            k8s_workflow_uuid: The uuid in the metadata field of the kubernetes CRD object for this instance
        """

        # VV: we cannot have "/" in filenames but it would be nice if we could see at a glance which files
        # are "/data" and which are "/input". So here we're removing any "/prefix/directories" and mounting the
        # files in the appropriate place inside self.construct_k8s_workflow()
        files = {os.path.basename(may_path): value for (may_path, value) in self.embedded_files.items()}

        if not files:
            return

        body = {
            'metadata': {
                'name': f'files-{self.rest_uid}',
                'labels': {
                    'workflow': self.rest_uid
                },
                'ownerReferences': self._generate_ownerref_to_workflow(k8s_workflow_uuid)
            },
            # VV: expect that self.embedded_files contains "resolved" files i.e {name: contents}
            'data': files
        }
        return body

    def construct_k8s_secret_env_vars(self, k8s_workflow_uuid: str) -> Dict[str, Any] | None:
        """

        Args:
            k8s_workflow_uuid: The uuid in the metadata field of the kubernetes CRD object for this instance
        """
        env_vars = self.environment_variables_raw

        agg_env_vars = env_vars

        # VV: We do not want to expose S3 credentials inside the Workflow object because we store that as a YAML file
        # inside the PVC that holds the virtual experiment directory. Here, we inject new environment variables
        # that st4sd-runtime-k8s can use to pass them on to the initContainer running st4sd-runtime-k8s-input-s3

        s3_creds = self._payload_config.security.s3Input.my_contents

        if isinstance(s3_creds, apis.models.common.OptionFromS3Values):
            s3_creds = s3_creds.dict()
            agg_env_vars.update({
                f'ST4SD_S3_IN_{x.upper()}': s3_creds[x] for x in s3_creds
            })

        s3_creds = self._payload_config.security.s3Output.my_contents
        if isinstance(s3_creds, apis.models.common.OptionFromS3Values):
            # VV: Notice that there's no `bucket` here - this is by design
            env_names = {
                'accessKeyID': 'S3_ACCESS_KEY_ID',
                'endpoint': 'S3_END_POINT',
                'secretAccessKey': 'S3_SECRET_ACCESS_KEY'
            }
            s3_creds = s3_creds.dict()
            agg_env_vars.update({env_names[x]: s3_creds[x] for x in env_names})

        if not agg_env_vars:
            return None

        body = {
            'metadata': {
                'name': f'env-{self.rest_uid}',
                'labels': {
                    'workflow': self.rest_uid
                },
                'ownerReferences': self._generate_ownerref_to_workflow(k8s_workflow_uuid)
            },
            'data': {
                # VV: For now support env vars that are strings - i.e. no secretKeyRef
                str(x): base64.b64encode(agg_env_vars[x].encode('utf-8')).decode('utf-8') for x in agg_env_vars}
        }
        return body


def create_secret_for_git_package_source_security(
    source_git: apis.models.virtual_experiment.BasePackageSourceGit,
    db_secrets: apis.db.secrets.SecretsStorageTemplate,
) -> bool:
    """If  @source_git uses raw credentials to configure the oauth-token for git clone, the method creates Secret with
    the oauth-token and updates the source to use that instead of the raw token

    Args:
        source_git:
            The package source which describes how to retrieve the package definition from Git
        db_secrets:
            The database of secrets

    Returns:
        True if it creates a Secret, False otherwise

    Raises:
        apis.models.errors.ApiError:
            If the method is unable to create a Secret for some reason, the exception explains the reason.
    """
    security = source_git.security
    if security is None:
        # VV: The base package does not contain security metadata - it must be using the "default" Git oauth-token
        return False

    if isinstance(security.oauth, apis.models.virtual_experiment.SourceGitSecurityOAuth):
        if security.oauth.valueFrom is not None and security.oauth.valueFrom.secretKeyRef is not None:
            # VV: The base package is already using a Secret for its token
            return False
        elif security.oauth.value is not None:
            # VV: The base package contains an oauth-token, we need to create a secret for it
            pass
        else:
            raise apis.models.errors.ApiError(f"Not implemented git source.security {type(security)}")
    elif security.oauth is None and len(security.dict(exclude_none=True).keys()) > 0:
        raise apis.models.errors.ApiError(f"Not implemented git source.security {type(security)}")

    # VV: this is an embedded oauth-token, convert it
    url: str = source_git.location.url
    oauth_token = security.oauth.value
    # VV: Turns https://hello/world.git into world
    name = os.path.splitext(os.path.basename(url))[0]
    name = f"git-oauth-{name}-{binascii.b2a_hex(os.urandom(6)).decode('utf-8')}"

    secret = apis.db.secrets.Secret(name=name, data={'oauth-token': oauth_token})

    with db_secrets:
        db_secrets.secret_create(secret)

    source_git.security.oauth = apis.models.common.Option(
        valueFrom=apis.models.common.OptionValueFrom(
            secretKeyRef=apis.models.common.OptionFromSecretKeyRef(name=name, key="oauth-token")))

    return True


def create_secret_for_s3_package_source_security(
    source_s3: apis.models.virtual_experiment.BasePackageSourceS3,
    db_secrets: apis.db.secrets.SecretsStorageTemplate,
) -> bool:
    """If  @source_s3 uses raw credentials to configure the S3 credentials, the method creates Secret with
    the credentials and updates the source to use that instead of the raw secrets.

    Args:
        source_s3:
            The package source which describes how to retrieve the package definition from S3
        db_secrets:
            The database of secrets

    Returns:
        True if it creates a Secret, False otherwise

    Raises:
        apis.models.errors.ApiError:
            If the method is unable to create a Secret for some reason, the exception explains the reason.
    """
    if not source_s3.security or not source_s3.security.credentials or not source_s3.security.credentials.value:
        return False

    credentials = source_s3.security.credentials

    if credentials.value is None or (
            credentials.value.accessKeyID is None
            and credentials.value.secretAccessKey is None
    ):
        return False

    rand = random.Random()
    characters = string.ascii_letters + string.digits
    secret_name = ''.join((rand.choice(characters) for x in range(10)))

    data = {}
    credentials.valueFrom = apis.models.virtual_experiment.SourceS3SecurityCredentialsValueFrom(
       secretName=secret_name,
    )

    if credentials.value.accessKeyID is not None:
        data['S3_ACCESS_KEY_ID'] = credentials.value.accessKeyID
        credentials.valueFrom.keyAccessKeyID = "S3_ACCESS_KEY_ID"

    if credentials.value.secretAccessKey is not None:
        data['S3_SECRET_ACCESS_KEY'] = credentials.value.secretAccessKey
        credentials.valueFrom.keySecretAccessKey = "S3_SECRET_ACCESS_KEY"

    credentials.value = None
    secret = apis.db.secrets.Secret(
        name=secret_name,
        data=data
    )

    with db_secrets:
        db_secrets.secret_create(secret)

    return True



def rewrite_security_of_package_source(
        base: apis.models.virtual_experiment.BasePackage,
        db_secrets: apis.db.secrets.SecretsStorageTemplate,
):
    """If any package uses raw credentials to configure the security of its package source it creates
    a secret and references the credentials instead.

    Inspects an experiment definition and if it contains the field package.type.git.oauth-token it
    validates that it's also cloning a https:// url. Then it creates a Secret object to hold that token
    and rewrites the experiment definition to use the new Secret object

    Arguments:
        base: The description of a base package - may be updated to reflect new security options
        db_secrets: The database of secrets

    Returns:
        True if it creates a Secret, False otherwise

    Raises:
        apis.models.errors.ApiError:
            If the method is unable to create a Secret for some reason, the exception explains the reason.
    """
    source = base.source

    if source.git:
        return create_secret_for_git_package_source_security(
            source_git=source.git,
            db_secrets=db_secrets,
        )
    elif source.dataset:
        return False
    elif source.s3:
        return create_secret_for_s3_package_source_security(
            source_s3=source.s3,
            db_secrets=db_secrets,
        )

    return False



def prepare_parameterised_package_for_download_definition(
        ve: apis.models.virtual_experiment.ParameterisedPackage,
        db_secrets: apis.db.secrets.SecretsStorageTemplate
):
    """Processes parameterised package to securely use credentials for retrieving its definition

    For example, if the parameterised package lives on a Git server and contains an oauth-token, the PVEP will be
    updated so that it references a secret which holds the git oauth-token.

    Arguments:
        ve: The parameterised package to prepare.
        db_secrets: The database of secrets
    """
    base_packages = ve.base.packages
    for bp in base_packages:
        try:
            rewrite_security_of_package_source(bp, db_secrets=db_secrets)
        except Exception as e:
            logger.warning(f"Cannot create OAuth secret for {ve.metadata.package.name}/{bp.name} due to {e}. "
                           f"Traceback {traceback.format_exc()}")
            raise apis.runtime.errors.CannotCreateOAuthSecretError(bp.name)


def get_and_validate_parameterised_package(
        ve: apis.models.virtual_experiment.ParameterisedPackage,
        package_metadata_collection: apis.storage.PackageMetadataCollection,
):
    """Retrieves the files of a parameterised package and validates its definition

    Notes: ::

        - If PVEP is "derived" method also stores the concretised FlowIR definition on the filesystem.
        - Database should not be already open

    Args:
        ve:
            the parameterised virtual experiment package (PVEP)
        package_metadata_collection:
            The collection of the package metadata

    Raises:
        apis.models.errors.ApiError:
            If the virtual experiment is invalid or inconsistent with the PVEP
    """
    parser = apis.models.common.parser_important_elaunch_arguments()
    try:
        _ = parser.parse_known_args(ve.parameterisation.presets.runtime.args)
    except BaseException as e:
        raise apis.models.errors.ApiError(f"Invalid parameterisation.presets.runtime.args {e}")

    # VV: Start with a fresh MetadataRegistry
    ve.metadata.registry = apis.models.virtual_experiment.MetadataRegistry()

    with package_metadata_collection:
        # VV: Test parameterisation platforms and if not provided, find common platforms of base packages
        known_platforms = ve.get_known_platforms() or package_metadata_collection.get_common_platforms()
        valid_platforms = set()
        invalid_platforms = set()

        for bp in ve.base.packages:
            metadata = package_metadata_collection.get_metadata(bp.name)
            concrete = metadata.concrete.copy()
            # VV: An experiment (e.g. a derived one from a relationship) may contain multiple base-packages
            # we must check each individual base-package for the platforms it contains.
            # We must also make sure that all the platforms that are in the parameterisation of the experiment
            # have been validated. If a platform is invalid even for just 1 of the packages then we consider
            # the entire experiment to be broken.
            for p in set(known_platforms).intersection(concrete.platforms):
                try:
                    concrete.configure_platform(p)
                    errors = concrete.validate(metadata.top_level_folders)
                    valid_platforms.add(p)
                except Exception as e:
                    invalid_platforms.add(p)
                    errors = [e]

                if len(errors) and all([
                    isinstance(x, experiment.model.errors.FlowIRPlatformUnknown) for x in errors
                ]):
                    logger.warning(f"{bp.name} does not contain platform {p} - will not extract information from it")
                    continue

                if len(errors):
                    errors = "\n".join([str(x) for x in errors])
                    msg = f"Invalid platform {p} in base package {bp.name}. Consider updating the parameterisation " \
                          f"configuration to exclude invalid platforms - {errors}"
                    logger.warning(msg)
                    raise apis.models.errors.ApiError(msg)
        if sorted(valid_platforms) != sorted(known_platforms):
            raise apis.models.errors.ApiError(
                f"The experiment parameterisation involves platforms {sorted(known_platforms)} but the "
                f"base package(s) contain only these valid platforms "
                f"{sorted(valid_platforms.difference(invalid_platforms))}")


def combine_multipackage_parameterised_package(
        ve: apis.models.virtual_experiment.ParameterisedPackage,
        package_metadata_collection: apis.storage.PackageMetadataCollection,
        path_multipackage: Optional[str] = None,
) -> apis.runtime.package_derived.DerivedPackage:
    """Combines the multiple base packages of a PVEP into a single DSL

    Arguments:
        ve: The parameterised virtual experiment package
        package_metadata_collection: The collection of the package metadata
        path_multipackage: (Optional) The directory under which the derived package will be stored when using
            persistent storage

    Returns a apis.runtime.package_derived.DerivedPackage which describes the resulting unified DSL
    """
    if len(ve.base.packages) > 1 and ve.base.connections:
        derived = apis.runtime.package_derived.DerivedPackage(ve, directory_to_place_derived=path_multipackage)
        derived.synthesize(package_metadata=package_metadata_collection, platforms=ve.get_known_platforms())
        ve.update_digest()
        return derived
    elif len(ve.base.packages) != 1:
        if ve.base.connections:
            msg = "but it has base.connections"
        else:
            msg = "and does not have base.connections"
        raise apis.models.errors.ApiError(f"Virtual experiment does not contain exactly 1 base package but "
                                          f"{len(ve.base.packages)} {msg}")


def update_registry_metadata_of_parameterised_package(
        ve: apis.models.virtual_experiment.ParameterisedPackage,
        concrete: experiment.model.frontends.flowir.FlowIRConcrete,
        data_files: List[str],
):
    """Inspects a virtual experiment and updates its .metadata.registry fields to reflect information about the PVEP

    Arguments:
        ve: The parameterised virtual experiment package (PVEP). The PVEP object is updated.
        concrete: The virtual experiment definition
        data_files: A list of files under the `data` directory of the virtual experiment.
    """
    try:
        merged = apis.models.virtual_experiment.MetadataRegistry \
            .from_flowir_concrete_and_data(
            concrete=concrete,
            data_files=data_files,
            platforms=ve.parameterisation.get_available_platforms(),
            variable_names=ve.parameterisation.get_configurable_variable_names()
        )

        merged.inherit_defaults(ve.parameterisation)

        # VV: Now look at the global variables of all platforms in `concrete` and fill in any missing
        # default values (i.e. values for which there's no mention in ve.parameterisation)

        known_platforms_and_default = list(ve.get_known_platforms() or concrete.platforms)

        if experiment.model.frontends.flowir.FlowIR.LabelDefault not in known_platforms_and_default:
            known_platforms_and_default.append('default')

        default_values = concrete.get_default_global_variables()

        for p in known_platforms_and_default:
            p_vars = concrete.get_platform_global_variables(p)
            full_context = copy.deepcopy(default_values)
            full_context.update(p_vars or {})

            for key in p_vars or {}:
                try:
                    p_value = experiment.model.frontends.flowir.FlowIR.fill_in(
                        str(p_vars[key]), full_context, ignore_errors=True, label=None, is_primitive=True)
                except Exception as e:
                    logger.warning(f"Unable to expand variable {key}={p_vars[key]} due to {e} - "
                                   f"will assume that this is not a problem")
                    p_value = str(p_vars[key])

                try:
                    v = merged.executionOptionsDefaults.get_variable(key)
                except KeyError:
                    merged.executionOptionsDefaults.variables.append(
                        apis.models.virtual_experiment.VariableWithDefaultValues(name=key, valueFrom=[
                            apis.models.virtual_experiment.ValueInPlatform(value=p_value, platform=p)]))
                else:
                    if not any(filter(lambda pv: pv.platform == p, v.valueFrom)):
                        # VV: There is no information about this platform for this variable
                        v.valueFrom.append(apis.models.virtual_experiment.ValueInPlatform(value=p_value, platform=p))

        ve.metadata.registry.inputs = merged.inputs
        ve.metadata.registry.data = merged.data
        ve.metadata.registry.containerImages = merged.containerImages
        ve.metadata.registry.executionOptionsDefaults = merged.executionOptionsDefaults
        ve.metadata.registry.interface = concrete.get_interface() or {}
        ve.metadata.registry.platforms = merged.platforms
    except apis.models.errors.ApiError as e:
        logger.warning(f"Could not extract registry metadata due to {e}. "
                       f"Traceback: {traceback.format_exc()}")
        raise e from e
    except Exception as e:
        logger.warning(f"Could not extract registry metadata due to {e}. "
                       f"Traceback: {traceback.format_exc()}")
        raise apis.models.errors.ApiError(f"Unable to extract registry metadata due to unexpected error") from e


def access_and_validate_virtual_experiment_packages(
        ve: apis.models.virtual_experiment.ParameterisedPackage,
        packages: apis.storage.PackageMetadataCollection,
        path_multipackage: Optional[str] = None,
) -> apis.models.virtual_experiment.StorageMetadata:
    """Validates a Parameterised Virtual Experiment Package modifies it (e.g. add createdOn and digest)

    Notes: ::

        - If PVEP is "derived", method also stores the synthesized standalone directory on persistent storage
        - If PVEP is "derived", method also double checks that the bindings use the correct input/output types

    Arguments:
        ve:
            the parameterised virtual experiment package (PVEP)
        packages:
            The collection of the package metadata
        path_multipackage:
            (Optional - only for multi-package PVEPs) Path to store the aggregate virtual experiment
            that is the result of a Synthesis step following instructions encoded in the multi-package PVEP

    Returns:
        The metadata (concrete, datafiles, manifestData) of the virtual experiment defined by this PVEP

    Raises:
        apis.models.errors.ApiError:
            If the virtual experiment is invalid or inconsistent with the PVEP
    """
    prepare_parameterised_package_for_download_definition(ve, db_secrets=packages.db_secrets)

    for package in ve.base.packages:
        if not package.graphs:
            continue

        for graph in package.graphs:
            for x in graph.bindings.input:
                graph.bindings.ensure_input_type(x)

            for x in graph.bindings.output:
                graph.bindings.ensure_output_type(x)

    with packages:
        get_and_validate_parameterised_package(ve, packages)

        if len(ve.base.packages) == 1:
            metadata = packages.get_metadata(ve.base.packages[0].name)
        else:
            # VV: Anything that does not have exactly 1 base package is supposed to be a multi-base package
            path_multipackage = path_multipackage or apis.models.constants.ROOT_STORE_DERIVED_PACKAGES
            derived = combine_multipackage_parameterised_package(ve, packages, path_multipackage=path_multipackage)

            ve.update_digest()

            metadata = apis.runtime.package_derived.DerivedVirtualExperimentMetadata(
                concrete=derived.concrete_synthesized,
                manifestData=apis.models.virtual_experiment.manifest_from_parameterised_package(ve),
                data=derived.data_files,
                derived=derived
            )

    return metadata


def validate_parameterised_package(
        ve: apis.models.virtual_experiment.ParameterisedPackage,
        metadata: apis.models.virtual_experiment.VirtualExperimentMetadata,
):
    """Validates a Parameterised Virtual Experiment Package and modifies it (e.g. add createdOn and digest)

    Arguments:
        ve: the parameterised virtual experiment package (PVEP) - updates this in memory
        metadata: Metadata for the Virtual Experiment that Parameterised Virtual Experiment Package points to

    Returns:
        The metadata (concrete, datafiles, manifestData) of the virtual experiment defined by this PVEP

    Raises:
        apis.models.errors.ApiError:
            If the virtual experiment is invalid or inconsistent with the PVEP
    """

    update_registry_metadata_of_parameterised_package(ve=ve, concrete=metadata.concrete, data_files=metadata.data)
    try:
        logger.info(f"Discovered registry metadata: {ve.metadata.registry}")
        ve.test()
    except Exception as e:
        logger.warning(f"Run into {e} while testing new parameterised package. "
                       f"Traceback: {traceback.format_exc()}")
        raise apis.models.errors.ApiError(f"Invalid parameterised package due to {e}") from e

    try:
        ve.update_digest()

        # VV: Generate createdOn timestamp right before adding to Database
        ve.metadata.registry.createdOn = ve.metadata.registry.get_time_now_as_str()
    except Exception as e:
        logger.warning(f"Run into {e} while adding parameterised virtual experiment package to database. "
                       f"Traceback: {traceback.format_exc()}")
        raise apis.models.errors.ApiError(f"Unable to add experiment to database due to {e}")
