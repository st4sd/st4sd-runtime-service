# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import copy
import datetime
import difflib
import logging
import os.path
from collections import namedtuple
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import cast

import experiment.model.errors
import experiment.model.frontends.flowir
import experiment.model.graph
import experiment.model.storage
import pydantic
from pydantic import validator

import apis.models.common
import apis.models.constants
import apis.models.errors
import apis.models.from_core

TSourceDatasetLocation = namedtuple('TSourceDatasetLocation', ['dataset_name'])
TBaseConfig = namedtuple('TBaseConfig', ['path', 'manifestPath'])

"""Helper classes to access and manipulate definitions of virtual experiment entries"""


class BasePackageSource(apis.models.common.Digestable):
    location: None
    security: None
    version: None


class SourceGitSecurityOAuth(apis.models.common.Option):
    pass

class BasePackageSourceGitSecurity(apis.models.common.Digestable):
    oauth: Optional[SourceGitSecurityOAuth] = None


class SourceGitLocation(apis.models.common.Digestable):
    url: Optional[str] = None
    branch: Optional[str] = None
    tag: Optional[str] = None
    commit: Optional[str] = None


class BasePackageSourceGit(BasePackageSource):
    security: Optional[BasePackageSourceGitSecurity] = None
    location: SourceGitLocation
    version: Optional[str] = None

    @validator('location')
    def single_location_source(cls, value: SourceGitLocation):
        raw = value.dict(exclude_none=True)
        raw = {x: raw[x] for x in raw if x != "url"}
        if len(raw) > 1:
            raise ValueError(f"location must not contain more than 1 of branch, tag, or commit")
        return value


class DatasetInfo(apis.models.common.Digestable):
    dataset: str


class BasePackageSourceDataset(BasePackageSource):
    location: DatasetInfo
    version: Optional[str] = None
    security: Optional[DatasetInfo] = None

    @validator('security', always=True)
    def set_default_security(cls, value: DatasetInfo | None, values: Dict[str, Any]) -> DatasetInfo:
        if value is None:
            return values['location']


class DependencyImageRegistry(apis.models.common.Digestable):
    serverUrl: str
    security: Optional[apis.models.common.Option]


class BasePackageDependencies(apis.models.common.Digestable):
    imageRegistries: List[DependencyImageRegistry] = []


class BasePackageConfig(apis.models.common.Digestable):
    path: Optional[str]
    manifestPath: Optional[str]


class StorageMetadata(pydantic.BaseModel):
    class Config:
        arbitrary_types_allowed = True

    rootDirectory: Optional[str] = None
    location: Optional[str] = None
    concrete: experiment.model.frontends.flowir.FlowIRConcrete
    top_level_folders: List[str] = []
    manifestData: Dict[str, str]
    data: List[str]

    def path_offset_location(self, path: str) -> str | None:
        if self.location is None:
            return None
        if os.path.isdir(self.location):
            return os.path.join(self.location, path)
        elif os.path.isfile(self.location):
            parent = os.path.dirname(self.location)
            return os.path.join(parent, path)

    def get_path_to_application_directory(self, directory: str) -> str | None:
        if not self.location:
            return None

        directory_path = None

        if directory in self.manifestData:
            # VV: The manifest may either contain absolute paths or paths that are relative to the parent
            # of the experiment package (i.e. package.location)
            directory_path = self.manifestData[directory].rsplit(':', 1)[0]

            if os.path.isabs(directory_path) is False:
                parent_dir = os.path.dirname(self.location)
                directory_path = os.path.join(parent_dir, directory_path)
        elif self.location and os.path.isdir(self.location):
            directory_path = os.path.join(self.location, directory)

        return directory_path

    def discover_data_files(self):
        data_files = []
        data_path = self.get_path_to_application_directory('data')

        if data_path is not None and os.path.isdir(data_path):
            for name in os.listdir(data_path):
                full_path = os.path.join(data_path, name)
                if os.path.isfile(full_path):
                    data_files.append(name)
        self.data = sorted(data_files)

        return list(self.data)

    @classmethod
    def from_config(
            cls,
            config: BasePackageConfig,
            platform: str | None = None,
            prefix_paths: str | None = None,
            find_any_valid_platform: bool = False,
    ) -> StorageMetadata:
        """Loads FlowIRConcrete and discovers data files of a BasePackage

        Args:
            config: The configuration location of the base package
            platform: The platform to use for loading the base package
            prefix_paths: (optional) a path to prefix the config.path and config.manifestPath

        Returns:
            A ConcreteData which contains the FlowIRConcrete and the list of files that are immediate children of
            the `data` directory.
        """

        config = BasePackageConfig(path=config.path, manifestPath=config.manifestPath)

        if prefix_paths:
            def to_rel_path(prefix_paths: str, what_path: str | None, return_if_empty: bool = True) -> str:
                if what_path:
                    if what_path.startswith('/'):
                        what_path = what_path[1:]
                    return os.path.join(prefix_paths, what_path)
                return prefix_paths if return_if_empty else None

            config.path = to_rel_path(prefix_paths, config.path)
            config.manifestPath = to_rel_path(prefix_paths, config.manifestPath, return_if_empty=False)

        workflow_manifest = config.manifestPath
        try:
            pkg = experiment.model.storage.ExperimentPackage.packageFromLocation(
                location=config.path,
                manifest=config.manifestPath,
                platform=platform,
                validate=False,
                variable_substitute=False,
            )
        except experiment.model.errors.PackageUnknownFormatError as e:
            raise apis.models.errors.ApiError(
                f"Could not find a valid virtual experiment definition at base.config={config.dict()} please inspect "
                f"base.config and correct it") from e
        except experiment.model.errors.ExperimentMissingConfigurationError as e:
            raise apis.models.errors.ApiError(
                f"Could not find a virtual experiment definition at base.config={config.dict()} please inspect "
                f"base.config and correct it") from e
        except experiment.model.errors.ExperimentInvalidConfigurationError as e:
            raise apis.models.errors.ApiError(
                f"Invalid virtual experiment definition at base.config={config.dict()} please fix the definition of "
                f"the package, test it using etest.py, and then retry pushing it. The error was: {e}") from e

        ret = StorageMetadata(
            concrete=pkg.configuration.get_flowir_concrete(),
            data=[],
            manifestData=pkg.configuration.manifestData,
            location=config.path,
            rootDirectory=prefix_paths,
            top_level_folders=pkg.configuration.top_level_folders
        )

        ret.discover_data_files()
        return ret


class BaseSource(apis.models.common.Digestable):
    git: Optional[BasePackageSourceGit]
    dataset: Optional[BasePackageSourceDataset]


class OrchestratorResources(apis.models.common.Digestable):
    cpu: Optional[str]
    memory: Optional[str]


class ParameterisationRuntime(apis.models.common.Digestable):
    resources: OrchestratorResources = OrchestratorResources()
    args: List[str] = []


class Configuration(apis.models.common.Digestable):
    image: Optional[str] = None
    s3FetchFilesImage: Optional[str] = pydantic.Field(alias="s3-fetch-files-image")
    workflowMonitoringImage: Optional[str] = pydantic.Field(alias="workflow-monitoring-image")
    gitsecret: Optional[str] = None
    gitsecretOauth: Optional[str] = pydantic.Field(
        None,
        alias="gitsecret-oauth",
        description="Name of Secret object which contains the `oauth-token` key")
    imagePullSecrets: List[str] = []
    inputdatadir: Optional[str] = None
    workingVolume: Optional[str] = None
    defaultArguments: List[Dict[str, str]] = pydantic.Field(
        [], alias="default-arguments")


class NamespacePresets(apis.models.common.Digestable):
    runtime: ParameterisationRuntime = ParameterisationRuntime()

    @classmethod
    def from_configuration(cls, configuration) -> NamespacePresets:
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

        args = []
        for many_args in configuration.get('default-arguments', []):
            args.extend([f"{key}={many_args[key]}" for key in many_args])

        return NamespacePresets.parse_obj({'runtime': {
            'args': args
        }})

    @classmethod
    def parse_obj(cls, *args, **kwargs) -> NamespacePresets:
        return cast(NamespacePresets, super(NamespacePresets, cls).parse_obj(*args, **kwargs))


class VolumePersistentVolumeClaim(apis.models.common.Digestable):
    claimName: str
    readOnly: bool = True
    subPath: Optional[str]


class KubernetesNestedItem(apis.models.common.Digestable):
    key: str
    path: str


class VolumeConfigMap(apis.models.common.Digestable):
    name: str
    readOnly: bool = True
    items: List[KubernetesNestedItem] = []


class VolumeSecret(apis.models.common.Digestable):
    name: str
    readOnly: bool = True
    items: List[KubernetesNestedItem] = []


class VolumeDataset(apis.models.common.Digestable):
    name: str
    readOnly: bool = True
    subPath: Optional[str]


class PayloadVolumeType(apis.models.common.Digestable):
    persistentVolumeClaim: Optional[VolumePersistentVolumeClaim]
    configMap: Optional[VolumeConfigMap]
    dataset: Optional[VolumeDataset]
    secret: Optional[VolumeSecret]


class PayloadVolume(apis.models.common.Digestable):
    type: PayloadVolumeType
    applicationDependency: Optional[str]


def partition_dataset_uri(uri: str, protocol='dataset') -> Tuple[str, str]:
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


class PayloadSecurity(apis.models.common.Digestable):
    s3Input: apis.models.common.Option = apis.models.common.Option()
    s3Output: apis.models.common.Option = apis.models.common.Option()


class OldFileContent(apis.models.common.Digestable):
    filename: str
    content: Optional[str] = None


class OldVolumeType(apis.models.common.Digestable):
    persistentVolumeClaim: Optional[str] = None
    configMap: Optional[str] = None
    dataset: Optional[str] = None
    secret: Optional[str] = None


class OldVolume(apis.models.common.Digestable):
    type: OldVolumeType
    applicationDependency: str
    subPath: Optional[str]
    mountPath: Optional[str]
    readOnly: bool = True


class OldS3Credentials(apis.models.common.Digestable):
    dataset: Optional[str]
    accessKeyID: Optional[str]
    secretAccessKey: Optional[str]
    bucket: Optional[str]
    endpoint: Optional[str]
    region: Optional[str]


class OldS3Store(apis.models.common.Digestable):
    credentials: Optional[OldS3Credentials]
    bucketPath: Optional[str] = "workflow_instances/"


class PayloadExecutionRuntimePolicy(apis.models.common.Digestable):
    name: Optional[str] = None
    config: Dict[str, Any] = {}


class DeprecatedExperimentStartPayload(apis.models.common.Digestable):
    inputs: List[OldFileContent] = []
    data: List[OldFileContent] = []
    volumes: List[OldVolume] = []
    variables: Dict[str, Any] = {}
    additionalOptions: List[str] = []
    environmentVariables: Dict[str, Any] = {}
    orchestrator_resources: OrchestratorResources = OrchestratorResources(cpu="1", memory="500Mi")
    metadata: Dict[str, Any] = {}
    s3: Optional[OldS3Credentials]
    s3Store: Optional[OldS3Store]
    datasetStoreURI: Optional[str]
    platform: Optional[str]
    runtimePolicy: Optional[PayloadExecutionRuntimePolicy] = None

    @validator('metadata', 'variables', 'environmentVariables')
    def ensure_strings(cls, value: Dict[str, Any]):
        value = value or {}

        return {
            str(x): str(value[x]) for x in value
        }

    @classmethod
    def parse_obj(cls, *args, **kwargs) -> DeprecatedExperimentStartPayload:
        payload = kwargs.copy()
        #  VV: Rewrites the dlf:// URI of dlfStoreURI into a dataset:// URI
        try:
            dlfstoreuri: str = payload['dlfStoreURI']
            del payload['dlfStoreURI']
            if dlfstoreuri.startswith("dlf://"):
                dlfstoreuri = f"dataset://{dlfstoreuri[6:]}"
            payload['datasetStoreURI'] = dlfstoreuri
        except KeyError:
            pass

        return cast(DeprecatedExperimentStartPayload, super(DeprecatedExperimentStartPayload, cls)
                    .parse_obj(*args, **payload))


class PayloadExecutionOptions(apis.models.common.Digestable):
    platform: Optional[str]
    security: PayloadSecurity = PayloadSecurity()
    volumes: List[PayloadVolume] = []
    s3Output: Optional[apis.models.common.Option] = apis.models.common.Option()
    environmentVariables: List[apis.models.common.Option] = []
    inputs: List[apis.models.common.Option] = []
    data: List[apis.models.common.Option] = []
    variables: List[apis.models.common.Option] = []
    runtime: ParameterisationRuntime = ParameterisationRuntime()
    userMetadata: List[apis.models.common.Option] = []
    runtimePolicy: Optional[PayloadExecutionRuntimePolicy] = None

    @validator('userMetadata', each_item=True)
    def no_colon_in_name_value(cls, value: apis.models.common.Option):
        if value.value is None or value.valueFrom is not None:
            raise ValueError(f"The value of userMetadata {value.name} must be a constant (i.e. field .value)")

        if ':' in value.name:
            raise ValueError(f"The name of userMetadata {value.name} contains illegal ':'")

        if ':' in value.value:
            raise ValueError(f"The value of userMetadata {value.name} (\"{value.value}\") contains illegal ':'")

        return value

    @classmethod
    def from_old_payload(cls, old: DeprecatedExperimentStartPayload) -> PayloadExecutionOptions:
        config = PayloadExecutionOptions()
        config.runtimePolicy = old.runtimePolicy
        config.platform = old.platform
        config.runtime.resources = old.orchestrator_resources
        config.runtime.args = old.additionalOptions
        config.userMetadata = [
            apis.models.common.Option(name=name, value=value) for name, value in old.metadata.items()
        ]

        old_env_vars = old.environmentVariables
        config.environmentVariables = [
            apis.models.common.Option(name=name, value=old_env_vars[name]) for name in old_env_vars
        ]

        vars = old.variables
        config.variables = [
            apis.models.common.Option(name=name, value=vars[name]) for name in vars
        ]

        old_volumes = old.volumes
        new_volumes = []
        for ov in old_volumes:
            volume = ov.dict(exclude_none=True)
            new_vol = {}
            if 'applicationDependency' in volume:
                new_vol['applicationDependency'] = volume['applicationDependency']

            for what in ['persistentVolumeClaim', 'configMap', 'dataset', 'secret']:
                if volume['type'].get(what):
                    new_vol['type'] = {
                        what: {
                            x: volume[x] for x in volume if x not in ('type', 'applicationDependency')
                        }
                    }
                    if what == 'persistentVolumeClaim':
                        new_vol['type'][what]['claimName'] = volume['type'][what]
                    else:
                        new_vol['type'][what]['name'] = volume['type'][what]

            new_volumes.append(PayloadVolume.parse_obj(new_vol))
        config.volumes = new_volumes

        if old.s3:
            if old.s3.dataset:
                config.security.s3Input.valueFrom = apis.models.common.OptionValueFrom(
                    datasetRef=apis.models.common.OptionFromDatasetRef(name=old.s3.dataset)
                )
            else:
                config.security.s3Input.valueFrom = apis.models.common.OptionValueFrom(
                    s3Ref=apis.models.common.OptionFromS3Values.parse_obj(old.s3.dict())
                )

        if old.s3Store:
            creds = old.s3Store.credentials.dict(exclude_none=True)
            config.security.s3Output.valueFrom = apis.models.common.OptionValueFrom(
                # VV: We store JUST the credentials here, the bucketPath goes to config.s3Output.valueFrom.s3Ref.path
                s3Ref=apis.models.common.OptionFromS3Values.parse_obj({x: creds[x] for x in creds if x != 'path'})
            )
            config.s3Output = apis.models.common.Option(valueFrom=apis.models.common.OptionValueFrom(
                s3Ref=apis.models.common.OptionFromS3Values(path=old.s3Store.bucketPath))
            )

        if old.datasetStoreURI:
            name, path = partition_dataset_uri(old.datasetStoreURI, 'dataset')

            config.security.s3Output.valueFrom = apis.models.common.OptionValueFrom(
                datasetRef=apis.models.common.OptionFromDatasetRef(name=name, path=path)
            )

        def parse_old_file_contents(fc: OldFileContent) -> apis.models.common.Option:
            if fc.content:
                # VV: This is an "embedded" file - it contains the contents of the file
                ret = apis.models.common.Option(name=fc.filename, value=fc.content)
            else:
                # VV: This file is retrieved from old.s3 (either dataset, or S3) - we will reuse the credentials from
                # config.security - here we just record path to the files to download
                ret = apis.models.common.Option(
                    name=os.path.basename(fc.filename),
                    valueFrom=apis.models.common.OptionValueFrom())
                if config.security.s3Input.valueFrom.s3Ref:
                    ret.valueFrom.s3Ref = apis.models.common.OptionFromS3Values(path=fc.filename)
                else:
                    ret.valueFrom.datasetRef = apis.models.common.OptionFromDatasetRef(path=fc.filename)
            return ret

        config.inputs = [parse_old_file_contents(x) for x in old.inputs]
        config.data = [parse_old_file_contents(x) for x in old.data]

        return config

    def configure_output_s3(
            self,
            path: str,
            s3_security: apis.models.common.OptionFromS3Values):
        s3_ref = apis.models.common.OptionFromS3Values(path=path)
        self.s3Output.valueFrom = apis.models.common.OptionValueFrom(s3Ref=s3_ref)
        # VV: now set the security configuration - this goes in a different place
        self.security.s3Output.valueFrom = apis.models.common.OptionValueFrom(s3Ref=s3_security)

    @classmethod
    def parse_obj(cls, *args, **kwargs) -> PayloadExecutionOptions:
        return cast(PayloadExecutionOptions, super(PayloadExecutionOptions, cls).parse_obj(*args, **kwargs))


class ParameterisationPresets(apis.models.common.Digestable):
    variables: List[apis.models.common.Option] = []
    runtime: ParameterisationRuntime = ParameterisationRuntime()
    data: List[apis.models.common.Option] = []
    environmentVariables: List[apis.models.common.Option] = []
    platform: Optional[str]

    def get_variable(self, name: str) -> apis.models.common.Option:
        for v in self.variables:
            if name == v.name:
                return v
        raise KeyError(f"Unknown variable", name)


class ParameterisationExecutionOptions(apis.models.common.Digestable):
    variables: List[apis.models.common.OptionMany] = []
    data: List[apis.models.common.OptionMany] = []
    runtime: ParameterisationRuntime = ParameterisationRuntime()
    platform: List[str] = []

    def get_variable(self, name: str) -> apis.models.common.OptionMany:
        for v in self.variables:
            if name == v.name:
                return v
        raise KeyError(f"Unknown variable", name)


class MetadataPackage(apis.models.common.Digestable):
    name: Optional[str]
    tags: Optional[List[str]] = []
    keywords: List[str] = []
    license: Optional[str]
    maintainer: Optional[str]
    description: Optional[str]

    @validator('name')
    def name_must_be_valid_k8s_object_name(cls, name):
        valid_k8s = apis.models.common.valid_k8s_label(
            'name',
            name,
            pattern_value=apis.models.common.K8S_PATTERN_OBJECT_NAME)

        if valid_k8s is False or len(name) > (64 - (1 + 6)):
            # VV: We use the suffix `-XXXXXX` to make workflow ids unique
            rules = ("Valid names must match the regular expression "
                     "\"[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*\" and be less than %d "
                     "characters long" % (64 - (1 + 6)))
            raise ValueError(f"Invalid metadata.package.name {name}. {rules}")

        return name


class ValueInPlatform(apis.models.common.Digestable):
    value: str
    platform: Optional[str] = None


class VariableWithDefaultValues(apis.models.common.Digestable):
    name: str
    valueFrom: List[ValueInPlatform] = pydantic.Field(
        [], description="One entry per platform that configures the value of this variable. If platform is none "
                        "then the default value is the default specified in executionOptions")

    def get_platform_value(self, platform: str | None) -> str:
        for p in self.valueFrom:
            if p.platform == platform:
                return p.value
        raise KeyError("No platform", platform)


class ExecutionOptionDefaults(apis.models.common.Digestable):
    variables: List[VariableWithDefaultValues] = pydantic.Field(
        [], description="One entry per variable that users can override with their payload to experiments/$id/start")

    def get_variable(self, name: str) -> VariableWithDefaultValues:
        for v in self.variables:
            if name == v.name:
                return v
        raise KeyError(f"Unknown variable", name)


class MetadataRegistry(apis.models.common.Digestable):
    createdOn: Optional[str] = None
    digest: Optional[str] = None
    tags: Optional[List[str]] = []
    timesExecuted: int = 0
    interface: Dict[str, Any] = {}
    inputs: Optional[List[apis.models.common.Option]] = []
    data: Optional[List[apis.models.common.Option]] = []
    containerImages: Optional[List[apis.models.common.Option]] = []
    executionOptionsDefaults: ExecutionOptionDefaults = ExecutionOptionDefaults()

    @classmethod
    def get_time_now_as_str(self) -> str:
        now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
        return now.strftime(apis.models.constants.TIME_FORMAT)

    def get_data_names(self) -> List[str]:
        return [x.name for x in self.data]

    @validator('createdOn', always=True)
    def set_default_value(cls, value: str | None) -> str:
        if value is None:
            return cls.get_time_now_as_str()
        return value

    @validator('inputs', 'data', 'containerImages', always=True, each_item=True)
    def must_only_contain_name(cls, value: apis.models.common.Option) -> apis.models.common.Option:
        raw = value.dict()
        if list(raw) != ["name"]:
            raise ValueError("Must contain just the key \"name\"")
        return value

    def inherit_defaults(self, parameterisation: Parameterisation):
        """Extracts defaults from parameterisation and updates current executionOptionsDefaults

        The method updates @self

        Args:
            parameterisation: The parameterisation options
        """
        platforms = parameterisation.get_available_platforms()
        if not platforms:
            platforms = ['default']

        for v in parameterisation.executionOptions.variables:
            if v.value:
                value = v.value
            elif v.valueFrom:
                value = v.valueFrom[0].value
            else:
                continue

            try:
                cur_var = self.executionOptionsDefaults.get_variable(v.name)
            except KeyError:
                cur_var = VariableWithDefaultValues(name=v.name)
                self.executionOptionsDefaults.variables.append(cur_var)

            cur_var.valueFrom = [ValueInPlatform(value=value, platform=p) for p in platforms]

        for v in parameterisation.presets.variables:
            try:
                cur_var = self.executionOptionsDefaults.get_variable(v.name)
            except KeyError:
                cur_var = VariableWithDefaultValues(name=v.name)
                self.executionOptionsDefaults.variables.append(cur_var)

            cur_var.valueFrom = [ValueInPlatform(value=v.value, platform=p) for p in platforms]

    @classmethod
    def from_flowir_concrete_and_data(
            cls,
            concrete: experiment.model.frontends.flowir.FlowIRConcrete,
            data_files: List[str],
            platforms: List[str] | None,
            variable_names: List[str],
    ) -> MetadataRegistry:
        """Extracts useful information from a FlowIRConcrete object

        Args:
            concrete: A FlowIRConcrete object
            data_files: List of filenames in the root of the data directory
            platforms: Which platforms to parse. If @platforms is None, it defaults to all platforms

        Returns:
            A MetadataRegistry instance

        Raises:
            apis.models.errors.CannotMergeMetadataRegistryError: if the platforms disagree on inputs or data
        """
        if platforms is None:
            platforms = concrete.platforms

        merged: Optional[MetadataRegistry] = None

        log = logging.getLogger("FlowIR")

        default_platform_vars = concrete.get_platform_global_variables('default')

        for platform in platforms:
            concrete.configure_platform(platform)
            comp_ids = concrete.get_component_identifiers(recompute=True, include_documents=True)

            inputs: Set[str] = set()
            container_images: Set[str] = set()

            for comp_id in comp_ids:
                try:

                    raw_conf = concrete.get_component_configuration(
                        comp_id, raw=True, is_primitive=True, include_default=True, inject_missing_fields=True,
                    )
                    references = raw_conf.get('references', [])
                    backend = raw_conf['resourceManager']['config']['backend']
                    image = raw_conf['resourceManager']['kubernetes']['image']
                    lsf_image = raw_conf['resourceManager']['lsf']['dockerImage']
                    try:
                        references = experiment.model.frontends.flowir.FlowIR.fill_in(
                            references, raw_conf['variables'], flowir=None, label="references", is_primitive=True)
                    except Exception as e:
                        log.info(f"Platform {platform} is missing variables - cannot resolve references due to {e}")

                    try:
                        backend = experiment.model.frontends.flowir.FlowIR.fill_in(
                            backend, raw_conf['variables'], flowir=None, label="backend", is_primitive=True)
                    except Exception as e:
                        log.info(f"Platform {platform} is missing variables - cannot resolve backend due to {e}")

                    try:
                        image = experiment.model.frontends.flowir.FlowIR.fill_in(
                            image, raw_conf['variables'], flowir=None, label="k8s_image", is_primitive=True)
                    except Exception as e:
                        image = ''
                        log.info(f"Platform {platform} is missing variables - cannot resolve Kubernetes image "
                                 f"due to {e}")

                    try:
                        lsf_image = experiment.model.frontends.flowir.FlowIR.fill_in(
                            lsf_image, raw_conf['variables'], flowir=None, label="lsf_image", is_primitive=True)
                    except Exception as e:
                        lsf_image = ''
                        log.info(f"Platform {platform} is missing variables - cannot resolve LSF image due to {e}")

                    for ref in references:
                        dref = apis.models.from_core.DataReference(ref)

                        if dref.externalProducerName == "input":
                            inputs.add(dref.pathRef)

                    if backend == 'kubernetes':
                        if image:
                            container_images.add(image)
                    elif backend == 'lsf':
                        if lsf_image:
                            container_images.add(lsf_image)
                except Exception as e:
                    raise experiment.model.errors.EnhancedException(
                        f"Platform {platform} contains invalid component {comp_id}", e) from e

            var_defaults = []
            platform_vars = concrete.get_platform_global_variables(platform, return_copy=True)
            for v in variable_names:
                try:
                    value = platform_vars[v]
                except KeyError:
                    try:
                        value = default_platform_vars[v]
                    except KeyError:
                        raise apis.models.errors.UnknownVariableError(v, platform)

                var_defaults.append(VariableWithDefaultValues(
                    name=v, valueFrom=[ValueInPlatform(value=value, platform=platform)]))

            current = cls(
                inputs=[apis.models.common.Option(name=filename) for filename in inputs],
                containerImages=[apis.models.common.Option(name=name) for name in container_images],
                executionOptionsDefaults=ExecutionOptionDefaults(variables=var_defaults)
            )

            if merged is None:
                merged = current
            else:
                try:
                    merged = cls.merge(merged, current)
                except apis.models.errors.CannotMergeMetadataRegistryError as e:
                    raise apis.models.errors.InconsistentPlatformError(platform, str(e), e) from e
                except Exception as e:
                    raise apis.models.errors.InconsistentPlatformError(platform, f"Unexpected {type(e)}: {e}", e) from e

        merged.data = [apis.models.common.Option(name=filename) for filename in data_files]
        return merged

    @classmethod
    def merge(cls, *many: MetadataRegistry) -> MetadataRegistry:
        """Merges (some) of the information in multiple MetadataRegistries.

        Currently, this method only merges:
            - inputs (all must agree)
            - data  (all must agree)
            - containerImages (union operation)
            - executionOptionDefaults.variables (union operation)

        Moreover, it:
            - does not explicitly set any other field (i.e. it populates remaining fields with defaults of Class)

        Args:
            many: Multiple metadata registries

        Raises:
            apis.models.errors.CannotMergeMetadataRegistryError: if inputs or data contain different entries
        """

        # VV: Currently, all three are List[apis.models.common.Option] with just .name - we must ensure that all in
        # @many agree on inputs and data. However, we just want to aggregate all containerImages
        inputs: Optional[Set[str]] = None
        data: Optional[Set[str]] = None
        containerImages: Set[str] = set()
        execution_option_defaults_variables: Dict[
            str, apis.models.virtual_experiment.VariableWithDefaultValues] = {}

        for one in many:
            if inputs is None:
                inputs = {x.name for x in one.inputs}
            else:
                one_inputs = {x.name for x in one.inputs}
                if inputs != one_inputs:
                    raise apis.models.errors.CannotMergeMetadataRegistryError(
                        'inputs', one.inputs, "Inputs disagree with other MetadataRegistry objects", one)

            if data is None:
                data = {x.name for x in one.data}
            else:
                one_data = {x.name for x in one.data}
                if data != one_data:
                    raise apis.models.errors.CannotMergeMetadataRegistryError(
                        'data', one.data, "Data disagrees with other MetadataRegistry objects", one)

            containerImages.update({x.name for x in one.containerImages})

            for v in one.executionOptionsDefaults.variables:
                if v.name not in execution_option_defaults_variables:
                    execution_option_defaults_variables[v.name] = v
                    continue
                old = execution_option_defaults_variables[v.name]

                for mine in v.valueFrom:
                    for p in old.valueFrom:
                        if p.platform == mine.platform:
                            if p.value != mine.value:
                                raise apis.models.errors.CannotMergeMetadataRegistryError(
                                    'executionOptionDefaultss.variables',
                                    one.executionOptionsDefaults.variables,
                                    f"Variable {v.name} disagrees with value of platform {mine.platform} of other "
                                    f"MetadataRegistry objects ", one)
                            break
                    else:
                        # VV: there is no such platform in old, inject it
                        old.valueFrom.append(mine)

        return MetadataRegistry(
            inputs=[apis.models.common.Option(name=name) for name in (inputs or {})],
            data=[apis.models.common.Option(name=name) for name in (data or {})],
            containerImages=[apis.models.common.Option(name=name) for name in containerImages],
            executionOptionsDefaults=apis.models.virtual_experiment.ExecutionOptionDefaults(
                variables=list(execution_option_defaults_variables.values()))
        )


class Metadata(apis.models.common.Digestable):
    package: MetadataPackage = MetadataPackage()
    registry: MetadataRegistry = MetadataRegistry()

    def get_unique_identifier_str(self) -> str:
        return apis.models.common.PackageIdentifier.from_parts(
            self.package.name, tag=None, digest=self.registry.digest).identifier

class Parameterisation(apis.models.common.Digestable):
    presets: ParameterisationPresets = ParameterisationPresets()
    executionOptions: ParameterisationExecutionOptions = ParameterisationExecutionOptions()

    def get_available_platforms(self) -> List[str] | None:
        """Returns the available platforms for executing this virtual experiment

        Returns:
            A list of platform names that the virtual experiment can run as. Returns None
            if there are no parameterisation options regarding the platform name.
        """
        if self.presets.platform:
            return [self.presets.platform]
        if self.executionOptions.platform:
            return self.executionOptions.platform

    def get_configurable_variable_names(self) -> List[str]:
        """Returns the names of variables that users can set the value of"""
        return [x.name for x in self.executionOptions.variables]


class GraphBinding(apis.models.common.Digestable):
    name: str = pydantic.Field(
        None, description="Name in the scope of this collection of bindings, "
                          "must not contain string !!! or \\n. "
                          "If None then reference and optionally stages must be provided")
    reference: Optional[str] = pydantic.Field(None, description="A FlowIR reference to associate with binding")
    type: Optional[str] = pydantic.Field(
        None, description="Valid types are input and output, if left None and binding belongs to a collection "
                          "the type field receives the approriate default value")
    stages: Optional[List[str]] = pydantic.Field(
        None, description="If reference points to multiple components which have the same name "
                          "but belong to multiple stages")

    @validator('name')
    def check_name(cls, value: str):
        if value is None:
            return

        if '!!!' in value:
            raise ValueError("Name cannot contain string \"!!!\"")

        if '\n' in value:
            raise ValueError("Name cannot contain new line characters (\\n)")
        return value

    @validator('reference')
    def must_be_valid_reference(cls, value: str | None):
        if value is not None:
            apis.models.from_core.DataReference(value)
            return value


class GraphBindingCollection(apis.models.common.Digestable):
    input: List[GraphBinding] = []
    output: List[GraphBinding] = []

    def get_input_binding(self, name: str) -> GraphBinding:
        for x in self.input:
            if x.name == name:
                return x
        raise KeyError(f"Unknown input binding {name} - known bindings are {[x.name for x in self.input]}")

    def get_output_binding(self, name: str) -> GraphBinding:
        for x in self.output:
            if x.name == name:
                return x
        raise KeyError(f"Unknown output binding {name} - known bindings are {[x.name for x in self.output]}")

    @classmethod
    def ensure_correct_type(cls, value: GraphBinding, correct_type: str):
        if value.type is None:
            value.type = correct_type
        if value.type != correct_type:
            raise ValueError(f"Binding {value.name} must have type \"{correct_type}\", "
                             f"instead it has \"{value.type}\"")
        return value

    @validator('input', each_item=True)
    def ensure_input_type(cls, value: GraphBinding):
        return cls.ensure_correct_type(value, "input")

    @validator('output', each_item=True)
    def ensure_output_type(cls, value: GraphBinding):
        return cls.ensure_correct_type(value, "output")


class BasePackageGraphNodeUseBinding(apis.models.common.Digestable):
    source: BasePackageGraphBindingSource = pydantic.Field(
        ..., description="Identifies the source of which the value will replace the reference")
    reference: str = pydantic.Field(
        ..., description="An entry in the component's references. "
                         "The reference will be rewritten to point to the source")


class BasePackageGraphNode(apis.models.common.Digestable):
    reference: str = pydantic.Field(
        ..., description="An absolute FlowIR reference string of an un-replicated component, e.g. stage0.simulation")

    @validator('reference')
    def check_reference(cls, reference: str):
        dref = experiment.model.graph.ComponentIdentifier(reference)
        if dref.stageIndex is None:
            raise ValueError(f"Node reference {reference} is not absolute - missing stageIndex")

        if '/' in dref.namespace:
            raise ValueError(f"Node reference {reference} does not point to a node")

        return reference


class BasePackageGraph(apis.models.common.Digestable):
    name: str
    bindings: GraphBindingCollection = GraphBindingCollection()
    nodes: List[BasePackageGraphNode] = []

    def partition_name(self) -> Tuple[str, str]:
        """Returns (${package.Name}, ${graph.Name})"""
        try:
            package_name, graph_name = self.name.split('/')
        except ValueError:
            raise ValueError(f"Graph name \"{self.name}\" must be in the format ${{package.Name}}/${{graph.Name}}")
        return package_name, graph_name


class BasePackage(apis.models.common.Digestable):
    name: str = "main"
    source: BaseSource = BaseSource()
    dependencies: BasePackageDependencies = BasePackageDependencies()
    config: BasePackageConfig = BasePackageConfig()
    graphs: List[BasePackageGraph] = []

    def get_graph(self, name: str) -> BasePackageGraph:
        for x in self.graphs:
            if x.name == name:
                return x
        raise KeyError(f"Unknown graph {name} in base package {self.name}")


class BindingOptionValueFromGraph(apis.models.common.Digestable):
    name: str = pydantic.Field(..., description="Name of the graph, format is ${package.Name}/${graph.Name}}")
    binding: GraphBinding = pydantic.Field(
        ..., description="The source binding of which to use the value. It must be of type \"output\"")

    def partition_name(self) -> Tuple[str, str]:
        """Returns (${package.Name}, ${graph.Name})"""
        try:
            package_name, graph_name = self.name.split('/')
        except ValueError:
            raise ValueError("Graph name must be in the format ${package.Name}/${graph.Name}")
        return package_name, graph_name

    @validator('binding')
    def check_source_binding_name(cls, binding: GraphBinding):
        if binding.name is None:
            if binding.reference is None:
                raise ValueError("Binding must have a name OR a reference (and optionally stages)")
        return binding

    @validator('binding')
    def check_source_binding_type(cls, binding: GraphBinding):
        if binding.type is None:
            binding.type = "output"

        if binding.type != "output":
            raise ValueError("Must be output binding")
        return binding

    @validator('name')
    def check_source_graph_name(cls, name: str):
        if not name:
            raise ValueError("Missing a name")
        try:
            package_name, graph_name = name.split('/')
        except ValueError:
            raise ValueError("Graph name bust be in the format ${package.Name}/${graph.Name}")

        if not graph_name or not package_name:
            if not graph_name:
                what = "${graph.Name}"
            else:
                what = "${package.Name}"

            raise ValueError(f"Graph adheres to format ${{package.Name}}/${{graph.Name}}, but {what} is empty")
        return name


class BindingOptionValueFromApplicationDependency(apis.models.common.Digestable):
    reference: str = pydantic.Field(..., description="Reference to application dependency in the derived package")

    @validator('reference')
    def must_be_valid_reference(cls, value: str):
        apis.models.from_core.DataReference(value)
        return value


class BindingOptionValueFrom(apis.models.common.Digestable):
    graph: Optional[BindingOptionValueFromGraph] = None
    applicationDependency: Optional[BindingOptionValueFromApplicationDependency] = None


class BindingOption(apis.models.common.Digestable):
    name: str = pydantic.Field(..., description="The symbolic name")
    valueFrom: BindingOptionValueFrom = pydantic.Field(
        ..., description="The source of the value to map the symbolic name to")


class BasePackageGraphInstance(apis.models.common.Digestable):
    graph: BasePackageGraph = pydantic.Field(
        ..., description="The graph to instantiate, its name must be ${basePackage.name}/${graph.name}")
    bindings: List[BindingOption] = []

    @validator('graph')
    def check_just_graph_name(cls, value: BasePackageGraph):
        if len(value.bindings.input) != 0 or len(value.bindings.output) != 0 or not value.name:
            raise ValueError("Must only contain the name field")
        return value


class PathInsidePackage(apis.models.common.Digestable):
    packageName: Optional[str] = pydantic.Field(
        None, description="Package Name")
    path: Optional[str] = pydantic.Field(
        None, description="Relative path to location of package")


class IncludePath(apis.models.common.Digestable):
    source: PathInsidePackage = pydantic.Field(..., description="Source of path")
    dest: Optional[PathInsidePackage] = pydantic.Field(
        None, description="Destination of path, defaults to just \"path: source.path\"")

    @validator('dest', always=True)
    def set_default_dest(cls, value: Optional[PathInsidePackage], values: Dict[str, Any]):
        if value is None:
            source: PathInsidePackage = values['source']
            return PathInsidePackage(path=source.path)
        return value


class VirtualExperimentBase(apis.models.common.Digestable):
    packages: List[BasePackage] = []
    connections: List[BasePackageGraphInstance] = pydantic.Field(
        [], description="Instructions to connect together graphs of base packages")
    includePaths: List[IncludePath] = pydantic.Field(
        [], description="Files inside")
    output: List[BindingOption] = pydantic.Field(
        [], description="Outputs of derived package which point to output bindings "
                        "of graphs extracted from base packages")
    interface: Optional[apis.models.from_core.FlowIRInterface] = pydantic.Field(
        None, description="Instructions to build an interface for derived package. The interface may use "
                          "derived outputs")

    def get_package(self, name: str) -> BasePackage:
        for x in self.packages:
            if x.name == name:
                return x
        raise KeyError(f"Unknown package {name}")

    @validator('packages')
    def unique_names(cls, value: List[BasePackage]):
        if len(value) == 0:
            raise ValueError("There must be at least 1 base package")

        names = set()
        for pkg in names:
            if pkg.name in names:
                raise ValueError(f"Multiple definitions of base package {pkg.name}")
            names.add(pkg.name)

        return value

    @validator('output')
    def check_outputs(cls, value: List[BindingOption], values: Dict[str, Any]):
        names = set()

        known_input_bindings: Dict[str, Dict[str, List[str]]] = {}
        known_output_bindings: Dict[str, Dict[str, List[str]]] = {}

        packages: List[BasePackage] = values['packages']

        for pkg in packages:
            known_output_bindings[pkg.name] = {}
            known_input_bindings[pkg.name] = {}

            for graph in pkg.graphs:
                known_input_bindings[pkg.name][graph.name] = {x.name: x for x in graph.bindings.input}
                known_output_bindings[pkg.name][graph.name] = {x.name: x for x in graph.bindings.output}

        for idx, bo in enumerate(value):
            if not bo.name:
                raise ValueError(f"Output entry {idx} does not have a name")
            if bo.name in names:
                raise ValueError(f"Multiple definitions of output {bo.name}")
            names.add(bo.name)

            if bo.valueFrom.graph is None:
                raise ValueError(f"Output {bo.name} must have valueFrom.graph")
            if len(bo.valueFrom.dict(exclude_none=True)) != 1:
                raise ValueError(f"Output {bo.name} must only have valueFrom.graph")
            if not (bo.valueFrom.graph.binding.name or bo.valueFrom.graph.binding.reference):
                raise ValueError(f"Output {bo.name} does not have valueFrom.graph.binding.[name or reference]")
            if bo.valueFrom.graph.name is None:
                raise ValueError(f"Output {bo.name} does not reference a graph")
            pkg_name, graph_name = bo.valueFrom.graph.partition_name()
            if pkg_name not in known_output_bindings:
                raise ValueError(f"Unknown package name {pkg_name}")
            if graph_name not in known_output_bindings[pkg_name]:
                raise ValueError(f"Unknown graph name {graph_name} for {pkg_name}")
            if (bo.valueFrom.graph.binding.name and (
                    bo.valueFrom.graph.binding.name not in known_output_bindings[pkg_name][graph_name])):
                raise ValueError(f"Unknown input binding {bo.valueFrom.graph.binding.name} for "
                                 f"{bo.valueFrom.graph.name}")

        return value


class ParameterisedPackage(apis.models.common.Digestable):
    base: VirtualExperimentBase = VirtualExperimentBase()
    metadata: Metadata = Metadata()
    parameterisation: Parameterisation = Parameterisation()

    def get_known_platforms(self) -> List[str] | None:
        if self.parameterisation.presets.platform:
            return [self.parameterisation.presets.platform]
        if self.parameterisation.executionOptions.platform:
            return self.parameterisation.executionOptions.platform
        return None

    @validator('base')
    def unique_base_identifiers(cls, value: VirtualExperimentBase):
        names = set()
        for base in value.packages:
            if base.name in names:
                raise ValueError(f"Multiple definitions of base package {base.name}")
            names.add(base.name)
        return value

    @property
    def registry_created_on(self) -> datetime.datetime:
        dt = datetime.datetime.strptime(self.metadata.registry.createdOn, apis.models.constants.TIME_FORMAT)
        return dt.replace(tzinfo=datetime.timezone.utc)

    def to_digestable(self) -> apis.models.common.Digestable:
        # VV: We shouldn't add the package name in here, or the tag. This will allow us to check if multiple
        # "virtual experiment entries" are actually the same one but with different names.
        # The same is true for "maintainer" - there's a chance 2 people upload the exact same experiment
        return apis.models.common.DigestableBase(
            base=self.base,
            parameterisation_presets=self.parameterisation.presets,
            parameterisation_executionOptions=self.parameterisation.executionOptions,
        )

    def update_digest(self):
        """Generates a digest of the virtual experiment entry"""
        self.metadata.registry.digest = self.to_digestable().to_digest()

    @classmethod
    def parse_obj(cls, *args, **kwargs) -> ParameterisedPackage:
        return cast(ParameterisedPackage, super(ParameterisedPackage, cls).parse_obj(*args, **kwargs))

    def test(self):
        """Tests whether the contents of the parameterised package make sense"""
        data_names = self.metadata.registry.get_data_names()

        for i, d in enumerate(self.parameterisation.executionOptions.data):
            if d.name not in data_names:
                msg = (f"The data file parameterisation.executionOptions.data[{i}].name={d.name} is not part of "
                       f"metadata.registry.data")
                possibilities = difflib.get_close_matches(d.name, data_names)
                if len(possibilities):
                    msg += f" - did you mean {possibilities[0]}"

                raise ValueError(msg)


class ParameterisedPackageDropUnknown(ParameterisedPackage):
    @classmethod
    def parse_obj(cls, obj, *args, **kwargs) -> ParameterisedPackageDropUnknown:
        # VV: Get rid of all "value_error.extra" errors by REMOVING the offending fields

        try:
            return cast(ParameterisedPackageDropUnknown, super(ParameterisedPackageDropUnknown, cls) \
                        .parse_obj(obj, *args, **kwargs))
        except pydantic.error_wrappers.ValidationError as e:
            obj = copy.deepcopy(obj)
            logging.getLogger().info(f"This VirtualExperiment contains errors {e.errors()} - will delete uknown fields "
                                     f"and try again")
            for err in e.errors():
                if err['type'] == 'value_error.extra':
                    what = obj

                    for x in err['loc'][:-1]:
                        what = what[x]

                    del what[err['loc'][-1]]

            return cast(ParameterisedPackageDropUnknown, super(ParameterisedPackageDropUnknown, cls) \
                        .parse_obj(obj, *args, **kwargs))
