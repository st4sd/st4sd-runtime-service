# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


"""
File contains all models, and `Namespace` instances, of all APIs in this flask application
"""
from flask_restx import Namespace, fields

############################ URL Map ############################

api_url_map = Namespace(
    "url-map", description="Operations to interact with workflow software stack URLs"
)

############################ Image Pull Secrets ############################

api_image_pull_secret = Namespace(
    "image-pull-secrets", description="Operations to view/create/edit imagePullSecrets"
)

model_get_specific = api_image_pull_secret.model(
    "imagepullsecret-identifier", {"id": fields.String(required=True)}
)

model_imagePullSecret_full = api_image_pull_secret.model(
    "imagepullsecret",
    {
        "server": fields.String(
            description="docker registry, e.g. res-drl-hpc-docker-local.artifactory.swg-devops.com",
            required=True,
            example="url-docker-registry",
        ),
        "username": fields.String(
            description="username to use when authenticating to docker registry",
            required=True,
        ),
        "password": fields.String(
            password="password to use when authenticating to docker registry",
            required=True,
        ),
    },
)

model_put = api_image_pull_secret.model(
    "update_image_pull_secret", model_imagePullSecret_full
)

# VV: Cannot create models out of Lists (flask-restx models must be Dictionaries)
example_imagePullSecret_get = fields.List(
    fields.String(
        description="docker registry, e.g. res-drl-hpc-docker-local.artifactory.swg-devops.com",
        example="url-docker-registry",
    ),
    description="GET model for imagePullSecret with a given id",
    example=["url-docker-registry1", "url-docker-registry2"],
)

example_imagePullSecret_get_all = fields.Raw(
    {},
    example={
        "k8s-secret-name": ["url-docker-registry1", "url-docker-registry2"],
    },
)

############################ Authorisation ############################

api_authorisation = Namespace(
    "authorisation", description="Operations to interact with authorization token(s)"
)

############################ Datasets ############################

api_datasets = Namespace("datasets", description="Datashim related operations")

s3_model = api_datasets.model(
    "dataset-s3-configuration",
    {
        "accessKeyID": fields.String(
            required=True, description="Access key id", default=""
        ),
        "secretAccessKey": fields.String(
            required=True, description="Secret access key", default=""
        ),
        "bucket": fields.String(
            required=True, description="Name of bucket", default=""
        ),
        "endpoint": fields.String(
            required=True,
            default="",
            description="Endpoint URL (e.g. https://s3.eu-gb.cloud-object-storage.appdomain.cloud)",
        ),
        "region": fields.String(
            required=False, description="Region (optional)", default=""
        ),
    },
)

############################ Experiments ############################

api_experiments = Namespace("experiments", description="Experiments related operations")

mOptionValueFromSecretKeyRef = api_experiments.model(
    "option-valuefrom-secretkeyref",
    {"key": fields.String(required=False), "name": fields.String()},
)


mOptionValueFromS3SecretKeyRef = api_experiments.model(
    "option-valuefrom-s3secretkeyref",
    {
        "keyAccessKeyID": fields.String(required=False),
        "keySecretAccessKey": fields.String(required=False),
        "keyBucket": fields.String(required=False),
        "keyEndpoint": fields.String(required=False),
        "keyPath": fields.String(required=False),
    },
)


mOptionValueFromS3Values = api_experiments.model(
    "option-valuefrom-s3values",
    {
        "accessKeyID": fields.String(required=False),
        "secretAccessKey": fields.String(required=False),
        "bucket": fields.String(required=False),
        "endpoint": fields.String(required=False),
        "path": fields.String(required=False),
        "rename": fields.String(
            description="If set, and path is not None then this means that the path filename should be renamed "
            "to match @rename",
            required=False,
        ),
        "region": fields.String(required=False),
    },
)


mOptionValueFromDatasetRef = api_experiments.model(
    "option-valuefrom-datasetref",
    {
        "name": fields.String(),
        "path": fields.String(required=False),
        "rename": fields.String(
            description="If set, and path is not None then this means that the path filename should be renamed "
            "to match @rename",
            required=False,
        ),
    },
)

mOptionValueFromUsernamePassword = api_experiments.model(
    "option-valuefrom-usernamepassword",
    {
        "username": fields.String(required=False),
        "password": fields.String(required=False),
    },
)


mVeBasePackageSourceGitSecurityOauthValueFrom = api_experiments.model(
    "ve-base-package-source-git-security-oauth-valuefrom",
    {
        "secretKeyRef": fields.Nested(mOptionValueFromSecretKeyRef, required=False),
    },
)

mVeBasePackageSourceGitSecurityOauth = api_experiments.model(
    "ve-base-package-source-git-security-oauth",
    {
        "value": fields.String(required=False),
        "valueFrom": fields.Nested(mVeBasePackageSourceGitSecurityOauthValueFrom, required=False),
    },
)

mVeBasePackageSourceGitSecurity = api_experiments.model(
    "ve-base-package-source-git-security",
    {"oauth": fields.Nested(mVeBasePackageSourceGitSecurityOauth, required=False)},
)

mVeBasePackageSourceGitLocation = api_experiments.model(
    "ve-base-package-source-git-location",
    {
        "branch": fields.String(description="Git branch name, mutually exclusive with tag and commit", required=False),
        "tag": fields.String(description="Git tag name, mutually exclusive with branch and commit", required=False),
        "commit": fields.String(description="Git commit digest, mutually exclusive with branch and tag",
                                required=False),
        "url": fields.String(description="Git url, must provide this if package is hosted on a Git server",
                             required=False),
    },
)

mVeBasePackageSourceGit = api_experiments.model(
    "ve-base-package-source-git",
    {
        "security": fields.Nested(mVeBasePackageSourceGitSecurity, required=False),
        "location": fields.Nested(mVeBasePackageSourceGitLocation, required=False),
        "version": fields.String(required=False),
    },
)


mVeBasePackageSourceDatasetInfo = api_experiments.model(
    "ve-base-package-source-dataset-info", {"dataset": fields.String()}
)

mVeBasePackageSourceDataset = api_experiments.model(
    "ve-base-package-source-dataset",
    {
        "location": fields.Nested(mVeBasePackageSourceDatasetInfo, required=False),
        "version": fields.String(required=False),
        "security": fields.Nested(mVeBasePackageSourceDatasetInfo, required=False),
    },
)

mVeBasePackageSource = api_experiments.model(
    "ve-base-package-source",
    {
        "git": fields.Nested(mVeBasePackageSourceGit, required=False),
        "dataset": fields.Nested(mVeBasePackageSourceDataset, required=False),
    },
)

mVeBasePackageConfig = api_experiments.model(
    "ve-base-package-config",
    {
        "path": fields.String(required=False),
        "manifestPath": fields.String(required=False),
    },
)

mVeBasePackageDependenciesImageRegistrySecurityValueFromSecretKeyRef = api_experiments.model(
    "ve-base-package-dependencies-imageregistry-security-valuefrom-secretkeyref",
    {
        "secretKeyRef": fields.Nested(mOptionValueFromSecretKeyRef, required=False),
    },
)

mVeBasePackageDependenciesImageRegistrySecurity = api_experiments.model(
    "ve-base-package-dependencies-imageregistry-security-valuefrom-secretkeyref",
    {
        "value": fields.String(required=False),
        "valueFrom": fields.Nested(mVeBasePackageDependenciesImageRegistrySecurityValueFromSecretKeyRef,
                                   required=False),
    },
)

mVeBasePackageDependenciesImageRegistry = api_experiments.model(
    "ve-base-package-dependencies-imageregistry",
    {
        "serverUrl": fields.String(required=False),
        "security": fields.Nested(mVeBasePackageDependenciesImageRegistrySecurity, required=False)
    },
)

mVeBasePackageDependencies = api_experiments.model(
    "ve-base-package-dependencies",
    {
        "imageRegistries": fields.List(
            fields.Nested(mVeBasePackageDependenciesImageRegistry), default=[]
        )
    },
)

# VV: this is a beta field, we're hiding it for now
mVeBasePackageGraphBinding = api_experiments.model(
    "ve-base-package-graph-binding",
    {
        "name": fields.String(
            description="Name in the scope of this collection of bindings, "
            "must not contain string !!! or \\n. "
            "If None then reference and optionally stages must be provided"
        ),
        "reference": fields.String(
            description="A FlowIR reference to associate with binding", required=False
        ),
        "type": fields.String(
            description="Valid types are input and output, if left None and binding belongs to a collection "
            "the type field receives the approriate default value",
            required=False,
        ),
        "stages": fields.List(
            fields.String(
                description="If reference points to multiple components which have the same name "
                "but belong to multiple stages"
            ),
            required=False,
        ),
    },
)

# VV: this is a beta field, we're hiding it for now
mVeBasePackageGraphBindingCollection = api_experiments.model(
    "ve-base-package-graph-bindingcollection",
    {
        "input": fields.List(fields.Nested(mVeBasePackageGraphBinding), default=[]),
        "output": fields.List(fields.Nested(mVeBasePackageGraphBinding), default=[]),
    },
)

# VV: this is a beta field, we're hiding it for now
mVeBasePackageGraphNode = api_experiments.model(
    "ve-base-package-graph-node",
    {
        "reference": fields.String(
            description="An absolute FlowIR reference string of an un-replicated component, e.g. stage0.simulation"
        )
    },
)

# VV: this is a beta field, we're hiding it for now
mVeBasePackageGraph = api_experiments.model(
    "ve-base-package-graph",
    {
        "name": fields.String(),
        "bindings": fields.Nested(mVeBasePackageGraphBindingCollection),
        "nodes": fields.List(fields.Nested(mVeBasePackageGraphNode), default=[]),
    },
)

mVeBasePackage = api_experiments.model(
    "ve-base-package",
    {
        "name": fields.String(
            description='Unique name of base package in this virtual experiment entry. Defaults to "main"',
            default="main",
        ),
        "source": fields.Nested(mVeBasePackageSource),
        "config": fields.Nested(mVeBasePackageConfig),
        # VV: these are beta fields, we're hiding them for now
        # "dependencies": (fields.Nested(mVeBasePackageDependencies)),
        # "graph": fields.List(fields.Nested(mVeBasePackageGraph), default=[]),
    },
)

# VV: this is a beta field, we're hiding it for now
mBindingOptionValueFromApplicationDependency = api_experiments.model(
    "bindingoption-valuefrom-applicationdependency",
    {
        "referece": fields.String(
            description="Reference to application dependency in the derived package"
        )
    },
)

# VV: this is a beta field, we're hiding it for now
mBindingOptionValueFromGraph = api_experiments.model(
    "bindingoption-valuefrom-graph",
    {
        "name": fields.String(
            description="Name of the graph, format is ${package.Name}/${graph.Name}}"
        ),
        "binding": fields.Nested(
            mVeBasePackageGraphBinding,
            description='The source binding of which to use the value. It must be of type "output"',
        ),
    },
)

# VV: this is a beta field, we're hiding it for now
mBindingOptionValueFrom = api_experiments.model(
    "bindingoption-valuefrom",
    {
        "graph": fields.Nested(mBindingOptionValueFromGraph, required=False),
        "applicationDependency": fields.Nested(
            mBindingOptionValueFromApplicationDependency, required=False
        ),
    },
)

# VV: this is a beta field, we're hiding it for now
mBindingOption = api_experiments.model(
    "bindingoption",
    {
        "name": fields.String(description="The symbolic name"),
        "valueFrom": fields.Nested(
            mBindingOptionValueFrom,
            description="The source of the value to map the symbolic name to",
        ),
    },
)

# VV: this is a beta field, we're hiding it for now
mVeBasePackageGraphInstance = api_experiments.model(
    "ve-base-package-graph-instance",
    {
        "graph": fields.Nested(
            mVeBasePackageGraph,
            description="The graph to instantiate, its name must be ${basePackage.name}/${graph.name}",
        ),
        "bindings": fields.List(fields.Nested(mBindingOption), default=[]),
    },
)

# VV: this is a beta field, we're hiding it for now
mPathInsidePackage = api_experiments.model(
    "pathinsidepackage",
    {
        "packageName": fields.String(description="Package Name", required=False),
        "path": fields.String(
            description="Relative path to location of package", required=False
        ),
    },
)

# VV: this is a beta field, we're hiding it for now
mIncludePath = api_experiments.model(
    "includepath",
    {
        "source": fields.Nested(mPathInsidePackage, description="Source of path"),
        "dest": fields.Nested(
            mPathInsidePackage,
            description='Destination of path, defaults to just "path: source.path"',
            required=False,
        ),
    },
)

mExtractionMethodSource = api_experiments.model(
    "extractionmethodsource",
    {
        "path": fields.String(required=False),
        "pathList": fields.List(fields.String(), required=False),
        "keyOutput": fields.String(required=False),
    },
)

mFlowIRInterfaceInputExtractionMethodHookGetInputs = api_experiments.model(
    "flowirinterface-inputextractionmethod-hookgetinputs",
    {"source": fields.Nested(mExtractionMethodSource, required=False)},
)


mFlowIRInterfaceInputExtractionMethodCsvColumn = api_experiments.model(
    "flowirinterface-inputextractionmethod-csvcolumn",
    {
        "source": fields.Nested(mExtractionMethodSource, required=False),
        "args": fields.Raw(default={}),
    },
)

mFlowIRInterfaceInputExtractionMethod = api_experiments.model(
    "flowirinterface-inputextractionmethod",
    {
        "hookGetInputIds": fields.Nested(
            mFlowIRInterfaceInputExtractionMethodHookGetInputs, required=False
        ),
        "csvColumn": fields.Nested(
            mFlowIRInterfaceInputExtractionMethodCsvColumn, required=False
        ),
    },
)

mFlowIRInterfaceSpec = api_experiments.model(
    "flowirinterface-spec",
    {
        "namingScheme": fields.String(),
        "inputExtractionMethod": fields.Nested(mFlowIRInterfaceInputExtractionMethod),
        "hasAdditionalData": fields.Boolean(default=False),
    },
)

mFlowIRInterface = api_experiments.model(
    "flowirinterface",
    {
        "id": fields.String(required=False),
        "description": fields.String(required=False),
        "inputSpec": fields.Nested(mFlowIRInterfaceSpec),
        "propertiesSpec": fields.List(fields.Nested(mFlowIRInterfaceSpec), default=[]),
    },
)

mVeBase = api_experiments.model(
    "ve-base",
    {
        "packages": fields.List(fields.Nested(mVeBasePackage), default=[]),
        # "connections": fields.List(
        #     fields.Nested(mVeBasePackageGraphInstance), default=[]
        # ),
        # "includePaths": fields.List(fields.Nested(mIncludePath), default=[]),
        # "output": fields.List(fields.Nested(mBindingOption), default=[]),
        # "interface": fields.Nested(mFlowIRInterface, required=False),
    },
)

mVeMedataPackage = api_experiments.model(
    "ve-metadata-package",
    {
        "name": fields.String(required=False),
        "tags": fields.List(fields.String(), required=False, default=[]),
        "keywords": fields.List(fields.String(), default=[]),
        "license": fields.String(required=False),
        "maintainer": fields.String(required=False),
        "description": fields.String(required=False),
    },
)

mPlatformVariableValue = api_experiments.model(
    "valueinplatform",
    {
        "value": fields.String(),
        "platform": fields.String(required=False)
    },
)

mPlatformVariable = api_experiments.model(
    "variablewithdefaultvalues",
    {
        "name": fields.String(),
        "valueFrom": fields.List(fields.Nested(mPlatformVariableValue), default=[]),
    },
)

mExecutionOptionDefaults = api_experiments.model(
    "executionoptiondefaults",
    {
        "variables": fields.List(
            fields.Nested(mPlatformVariable),
            default=[],
            description="The default values of the variables in the virtual experiment DSL",
        )
    },
)

mDataFileName = api_experiments.model(
    "data-filename",
    {
        "name": fields.String(description="The name of the data file", required=False),
    },
)

mInputFileName = api_experiments.model(
    "input-filename",
    {
        "name": fields.String(description="The name of the input file", required=False),
    },
)

mContainerImage = api_experiments.model(
    "container-image",
    {
        "name": fields.String(description="The container image url", required=False),
    },
)

mVeMetadataRegistry = api_experiments.model(
    "ve-metadata-registry",
    {
        "createdOn": fields.String(required=False),
        "digest": fields.String(required=False),
        "tags": fields.List(fields.String(), required=False, default=[]),
        "timesExecuted": fields.Integer(default=0),
        "interface": fields.Raw(default={}),
        "inputs": fields.List(fields.Nested(mInputFileName), default=[], required=False),
        "data": fields.List(fields.Nested(mDataFileName), deafult=[], required=False),
        "containerImages": fields.List(
            fields.Nested(mContainerImage), default=[], required=False
        ),
        "executionOptionsDefaults": fields.Nested(mExecutionOptionDefaults),
    },
)

mVeMetadata = api_experiments.model(
    "ve-metadata",
    {
        "package": fields.Nested(mVeMedataPackage),
        "registry": fields.Nested(mVeMetadataRegistry),
    },
)

mOrchestratorResources = api_experiments.model(
    "orchestratorresources",
    {"cpu": fields.String(required=False), "memory": fields.String(required=False)},
)

mVeParameterisationRuntime = api_experiments.model(
    "ve-parameterisation-runtime",
    {
        "resources": fields.Nested(mOrchestratorResources),
        "args": fields.List(fields.String(), default=[]),
    },
)

mVeParameterisationPresetsVariable = api_experiments.model(
    "ve-parameterisation-presets-variable",
    {
        "name": fields.String(description="The name of the variable", required=False),
        "value": fields.String(description="The preset value of the variable", required=False),
    },
)


mVeParameterisationPresetsEnvironmentVariables = api_experiments.model(
    "ve-parameterisation-presets-environment-variables",
    {
        "name": fields.String(
            description="Name of the environment variable to inject into the containers in the orchestrator pod"),
        "value": fields.String(description="The value of the environment variable")
    }
)

mVeParameterisationPresets = api_experiments.model(
    "ve-parameterisation-presets",
    {
        "variables": fields.List(fields.Nested(mVeParameterisationPresetsVariable), default=[]),
        "runtime": fields.Nested(mVeParameterisationRuntime),
        "data": fields.List(fields.Nested(mDataFileName), default=[]),
        "environmentVariables": fields.List(fields.Nested(mVeParameterisationPresetsEnvironmentVariables), default=[]),
        "platform": fields.String(required=False),
    },
)

mValueChoice = api_experiments.model(
    "ve-parameterisation-executionoptions-variable-choice",
    {
        "value": fields.String("One of the valid choices")
    }
)

mVeParameterisationExecutionOptionsVariable = api_experiments.model(
    "ve-parameterisation-executionoptions-variable",
    {
        "name": fields.String(required=False),
        "value": fields.String(
            description="This is the default value of the variable, providing this field means "
                        "that the variable can recieve *any* value", required=False,
        ),
        "valueFrom": fields.List(fields.Nested(mValueChoice), required=False),
    },
)

mVeParameterisationExecutionOptions = api_experiments.model(
    "ve-parameterisation-executionoptions",
    {
        "variables": fields.List(fields.Nested(mVeParameterisationExecutionOptionsVariable), default=[]),
        "data": fields.List(fields.Nested(mDataFileName), default=[]),
        "runtime": fields.Nested(mVeParameterisationRuntime),
        "platform": fields.List(fields.String(), default=[]),
    },
)


mVeParameterisation = api_experiments.model(
    "ve-parameterisation",
    {
        "presets": fields.Nested(mVeParameterisationPresets),
        "executionOptions": fields.Nested(mVeParameterisationExecutionOptions),
    },
)

mVirtualExperiment = api_experiments.model(
    "ve",
    {
        "base": fields.Nested(mVeBase),
        "parameterisation": fields.Nested(mVeParameterisation),
        "metadata": fields.Nested(mVeMetadata),
    },
)

mPackageHistory = api_experiments.model(
    "package-history",
    {
        "tags": fields.List(
            fields.Nested(
                api_experiments.model(
                    "package-history-tag",
                    {
                        "tag": fields.String(
                            description="The tag e.g. latest", example="latest"
                        ),
                        "head": fields.String(
                            description="The digest that the tag points to",
                            example="sha256x67357eeed694e4f954fda270d6adba7b2399823c1b30dd1513d9e8c08d919399",
                        ),
                    },
                )
            )
        ),
        "untagged": fields.List(
            fields.Nested(
                api_experiments.model(
                    "package-history-untagged",
                    {
                        "digest": fields.String(
                            description="A digest which is no longer the head of any of the tags",
                            example="sha256xd5067fc65aa4b569348caf347ca76983f6ecd2e45cc708410ba806f3835905ef",
                        )
                    },
                )
            )
        ),
    },
)

mFileContent = api_experiments.model(
    "file-content",
    {
        "filename": fields.String(
            required=False,
            description="Filename. Mutually exclusive with sourceFilename and targetFilename",
            example="field.conf",
        ),
        "sourceFilename": fields.String(
            required=False,
            description="path to the filename. Mutually exclusive with filename and content. "
            "If set, must also provide sourceFilename",
        ),
        "targetFilename": fields.String(
            required=False,
            description="How to rename sourceFilename. Mutually exclusive with filename and content. "
            "If set, must also provide targetFilename",
        ),
        "content": fields.String(
            required=False,
            description="Content of file. Mutually exclusive with sourceFilename and targetFilename",
            example="""mole:capb,slampd,smlta
conc:4.2,1.4,0.5
salt:2.8""",
        ),
    },
)

# mDataContent = api_experiments.model('data-content', {
#     'filename': fields.String(required=True, description='Filename or relative path to file in S3 bucket',
#                               example="pag_data.csv"),
#     'content': fields.String(required=True, description='Content of file (will be discarded if using S3 bucket)'
#                              , example='label	SMILES\n'
#                                        'mymol	O=S(=O)([O-])c1c(C(F)(F)F)cc(C(F)(F)F)cc1C(F)(F)F.Cc1cc(OC(C)(C)C)cc(C)c1[S+](c1ccccc1)c1ccccc1')
# })

mS3 = api_experiments.model(
    "s3",
    {
        "dataset": fields.String(
            required=False,
            description="Identifier of Dataset to use (uses https://github.com/datashim-io/datashim). "
            "If set, remaining S3 information will not be used",
        ),
        "accessKeyID": fields.String(
            required=True, description="Access key id", default=""
        ),
        "secretAccessKey": fields.String(
            required=True, description="Secret access key", default=""
        ),
        "bucket": fields.String(
            required=True, description="Name of bucket", default=""
        ),
        "endpoint": fields.String(
            required=True,
            default="",
            description="Endpoint URL (e.g. https://s3.eu-gb.cloud-object-storage.appdomain.cloud)",
        ),
        "region": fields.String(
            required=False, description="Region (optional)", default=""
        ),
    },
)

mS3Credentials = api_experiments.model(
    "s3-credentials",
    {
        "accessKeyID": fields.String(
            required=True, description="Access key id", default=""
        ),
        "secretAccessKey": fields.String(
            required=True, description="Secret access key", default=""
        ),
        "bucket": fields.String(
            required=True, description="Name of bucket", default=""
        ),
        "endpoint": fields.String(
            required=True,
            default="",
            description="Endpoint URL (e.g. https://s3.eu-gb.cloud-object-storage.appdomain.cloud)",
        ),
        "region": fields.String(
            required=False, description="Region (optional)", default=""
        ),
    },
)

mS3Store = api_experiments.model(
    "s3-output",
    {
        "credentials": fields.Nested(
            mS3Credentials, default=None, required=True, description="S3 Configuration"
        ),
        "bucketPath": fields.String(
            required=True,
            default="workflow_instances/",
            description="The ST4SD runtime core will upload the workflow outputs under "
            "this path",
        ),
    },
)

defaultContainerResourcesCpu = "1"
defaultContainerResourcesMemory = "500Mi"

mContainerResources = api_experiments.model(
    "container-resources",
    {
        "cpu": fields.String(
            required=False,
            description="Cpu units to as a limit for the container (e.g. 0.1, 1)",
            example=defaultContainerResourcesCpu,
        ),
        "memory": fields.String(
            required=False,
            description="Memory defined as either bytes, mebibytes "
            "(e.g. 100Mi which is 104857600 bytes), or gibibytes (100Gi)",
            example=defaultContainerResourcesMemory,
        ),
    },
)

mVolumeType = api_experiments.model(
    "volume-type",
    {
        "persistentVolumeClaim": fields.String(
            required=False,
            description="(VOLUME_TYPE) name of PersistentVolumeClaim to mount, incompatible with other VOLUME_TYPE fields",
        ),
        "configMap": fields.String(
            required=False,
            description="(VOLUME_TYPE) name of ConfigMap to mount, incompatible with other VOLUME_TYPE fields",
        ),
        "dataset": fields.String(
            required=False,
            description="(VOLUME_TYPE) name of Dataset object to mount, incompatible with other VOLUME_TYPE fields",
        ),
        "secret": fields.String(
            required=False,
            description="(VOLUME_TYPE) name of Secret object to mount, incompatible with other VOLUME_TYPE fields",
        ),
    },
)

mVolume = api_experiments.model(
    "mount-volume",
    {
        "type": fields.Nested(
            mVolumeType, default={}, description="Volume type definition"
        ),
        "applicationDependency": fields.String(
            required=False,
            description="Application dependency for which flow will create a link that points to the mount-path of this "
            "volume (optional). This is expected to be an entry under the list of strings defined by the "
            "application-dependencies.<platform-name> field within the FlowIR of the workflow.",
        ),
        # "readOnly": fields.Boolean(required=False,
        #                            description="Mounted read-only if true, read-write otherwise (false or unspecified). "
        #                                        "Defaults to True.", default=True),
        "subPath": fields.String(
            required=False,
            description="Path within the volume from which the container's volume should be mounted. "
            'Defaults to "" (volume\'s root).',
        ),
        # "mountPath": fields.String(
        #     required=False,
        #     description="Path within the container at which the volume should be mounted. Must not contain ':'. "
        #                 "Defaults to /input-volumes/<name of PersistentVolumeClaim OR ConfigMap OR Dataset>"),
    },
)

mExperimentStart = api_experiments.model(
    "experiment-start",
    {
        "platform": fields.String(
            required=False,
            description="The platform to use for the execution of the virtual experiment. It should "
            "match the parameterisation options of the parameterised virtual experiment "
            "package you are starting.",
        ),
        "inputs": fields.List(fields.Nested(mFileContent), required=False),
        "data": fields.List(fields.Nested(mFileContent), required=False),
        "volumes": fields.List(fields.Nested(mVolume)),
        "variables": fields.Raw(
            example={"startIndex": 10, "numberMolecules": 2, "functional": "B3LYPV3"}
        ),
        "additionalOptions": fields.List(
            fields.String(
                required=False,
                description="Additional options to elaunch.py",
                example="--registerWorkflow=True",
            )
        ),
        "environmentVariables": fields.Raw(example={"RUNTIME_SECRET_TOKEN": "<token>"}),
        "orchestrator_resources": fields.Nested(
            mContainerResources,
            required=False,
            description="Hardware resource limits for the container that is hosting "
            "the workflow orchestrator",
        ),
        "metadata": fields.Raw(
            example={
                "exp-label": "no-spaces-allowed",
            }
        ),
        "s3": fields.Nested(
            mS3,
            required=False,
            description="S3 configuration (read the description of the Filename-Content pair model too)",
        ),
        "s3Store": fields.Nested(
            mS3Store,
            default={},
            required=False,
            description="Configuration to store outputs of workflow instance to a S3 bucket. "
            "Mutually exclusive with datasetStoreURI",
        ),
        # For backwards compatibility support both datasetStoreURI and dlfStoreURI but only advertise datasetStoreURI
        "datasetStoreURI": fields.String(
            required=False,
            description="Dataset URI to store outputs (uses github.com/datashim-io/datashim) "
            "i.e. dataset://<dataset-name>/path/in/dataset/to/upload/outputs/to. "
            "Mutually exclusive with s3Store.",
        ),
        "runtimePolicy": fields.Nested(
            api_experiments.model(
                "experiment-start-policy",
                {
                    "name": fields.String(description="Nane of runtime policy"),
                    "config": fields.Raw(
                        example={},
                        description="Configuration options for runtime policy",
                    ),
                },
            )
        ),
    },
)

mLambdaExperimentStart = api_experiments.model(
    "experiment-lambda-start",
    {
        "volumes": fields.List(fields.Nested(mVolume), required=False),
        "data": fields.List(fields.Nested(mFileContent), required=False),
        "scripts": fields.List(
            fields.Nested(mFileContent),
            required=False,
            description="Scripts to placed under the `bin` directory of the experiment",
        ),
        "variables": fields.Raw(
            example={"startIndex": 10, "numberMolecules": 2, "functional": "B3LYPV3"}
        ),
        "additionalOptions": fields.List(
            fields.String(
                required=False,
                description="Additional options to elaunch.py",
                example="--registerWorkflow=True",
            )
        ),
        "environmentVariables": fields.Raw(example={"RUNTIME_SECRET_TOKEN": "<token>"}),
        "orchestrator_resources": fields.Nested(
            mContainerResources,
            required=False,
            description="Hardware resource limits for the container that is hosting "
            "the workflow orchestrator",
        ),
        "metadata": fields.Raw(
            example={
                "exp-label": "no-spaces-allowed",
            },
            required=False,
        ),
        # "validate_flowir": fields.Boolean(description="Whether to validate FlowIR before executing workflow, "
        #               "default is True", default=False, required=False),
        "lambdaFlowIR": fields.Raw(
            example={
                "flowir key": "flowir value",
            },
            description="JSON representation of FlowIR",
        ),
        "s3": fields.Nested(
            mS3,
            required=False,
            description="S3 configuration (read the description of the Filename-Content pair model too)",
        ),
        "s3Store": fields.Nested(
            mS3Store,
            required=False,
            description="Configuration to store outputs of workflow instance to a S3 bucket. "
            "Mutually exclusive with datasetStoreURI",
        ),
        # For backwards compatibility support both datasetStoreURI and dlfStoreURI but only advertise datasetStoreURI
        "datasetStoreURI": fields.String(
            required=False,
            description="Dataset URI to store outputs (uses github.com/datashim-io/datashim) "
            "i.e. dataset://<dataset-name>/path/in/dataset/to/upload/outputs/to. "
            "Mutually exclusive with s3Store.",
        ),
    },
)

############################ Instances ############################

api_instances = Namespace(
    "instances", description="Instances of Experiments related operations"
)

# cost=0
# current-stage=Generating
# exit-status=N/A
# experiment-state=running
# stage-progress=0.952
# stage-state=running
# stages=['Setup', 'Generating', 'Postprocess']
# total-progress=0.9144
# updated=2019-11-25 15:43:44.671713
# TODO fill out the rest of the status
experiment_status = api_instances.model(
    "experiment-status",
    {
        "experiment-state": fields.String(required=True, example="running"),
        "stage-state": fields.String(required=True, example="running"),
        "stages": fields.List(fields.String),
        "current-stage": fields.String(),
        "meta": fields.Raw(),
        "exit-status": fields.String(),
        "total-progress": fields.Float(required=True),
        "stage-progress": fields.Float(required=True),
        "error-description": fields.String(),
    },
)

experiment_instance = api_instances.model(
    "experiment-instance",
    {
        "id": fields.String(required=True),
        "experiment": fields.Nested(mVirtualExperiment),
        "status": fields.Nested(experiment_status),
        "k8s-labels": fields.Raw(example={"rest-uid": "ionisation-energy-z3u2c"}),
        "outputs": fields.Raw(
            example={
                "AnionResults": {
                    "creationtime": "2019-12-04 19:00:54.993316",
                    "description": "Anion homo/lumo results",
                    "filename": "energies.csv",
                    "filepath": "/tmp/workdir/ionisation-energy-2019-12-04T181218.613966.instance/output/energies.csv",
                    "final": "no",
                    "production": "yes",
                    "type": "csv",
                    "version": "14",
                }
            }
        ),
    },
)

############################ relationships ############################
api_relationships = Namespace("relationships", description="")

m_graph_value = api_relationships.model(
    "relationship-transform-relationship-graphpvalue",
    {
        "name": fields.String(description="Symbolic name"),
        "default": fields.String(
            description="An optional default value that the symbolic name may contain"
        ),
    },
)

m_transform = api_relationships.model(
    "relationship-transform",
    {
        "inputGraph": fields.Nested(
            api_relationships.model(
                "query-relationships-transform-inputgraph",
                {
                    "identifier": fields.String(
                        description="The parameterised virtual experiment package containing the inputGraph",
                        required=True,
                    )
                },
            )
        ),
        "outputGraph": fields.Nested(
            api_relationships.model(
                "query-relationships-transform-outputgraph",
                {
                    "identifier": fields.String(
                        description="Regular expression to match the identifiers of "
                        "outputGraphs in transform relationships"
                    )
                },
            )
        ),
        "relationship": fields.Nested(
            api_relationships.model(
                "relationship-transform-relationship",
                {
                    "inferParameters": fields.Boolean(
                        description="Whether to auto-update relationship with information "
                        "about "
                        "mappings bettween parameters of the 2 graph fragments",
                        default=True,
                    ),
                    "inferResults": fields.Boolean(
                        description="Whether to auto-update relationship with information about "
                        "mappings bettween results of the 2 graph fragments",
                        default=True,
                    ),
                    "graphParameters": fields.List(
                        fields.Nested(
                            api_relationships.model(
                                "relationship-transform-relationship-graphparameters",
                                {
                                    "inputGraphParameter": fields.Nested(m_graph_value),
                                    "outputGraphParameter": fields.Nested(
                                        m_graph_value
                                    ),
                                },
                                description="Maps an inputGraph parameter to an outputGraph parameter",
                            )
                        )
                    ),
                    "graphResults": fields.List(
                        fields.Nested(
                            api_relationships.model(
                                "relationship-transform-relationship-graphresults",
                                {
                                    "inputGraphResult": fields.Nested(m_graph_value),
                                    "outputGraphResult": fields.Nested(m_graph_value),
                                },
                                description="Maps an outputGraph result to an inputGraph result",
                            )
                        )
                    ),
                },
            )
        ),
    },
)

m_relationship = api_relationships.model(
    "relationship",
    {
        "identifier": fields.String(required=True, example="pm3-to-dft"),
        "description": fields.String(
            required=False,
            description="Human readable description of transformation relationship",
        ),
        "transform": fields.Nested(m_transform),
    },
)

mSynthesizeOptions = api_relationships.model(
    "relationship-synthesize-options",
    {
        "generateParameterisation": fields.Boolean(
            default=True,
            description="Whether to auto-generate parameterisation options. When False the method will not auto-generate "
            "any parameterisation configuration for the synthesized parameterised virtual experiment package. "
            "When True the method generates 1 preset for each variable in the final experiment which has a "
            "unique value. It overrides this information using the presets/executionOptions of the "
            'parent(outputGraph). It overrides this merged information using the "parameterisation" '
            "settings that are part of this payload. The default is `True`",
        )
    },
)

m_payload_synthesize = api_relationships.model(
    "relationship-synthesize",
    {
        "options": fields.Nested(mSynthesizeOptions),
        "parameterisation": fields.Nested(mVeParameterisation),
    },
)

############################ Queries ############################

api_query = Namespace("query", description="Query operations")
mQueryExperiment = api_query.model(
    "query-experiments",
    {
        "package": fields.Nested(
            api_query.model(
                "query-experiments-package",
                {"definition": fields.Nested(mVeBasePackage)},
            )
        ),
        "relationship": fields.Nested(
            api_query.model(
                "query-experiments-relationship",
                {
                    "identifier": fields.String(
                        description="The identifier of the relationship"
                    ),
                    "transform": fields.Nested(
                        api_query.model(
                            "query-experiments-relationship-transform",
                            {
                                "matchInputGraph": fields.Boolean(
                                    description="Whether to query using the package of the inputGraph",
                                    default=False,
                                ),
                                "matchOutputGraph": fields.Boolean(
                                    description="Whether to query using the package of the inputGraph",
                                    default=False,
                                ),
                            },
                        )
                    ),
                },
            )
        ),
        "common": fields.Nested(
            api_query.model(
                "query-experiment-common",
                {
                    "matchPackageVersion": fields.Boolean(
                        description="Whether to match the version of packages",
                        default=False,
                    ),
                    "mustHaveOnePackage": fields.Boolean(
                        description="Match only parameterised virtual experiment packages with just one base package",
                        default=True,
                    ),
                },
            )
        ),
    },
    description="Query database for experiments",
)

mQueryRelationship = api_query.model(
    "query-relationships",
    {
        "identifier": fields.String(
            required=False,
            description="Regular expression to match names of relationships",
        ),
        "transform": fields.Nested(
            api_query.model(
                "query-relationships-transform",
                {
                    "inputGraph": fields.Nested(
                        api_query.model(
                            "query-relationships-transform-inputgraph",
                            {
                                "identifier": fields.String(
                                    description="Regular expression to match the identifiers of inputGraphs "
                                    "in transform relationships"
                                )
                            },
                        )
                    ),
                    "outputGraph": fields.Nested(
                        api_query.model(
                            "query-relationships-transform-outputgraph",
                            {
                                "identifier": fields.String(
                                    description="Regular expression to match the identifiers of outputGraphs "
                                    "in transform relationships"
                                )
                            },
                        )
                    ),
                },
            )
        ),
    },
)
