# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


"""
File contains all models, and `Namespace` instances, of all APIs in this flask application
"""
from flask_restx import Namespace, fields

############################ URL Map ############################

api_url_map = Namespace('url-map', description='Operations to interact with workflow software stack URLs')

############################ Image Pull Secrets ############################

api_image_pull_secret = Namespace('image-pull-secrets', description='Operations to view/create/edit imagePullSecrets')

model_get_specific = api_image_pull_secret.model('imagepullsecret-identifier', {
    'id': fields.String(required=True)
})

model_imagePullSecret_full = api_image_pull_secret.model("imagepullsecret", {
    'server': fields.String(description='docker registry, e.g. res-drl-hpc-docker-local.artifactory.swg-devops.com',
                            required=True, example='url-docker-registry'),
    'username': fields.String(description='username to use when authenticating to docker registry', required=True),
    'password': fields.String(password='password to use when authenticating to docker registry', required=True)
})

model_put = api_image_pull_secret.model("update_image_pull_secret", model_imagePullSecret_full)

# VV: Cannot create models out of Lists (flask-restx models must be Dictionaries)
example_imagePullSecret_get = fields.List(
    fields.String(description='docker registry, e.g. res-drl-hpc-docker-local.artifactory.swg-devops.com',
                  example='url-docker-registry'),
    description='GET model for imagePullSecret with a given id', example=["url-docker-registry1",
                                                                          "url-docker-registry2"])

example_imagePullSecret_get_all = fields.Raw({}, example={
    'k8s-secret-name': ['url-docker-registry1', 'url-docker-registry2'],
})

############################ Authorisation ############################

api_authorisation = Namespace('authorisation', description='Operations to interact with authorization token(s)')

############################ Datasets ############################

api_datasets = Namespace('datasets', description='Datashim related operations')

s3_model = api_datasets.model('dataset-s3-configuration', {
    'accessKeyID': fields.String(required=True, description='Access key id', default=''),
    'secretAccessKey': fields.String(required=True, description='Secret access key', default=''),
    'bucket': fields.String(required=True, description="Name of bucket", default=''),
    'endpoint': fields.String(
        required=True, default='',
        description="Endpoint URL (e.g. https://s3.eu-gb.cloud-object-storage.appdomain.cloud)"),
    'region': fields.String(required=False, description="Region (optional)", default=''),
})

############################ Experiments ############################

api_experiments = Namespace('experiments', description='Experiments related operations')

mBaseSourceGitOauth = api_experiments.model(
    'base-source-git-security-oauth',
    {
        'valueFrom': fields.Nested(
            api_experiments.model(
                'base-source-git-security-oauth-valueFrom',
                {
                    'secretKeyRef': fields.Nested(api_experiments.model(
                        "base-source-git-security-oauth-valuefrom-secret",
                        {
                            'name': fields.String(description="Name of Secret object"),
                            'key': fields.String(
                                description="Key in Secret object that holds token")
                        })),
                    'value': fields.String(description="An OAuth token")
                }))
    }, description="Specifies an OAuth token to use for authentication.")

mBaseSourceS3Security = api_experiments.model(
    'base-source-s3-security',
    {
        'valueFrom': fields.Nested(
            api_experiments.model(
                'base-source-s3-security',
                {
                    'secretS3KeyRef': fields.Nested(api_experiments.model(
                        "base-source-s3-security-valuefrom-s3-secret",
                        {
                            'name': fields.String(description="Name of Secret object"),
                            'keyAccessKeyID': fields.String(
                                description="Key in Secret object that holds accessKeyID",
                                default="accessKeyID"),
                            'keySecretAccessKey': fields.String(
                                description="Key in Secret object that holds secretAccessKey",
                                default="secretAccessKey"),
                        })),
                    's3Ref': fields.Nested(api_experiments.model(
                        'base-source-s3-security-valuefrom-values',
                        {
                            'accessKeyID': fields.String(description="Value of accessKeyID"),
                            'secretAccessKey': fields.String(description="Value of secretAccessKey")
                        }))
                }))
    }, description="Specifies an OAuth token to use for authentication.")

mBaseSourceGitLocation = api_experiments.model(
    'base-source-git-location',
    {
        'branch': fields.String(description="Git branch name"),
        'tag': fields.String(description="Git tag name"),
        'commit': fields.String(description="Git commit digest"),
    })

mBaseSourceGit = api_experiments.model(
    'base-source-git',
    {
        'security': fields.Nested(api_experiments.model(
            'base-source-git-security',
            {
                'oauth': fields.Nested(mBaseSourceGitOauth)
            })),
        'location': fields.Nested(mBaseSourceGitLocation),
    })

mBaseSourceS3 = api_experiments.model(
    'base-source-s3',
    {
        'security': fields.Nested(mBaseSourceS3Security),
        'location': fields.Nested(api_experiments.model(
            'base-source-s3-location',
            {
                'region': fields.String(description="S3 region identifier"),
                'endpoint': fields.String(description="S3 endpoint"),
                'bucket': fields.String(description="Name of bucket"),
            }
        ))
    })

mBaseSourceDataset = api_experiments.model(
    'base-source-dataset',
    {
        'security': fields.Nested(api_experiments.model(
            'base-source-dataset-security',
            {
                'fromValue': fields.Nested(api_experiments.model(
                    'base-source-dataset-security-fromvalue',
                    {
                        'datasetRef': fields.Nested(api_experiments.model(
                            'base-source-dataset-security-from-value-dataset',
                            {
                                'name': fields.String(description="Name of dataset object")
                            }
                        ))
                    }
                ))
            }
        ))
    }
)

mBaseDependencies = api_experiments.model(
    'base-dependencies',
    {
        'imageRegistries': fields.List(fields.Nested(api_experiments.model(
            'base-dependencies-imageregistry',
            {
                'url': fields.String("Url of image registry"),
                'security': fields.Nested(api_experiments.model(
                    'base-dependencies-imageregistry-security',
                    {
                        'secretKeyRef': fields.Nested(api_experiments.model(
                            'base-dependencies-imageregistry-security-secret',
                            {
                                'name': fields.String(description="Name of Kubernetes Secret"),
                                'key': fields.String(description="Name of key containing authentication information",
                                                     default=".dockerconfigjson")
                            }
                        )),
                        'usernamePassword': fields.Nested(api_experiments.model(
                            'base-dependencies-imageregistry-security-value',
                            {
                                'username': fields.String(description="Username for container registry"),
                                'password': fields.String(description="Password for container registry"),
                            }
                        ))
                    }))
            }
        )))
    })

mBasePackage = api_experiments.model("base-package", {
    'name': fields.String(
        description="Unique name of base package in this virtual experiment entry. Defaults to \"main\"",
        default="main"),
    'source': fields.Nested(api_experiments.model(
        "base-source",
        {
            'git': fields.Nested(mBaseSourceGit),
            's3': fields.Nested(mBaseSourceS3),
            'dataset': fields.Nested(mBaseSourceDataset),
        })),
    'config': fields.Nested(api_experiments.model(
        'base-source-config',
        {
            'path': fields.String(description="Path relative to location specified by source of package"),
            'manifestPath': fields.String(
                description="The manifest path. relative to location specified by source of package"),
        }
    )),
    'dependencies': fields.List(fields.Nested(mBaseDependencies))
})

mBase = api_experiments.model("base", {
    "packages": fields.List(fields.Nested(mBasePackage))
})

mValueFrom = api_experiments.model("option-definition-from", {
    'value': fields.String(description="A constant value")
})

mOption = api_experiments.model("option", {
    'name': fields.String(description="Name of option"),
    'valueFrom': fields.Nested(mValueFrom)
})

mRuntimeConfig = api_experiments.model(
    'runtime-configuration',
    {
        'args': fields.List(fields.String(
            description="command line argument to runtime. "
                        "These arguments cannot be overridden inside executionOptions "
                        "or payload to start virtual experiment")),
        'orchestratorResources': fields.Nested(api_experiments.model(
            "orchestrator-resources",
            {
                'cpu': fields.String(
                    description="How much cpu to request for the elaunch container between (0.0, 1.0]"),
                'memory': fields.String(description="How much memory to request for the elaunch container specified in"
                                                    "kubernetes format e.g. 5Gi")
            }
        ))
    })

mPresets = api_experiments.model(
    "presets",
    {
        'variables': fields.List(fields.Nested(mOption),
                                 description="Values of virtual experiment variables."),
        'data': fields.List(fields.Nested(mOption),
                            description="Contents of data files, name is the filename inside the data directory."),
        'runtime': fields.List(fields.Nested(mRuntimeConfig)),
        'environmentVariables': fields.List(fields.Nested(
            mOption, description="Environment variables to inject in the runtime orchestrator process.")),
        'platform': fields.String("Name of virtual experiment platform to use. "
                                  "If provided, the platform name cannot be overridden inside executionOptions "
                                  "or payload to start virtual experiment.")
    })

# VV: Notice that this contains a LIST of options associated with a "name" (platform, variable, env-var, etc)
mOptionChoices = api_experiments.model("execution-option-choices", {
    'name': fields.String(description="Name of option"),
    'valueFrom': fields.List(fields.Nested(mValueFrom), description="Array of possible values "
                                                                    "(default is 1st item in array)")
})

mOptionChoicesData = api_experiments.model("execution-data-choices", {
    'name': fields.String(description="Filename in data directory"),
})

mExecutionOptions = api_experiments.model(
    "execution-options",
    {
        'variables': fields.List(fields.Nested(mOptionChoices)),
        'data': fields.List(fields.Nested(mOptionChoicesData)),
        'runtime': fields.Nested(api_experiments.model('execution-options-runtime', {
            'args': fields.List(fields.String(
                description="Command line argument to runtime. Cannot override arguments in presets.runtime.args"))
        })),
        'platform': fields.List(fields.String(description="Name of virtual experiment platform")),
    })

mMetadata = api_experiments.model(
    "registry-entry-metadata",
    {
        'package': fields.Nested(api_experiments.model("registry-entry-metadata-package", {
            'name': fields.String(description="Name of virtual experiment"),
            'tags': fields.List(fields.String(
                description="An optional user-provided string to associate with virtual experiment digest",
                default="latest")),
            'maintainer': fields.String(description="String containing comma separated e-mails of maintainers"),
            'license': fields.String(description="String containing the license"),
            'labels': fields.Raw(description="Labels in the form of key: value pairs"),
        })),
        'registry': fields.Nested(api_experiments.model(
            'registry-entry-metadata-registry', {
                'digest': fields.String(description="Unique digest identification of this experiment registry"),
                'tags': fields.List(fields.String(
                    description="This field is automatically managed by the registry")),
                'createdOn': fields.String(description="UTC Datetime that this entry was created, datetime "
                                                       "string format is %Y-%m-%dT%H%M%S.%f%z")
            },
            description="This field is automatically generated"))
    },
    description="Metadata related to this virtual experiment")

mParameterisation = api_experiments.model("registry-entry-parameterisation", {
    'presets': fields.Nested(
        mPresets,
        description="Specifies preset parameterisation which cannot be overridden by executionOptions or "
                    "virtual experiment payload"),
    'executionOptions': fields.Nested(
        mExecutionOptions,
        description="Specifies which virtual experiment options can be configured "
                    "by virtual experiment start payload"),
})

mVirtualExperiment = api_experiments.model("virtual-experiment", {
    'base': fields.Nested(mBase),
    'parameterisation': fields.Nested(mParameterisation),
    'metadata': fields.Nested(mMetadata),
})

mPackageHistory = api_experiments.model("package-history", {
    'tags': fields.List(fields.Nested(api_experiments.model("package-history-tag", {
        'tag': fields.String(description="The tag e.g. latest", example="latest"),
        'head': fields.String(description="The digest that the tag points to",
                              example="sha256x67357eeed694e4f954fda270d6adba7b2399823c1b30dd1513d9e8c08d919399"),
    }))),
    'untagged': fields.List(fields.Nested(api_experiments.model(
        "package-history-untagged", {
            'digest': fields.String(description="A digest which is no longer the head of any of the tags",
                                    example="sha256xd5067fc65aa4b569348caf347ca76983f6ecd2e45cc708410ba806f3835905ef")
        })))
})

mFileContent = api_experiments.model('file-content', {
    'filename': fields.String(required=True, description='Filename', example="field.conf"),
    'content': fields.String(required=True, description='Content of file', example='mole:capb,slampd,smlta '
                                                                                   'conc:4.2,1.4,0.5 '
                                                                                   'salt:2.8')
})

# mDataContent = api_experiments.model('data-content', {
#     'filename': fields.String(required=True, description='Filename or relative path to file in S3 bucket',
#                               example="pag_data.csv"),
#     'content': fields.String(required=True, description='Content of file (will be discarded if using S3 bucket)'
#                              , example='label	SMILES\n'
#                                        'mymol	O=S(=O)([O-])c1c(C(F)(F)F)cc(C(F)(F)F)cc1C(F)(F)F.Cc1cc(OC(C)(C)C)cc(C)c1[S+](c1ccccc1)c1ccccc1')
# })

mS3 = api_experiments.model('s3', {
    'dataset': fields.String(required=False,
                             description="Identifier of Dataset to use (uses https://github.com/datashim-io/datashim). "
                                         "If set, remaining S3 information will not be used"),
    'accessKeyID': fields.String(required=True, description='Access key id', default=''),
    'secretAccessKey': fields.String(required=True, description='Secret access key', default=''),
    'bucket': fields.String(required=True, description="Name of bucket", default=''),
    'endpoint': fields.String(
        required=True, default='',
        description="Endpoint URL (e.g. https://s3.eu-gb.cloud-object-storage.appdomain.cloud)"),
    'region': fields.String(required=False, description="Region (optional)", default=''),
})

mS3Credentials = api_experiments.model('s3-credentials', {
    'accessKeyID': fields.String(required=True, description='Access key id', default=''),
    'secretAccessKey': fields.String(required=True, description='Secret access key', default=''),
    'bucket': fields.String(required=True, description="Name of bucket", default=''),
    'endpoint': fields.String(
        required=True, default='',
        description="Endpoint URL (e.g. https://s3.eu-gb.cloud-object-storage.appdomain.cloud)"),
    'region': fields.String(required=False, description="Region (optional)", default=''),
})

mS3Store = api_experiments.model('s3-output', {
    'credentials': fields.Nested(mS3Credentials, default=None, required=True, description="S3 Configuration"),
    'bucketPath': fields.String(required=True, default="workflow_instances/",
                                description="The ST4SD runtime core will upload the workflow outputs under "
                                            "this path")
})

defaultContainerResourcesCpu = '1'
defaultContainerResourcesMemory = '500Mi'

mContainerResources = api_experiments.model('container-resources', {
    'cpu': fields.String(
        required=False, description="Cpu units to as a limit for the container (e.g. 0.1, 1)",
        example=defaultContainerResourcesCpu),
    'memory': fields.String(
        required=False, description="Memory defined as either bytes, mebibytes "
                                    "(e.g. 100Mi which is 104857600 bytes), or gibibytes (100Gi)",
        example=defaultContainerResourcesMemory
    )}
                                            )

mVolumeType = api_experiments.model("volume-type", {
    "persistentVolumeClaim": fields.String(
        required=False,
        description="(VOLUME_TYPE) name of PersistentVolumeClaim to mount, incompatible with other VOLUME_TYPE fields"),
    "configMap": fields.String(
        required=False,
        description="(VOLUME_TYPE) name of ConfigMap to mount, incompatible with other VOLUME_TYPE fields"),
    "dataset": fields.String(
        required=False,
        description="(VOLUME_TYPE) name of Dataset object to mount, incompatible with other VOLUME_TYPE fields"),
    "secret": fields.String(
        required=False,
        description="(VOLUME_TYPE) name of Secret object to mount, incompatible with other VOLUME_TYPE fields"),
})

mVolume = api_experiments.model("mount-volume", {
    "type": fields.Nested(mVolumeType, default={}, description="Volume type definition"),
    "applicationDependency": fields.String(
        required=False,
        description="Application dependency for which flow will create a link that points to the mount-path of this "
                    "volume (optional). This is expected to be an entry under the list of strings defined by the "
                    "application-dependencies.<platform-name> field within the FlowIR of the workflow."),
    # "readOnly": fields.Boolean(required=False,
    #                            description="Mounted read-only if true, read-write otherwise (false or unspecified). "
    #                                        "Defaults to True.", default=True),
    "subPath": fields.String(required=False,
                             description="Path within the volume from which the container's volume should be mounted. "
                                         "Defaults to \"\" (volume's root)."),
    # "mountPath": fields.String(
    #     required=False,
    #     description="Path within the container at which the volume should be mounted. Must not contain ':'. "
    #                 "Defaults to /input-volumes/<name of PersistentVolumeClaim OR ConfigMap OR Dataset>"),
})

mExperimentStart = api_experiments.model('experiment-start', {
    'inputs': fields.List(fields.Nested(mFileContent), required=False),
    'data': fields.List(fields.Nested(mFileContent), required=False),
    'volumes': fields.List(fields.Nested(mVolume)),
    'variables': fields.Raw(example={'startIndex': 10,
                                     "numberMolecules": 2,
                                     "functional": "B3LYPV3"}),
    'additionalOptions': fields.List(fields.String(required=False,
                                                   description='Additional options to elaunch.py',
                                                   example='--registerWorkflow=True')),
    'environmentVariables': fields.Raw(example={'RUNTIME_SECRET_TOKEN': "<token>"}),
    'orchestrator_resources': fields.Nested(mContainerResources, required=False,
                                            description="Hardware resource limits for the container that is hosting "
                                                        "the workflow orchestrator"),
    'metadata': fields.Raw(example={
        'exp-label': 'no-spaces-allowed',
    }),
    's3': fields.Nested(mS3, required=False,
                        description="S3 configuration (read the description of the Filename-Content pair model too)"),
    's3Store': fields.Nested(mS3Store, default={}, required=False,
                             description="Configuration to store outputs of workflow instance to a S3 bucket. "
                                         "Mutually exclusive with datasetStoreURI"),
    # For backwards compatibility support both datasetStoreURI and dlfStoreURI but only advertise datasetStoreURI
    'datasetStoreURI': fields.String(required=False,
                                     description="Dataset URI to store outputs (uses github.com/datashim-io/datashim) "
                                                 "i.e. dataset://<dataset-name>/path/in/dataset/to/upload/outputs/to. "
                                                 "Mutually exclusive with s3Store."),
    'runtimePolicy': fields.Nested(api_experiments.model('experiment-start-policy', {
        'name': fields.String(description='Nane of runtime policy'),
        'config': fields.Raw(example={}, description="Configuration options for runtime policy")
    }))
})

mLambdaExperimentStart = api_experiments.model('experiment-lambda-start', {
    'volumes': fields.List(fields.Nested(mVolume), required=False),
    'data': fields.List(fields.Nested(mFileContent), required=False),
    'scripts': fields.List(fields.Nested(mFileContent), required=False,
                           description="Scripts to placed under the `bin` directory of the experiment"),
    'variables': fields.Raw(example={'startIndex': 10,
                                     "numberMolecules": 2,
                                     "functional": "B3LYPV3"}),
    'additionalOptions': fields.List(fields.String(required=False,
                                                   description='Additional options to elaunch.py',
                                                   example='--registerWorkflow=True')),
    'environmentVariables': fields.Raw(example={'RUNTIME_SECRET_TOKEN': "<token>"}),
    'orchestrator_resources': fields.Nested(mContainerResources, required=False,
                                            description="Hardware resource limits for the container that is hosting "
                                                        "the workflow orchestrator"),
    'metadata': fields.Raw(example={
        'exp-label': 'no-spaces-allowed',
    }, required=False),
    # "validate_flowir": fields.Boolean(description="Whether to validate FlowIR before executing workflow, "
    #               "default is True", default=False, required=False),
    'lambdaFlowIR': fields.Raw(example={
        'flowir key': 'flowir value',
    }, description="JSON representation of FlowIR"),
    's3': fields.Nested(mS3, required=False,
                        description="S3 configuration (read the description of the Filename-Content pair model too)"),
    's3Store': fields.Nested(mS3Store, required=False,
                             description="Configuration to store outputs of workflow instance to a S3 bucket. "
                                         "Mutually exclusive with datasetStoreURI"),
    # For backwards compatibility support both datasetStoreURI and dlfStoreURI but only advertise datasetStoreURI
    'datasetStoreURI': fields.String(required=False,
                                     description="Dataset URI to store outputs (uses github.com/datashim-io/datashim) "
                                                 "i.e. dataset://<dataset-name>/path/in/dataset/to/upload/outputs/to. "
                                                 "Mutually exclusive with s3Store.")
})

############################ Instances ############################

api_instances = Namespace('instances', description='Instances of Experiments related operations')

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
experiment_status = api_instances.model('experiment-status', {
    'experiment-state': fields.String(required=True, example='running'),
    'stage-state': fields.String(required=True, example='running'),
    'stages': fields.List(fields.String),
    'current-stage': fields.String(),
    'meta': fields.Raw(),
    'exit-status': fields.String(),
    'total-progress': fields.Float(required=True),
    'stage-progress': fields.Float(required=True),
    'error-description': fields.String(),
})

experiment_instance = api_instances.model('experiment-instance', {
    'id': fields.String(required=True),
    'experiment': fields.Nested(mVirtualExperiment),
    'status': fields.Nested(experiment_status),
    'k8s-labels': fields.Raw(
        example={
            'rest-uid': 'ionisation-energy-z3u2c'
        }
    ),
    'outputs': fields.Raw(example={
        "AnionResults": {
            "creationtime": "2019-12-04 19:00:54.993316",
            "description": "Anion homo/lumo results",
            "filename": "energies.csv",
            "filepath": "/tmp/workdir/ionisation-energy-2019-12-04T181218.613966.instance/output/energies.csv",
            "final": "no",
            "production": "yes",
            "type": "csv",
            "version": "14"
        }
    })
})

############################ relationships ############################
api_relationships = Namespace('relationships', description='')

m_transform = api_relationships.model('transform', {})

m_relationship = api_relationships.model('relationship', {
    'identifier': fields.String(required=True, example='anionsmiles-to-optimizedgeometry'),
    'transform': fields.Nested(m_transform)
})

m_payload_synthesize = api_relationships.model('relationship', {
    'parameterisation': fields.Nested(mParameterisation)
})

############################ Queries ############################

api_query = Namespace('query', description='Query operations')
mQueryExperiment = api_query.model(
    'query-experiments',
    {
        'package': fields.Nested(
            api_query.model(
                'query-experiments-package',
                {
                    'definition': fields.Nested(mBasePackage)
                })),
        'relationship': fields.Nested(api_query.model(
            'query-experiments-relationship', {
                'identifier': fields.String(description="The identifier of the relationship"),
                'transform': fields.Nested(api_query.model('query-experiments-relationship-transform', {
                    'matchInputGraph': fields.Boolean(
                        description="Whether to query using the package of the inputGraph", default=False),
                    'matchOutputGraph': fields.Boolean(
                        description="Whether to query using the package of the inputGraph", default=False),
                }))
            })),
        'common': fields.Nested(api_query.model(
            'query-experiment-common', {
                'matchPackageVersion': fields.Boolean(
                    description="Whether to match the version of packages", default=False),
                'mustHaveOnePackage': fields.Boolean(
                    description="Match only parameterised virtual experiment packages with just one base package",
                    default=True)
            }
        ))
    }, description="Query database for experiments")
