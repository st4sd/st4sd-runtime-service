# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import base64
import json
import logging
import os
import tempfile
import time

from typing import Dict
from typing import List

import pytest
import yaml

import pydantic

import apis.db.exp_packages
import apis.models.common
import apis.models.constants
import apis.models.errors
import apis.models.virtual_experiment
import apis.runtime.package
import apis.storage
import tests.conftest

import apis.kernel.internal_experiments



package_from_files = tests.conftest.package_from_files

logger = logging.getLogger('trp')

def b64_str(value: str) -> str:
    return base64.b64encode(value.encode('utf-8')).decode('utf-8')


# VV: fixture in conftest
def test_no_override_namespace_by_package(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    ve_sum_numbers.parameterisation.executionOptions.runtime.args.append('--helloFromExecutionOptions')

    namespace_presets = apis.models.virtual_experiment.NamespacePresets.parse_obj({
        'runtime': {
            'args':
                ['--useMemoization=yes']
        }
    })
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'runtime': {
            'args':
                ['--hello']
        }
    })
    package = apis.runtime.package.NamedPackage(ve_sum_numbers, namespace_presets, payload_config)

    assert [x for x in package.runtime_args if x.startswith('--useMemoization=')] == ['--useMemoization=yes']
    assert '--hello' in package.runtime_args
    assert '--helloFromExecutionOptions' in package.runtime_args


def test_default_platform(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    ve_sum_numbers.parameterisation.presets.platform = None
    ve_sum_numbers.parameterisation.executionOptions.platform = ['artifactory', 'default']

    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions()
    package = apis.runtime.package.NamedPackage(ve_sum_numbers, namespace_presets, payload_config)

    assert package.platform == "artifactory"


def test_no_override_platform(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    ve_sum_numbers.parameterisation.presets.platform = "hello"

    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions()
    package = apis.runtime.package.NamedPackage(ve_sum_numbers, namespace_presets, payload_config)

    assert package.platform == "hello"


def test_override_platform_config(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions()

    payload_config.platform = "default"
    package = apis.runtime.package.NamedPackage(ve_sum_numbers, namespace_presets, payload_config)

    assert package.platform == "default"


# VV: fixture in conftest
def test_error_override_namespace_by_package_presets(
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets.parse_obj({
        'runtime': {
            'args':
                ['--useMemoization=yes']
        }
    })
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'runtime': {
            'args':
                ['--hello']
        }
    })

    ve_sum_numbers.parameterisation.presets.runtime.args = ['--useMemoization=no']

    with pytest.raises(apis.models.errors.InvalidElaunchParameterChoices) as e:
        apis.runtime.package.NamedPackage(ve_sum_numbers, namespace_presets, payload_config)

    assert e.value.name == "useMemoization"
    # VV: This is the value that we are trying to override - not the one we tried to use
    assert e.value.valid_values == [True]


def test_error_override_namespace_by_package_execution_options(
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage
):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets.parse_obj({
        'runtime': {
            'args':
                ['--useMemoization=yes']
        }
    })
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'runtime': {
            'args':
                ['--hello']
        }
    })

    ve_sum_numbers.parameterisation.executionOptions.runtime.args = ['--useMemoization=no']

    with pytest.raises(apis.models.errors.InvalidElaunchParameterChoices) as e:
        apis.runtime.package.NamedPackage(ve_sum_numbers, namespace_presets, payload_config)

    assert e.value.name == "useMemoization"
    # VV: This is the value that we are trying to override - not the one we tried to use
    assert e.value.valid_values == [True]


def test_error_override_resources_namespace_by_package_execution_options(
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage
):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    namespace_presets.runtime.resources.memory = "1Gi"
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions()

    ve_sum_numbers.parameterisation.executionOptions.runtime.resources.cpu = None
    ve_sum_numbers.parameterisation.executionOptions.runtime.resources.memory = "10Gi"

    with pytest.raises(apis.models.errors.OverrideResourcesError) as e:
        apis.runtime.package.NamedPackage(ve_sum_numbers, namespace_presets, payload_config)

    assert e.value.offending_key == "parameterisation.executionOptions.runtime.resources.memory"
    # VV: This is the value that we are trying to override - not the one we tried to use
    assert e.value.overridden_key == "namespace.runtime.resources.memory"


# VV: fixture in conftest
def test_error_override_namespace_by_payload(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets.parse_obj({
        'runtime': {
            'args':
                ['--useMemoization=yes']
        }
    })
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'runtime': {
            'args':
                ['--useMemoization=no']
        }
    })

    with pytest.raises(apis.models.errors.InvalidElaunchParameterChoices) as e:
        apis.runtime.package.NamedPackage(ve_sum_numbers, namespace_presets, payload_config)

    assert e.value.name == "useMemoization"
    # VV: This is the value that we are trying to override - not the one we tried to use
    assert e.value.valid_values == [True]


def test_error_override_package_preset_by_payload(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets.parse_obj({})
    ve_sum_numbers.parameterisation.presets.runtime.args = ['--useMemoization=yes']
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'runtime': {
            'args':
                ['--useMemoization=no']
        }
    })

    with pytest.raises(apis.models.errors.InvalidElaunchParameterChoices) as e:
        apis.runtime.package.NamedPackage(ve_sum_numbers, namespace_presets, payload_config)

    assert e.value.name == "useMemoization"
    # VV: This is the value that we are trying to override - not the one we tried to use
    assert e.value.valid_values == [True]


def test_error_override_package_execution_options_by_payload(
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets.parse_obj({})
    ve_sum_numbers.parameterisation.executionOptions.runtime.args = ['--useMemoization=yes']
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'runtime': {
            'args':
                ['--useMemoization=no']
        }
    })

    with pytest.raises(apis.models.errors.InvalidElaunchParameterChoices) as e:
        apis.runtime.package.NamedPackage(ve_sum_numbers, namespace_presets, payload_config)

    assert e.value.name == "useMemoization"
    # VV: This is the value that we are trying to override - not the one we tried to use
    assert e.value.valid_values == [True]


def test_decode_payload_volume(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets.parse_obj({})
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'runtime': {
            'args':
                ['--hello']
        },
        'volumes': [
            {
                'applicationDependency': 'dep-pvc',
                'type': {
                    'persistentVolumeClaim': {
                        'claimName': 'pvc',
                        'subPath': 'my custom subpath',
                        'readOnly': False,
                    }
                }
            },
            {
                'applicationDependency': 'dep-secret',
                'type': {
                    'secret': {
                        'name': 'secret'
                    }
                }
            },

        ]
    })
    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    volumes = package.volumes
    volume_mounts = package.volume_mounts

    assert len(volumes) == 2

    assert volumes['persistentVolumeClaim:pvc'].name == 'volume0'
    assert volumes['persistentVolumeClaim:pvc'].config == {
        'name': 'volume0',
        'persistentVolumeClaim': {
            'claimName': 'pvc'
        }
    }

    assert volumes['secret:secret'].name == 'volume1'
    assert volumes['secret:secret'].config == {
        'name': 'volume1',
        'secret': {
            'secretName': 'secret'
        }
    }

    assert len(volume_mounts) == 2

    assert volume_mounts[0].volume_name == 'volume0'
    assert volume_mounts[0].config == {
        'name': 'volume0',
        'mountPath': os.path.join(apis.runtime.package.ROOT_VOLUME_MOUNTS, 'pvc'),
        'readOnly': False,
        'subPath': 'my custom subpath'
    }

    assert volume_mounts[1].volume_name == "volume1"
    assert volume_mounts[1].config == {
        'name': 'volume1',
        'mountPath': os.path.join(apis.runtime.package.ROOT_VOLUME_MOUNTS, 'secret'),
        'readOnly': True,
    }

    args = package.runtime_args

    args.index(f'--applicationDependencySource=dep-pvc:{os.path.join(apis.runtime.package.ROOT_VOLUME_MOUNTS, "pvc")}')
    args.index(f'--applicationDependencySource=dep-secret:'
               f'{os.path.join(apis.runtime.package.ROOT_VOLUME_MOUNTS, "secret")}')


def test_environment_variables(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'environmentVariables': [
            {
                'name': 'hello',
                'value': 'world',
            }
        ]
    })
    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    env_vars = package.environment_variables_raw

    assert env_vars == {
        'hello': 'world'
    }

    assert package.construct_k8s_secret_env_vars("dummy")['data'] == {
        'hello': b64_str('world')
    }


def test_workflow_variables_unbounded(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'variables': [
            {
                'name': 'hello',
                'value': 'world',
            }
        ]
    })

    ve_sum_numbers.parameterisation.executionOptions.variables = [
        apis.models.common.OptionMany.parse_obj({
            'name': 'hello',
            # VV: missing/empty valueFrom/value keys means that this is an unbounded variable
        })
    ]

    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    workflow_variables = package.workflow_variables

    assert workflow_variables == {
        'hello': 'world'
    }

    str_variables = yaml.dump({
        'global': {
            'hello': 'world'
        }
    })

    assert package.embedded_files == {
        'input/st4sd-variables.yaml': str_variables
    }


def test_workflow_variables_not_in_choices(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'variables': [
            {
                'name': 'hello',
                'value': 'world',
            }
        ]
    })

    ve_sum_numbers.parameterisation.executionOptions.variables = [
        apis.models.common.OptionMany(name='hello', valueFrom=[
            apis.models.common.OptionValueFromMany(value="not-world")
        ])
    ]

    with pytest.raises(apis.models.errors.OverrideVariableError) as e:
        _ = apis.runtime.package.NamedPackage(
            ve_sum_numbers,
            namespace_presets,
            payload_config)

    assert e.value.name == "hello"
    assert e.value.value == "world"


def test_workflow_variables_in_choices(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'variables': [
            {
                'name': 'hello',
                'value': 'world',
            }
        ]
    })

    ve_sum_numbers.parameterisation.executionOptions.variables = [
        apis.models.common.OptionMany(name='hello', valueFrom=[
            apis.models.common.OptionValueFromMany(value="not-world"),
            apis.models.common.OptionValueFromMany(value="world")
        ])
    ]

    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    assert package.workflow_variables['hello'] == 'world'


def test_workflow_variables_default_choices(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions()

    ve_sum_numbers.parameterisation.executionOptions.variables = [
        apis.models.common.OptionMany(name='hello', valueFrom=[
            apis.models.common.OptionValueFromMany(value="not-world"),
            apis.models.common.OptionValueFromMany(value="world")
        ])
    ]

    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    assert package.workflow_variables['hello'] == 'not-world'


def test_workflow_variables_default_choices_from_value(
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions()

    ve_sum_numbers.parameterisation.executionOptions.variables = [
        apis.models.common.OptionMany(name='hello', value="not-world")
    ]

    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)
    assert package.workflow_variables['hello'] == 'not-world'


def test_workflow_variables_not_allowed(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'variables': [
            {
                'name': 'hello',
                'value': 'world',
            }
        ]
    })

    with pytest.raises(apis.models.errors.OverrideVariableError) as e:
        _ = apis.runtime.package.NamedPackage(
            ve_sum_numbers,
            namespace_presets,
            payload_config)

    assert e.value.name == "hello"
    assert e.value.value == "world"


def test_workflow_data_files(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'data': [
            {
                'name': 'hello',
                'value': 'world',
            }
        ]
    })

    ve_sum_numbers.parameterisation.presets.data = [
        apis.models.common.Option.parse_obj({'name': 'not-hello', 'value': 'default-not-hello'})
    ]

    ve_sum_numbers.parameterisation.executionOptions.data = [
        apis.models.common.OptionMany.parse_obj({'name': 'hello'}),
        apis.models.common.OptionMany.parse_obj({'name': 'not-hello'})
    ]

    ve_sum_numbers.parameterisation.presets.variables = [
        apis.models.common.Option.parse_obj({'name': 'variable', 'value': 'value'})
    ]

    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    str_yaml = yaml.dump({
        'global': {
            'variable': 'value',
        }
    })

    assert package.embedded_files == {
        'data/hello': 'world',
        'data/not-hello': 'default-not-hello',
        'input/st4sd-variables.yaml': str_yaml,
    }

    configmap = package.construct_k8s_configmap_embedded_files("something")

    # VV: We don't want any directory prefixes in here package.construct_k8s_workflow() asks k8s to mount these in
    # the appropriate place.
    assert configmap['data'] == {
        'hello': 'world',
        'not-hello': 'default-not-hello',
        'st4sd-variables.yaml': str_yaml,
    }


def test_workflow_data_no_override_presets(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'data': [
            {
                'name': 'hello',
                'value': 'world',
            }
        ]
    })

    ve_sum_numbers.parameterisation.presets.data = [
        apis.models.common.Option.parse_obj({'name': 'hello', 'value': 'default-hello'}),
        apis.models.common.Option.parse_obj({'name': 'not-hello', 'value': 'default-not-hello'})
    ]

    with pytest.raises(apis.models.errors.OverrideDataFilesError) as e:
        _ = apis.runtime.package.NamedPackage(
            ve_sum_numbers,
            namespace_presets,
            payload_config)

    assert e.value.names == ['hello']


def test_workflow_data_no_matching_execopts(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'data': [
            {
                'name': 'hello',
                'value': 'world',
            }
        ]
    })

    ve_sum_numbers.parameterisation.presets.data = [
        apis.models.common.Option.parse_obj({'name': 'not-hello', 'value': 'default-not-hello'})
    ]

    with pytest.raises(apis.models.errors.OverrideDataFilesError) as e:
        _ = apis.runtime.package.NamedPackage(
            ve_sum_numbers,
            namespace_presets,
            payload_config)

    # VV: This means parameterisation.executionOptions does not allow overriding data file "hello"
    assert e.value.names == ['hello']


def test_decode_experiment_start_payload():
    payload = {
        "variables": {
            "startIndex": 0,
            "numberMolecules": 1,
        },
        "additionalOptions": [
            "--useMemoisation=true"
        ],
        "orchestrator_resources": {
            "cpu": "1",
            "memory": "2Gi"
        },
        "data": [{
            "content": "contents",
            "filename": "pag_data.csv"
        }],
        'volumes': [
            {
                'applicationDependency': 'foo',
                'readOnly': False,
                'type': {
                    'persistentVolumeClaim': 'foo-pvc'
                },
            },
            {
                'applicationDependency': 'bar',
                'type': {
                    'dataset': 'bar-dataset'
                },
            }
        ]
    }

    old = apis.models.virtual_experiment.DeprecatedExperimentStartPayload.parse_obj(payload)
    config = apis.models.virtual_experiment.PayloadExecutionOptions.from_old_payload(old)

    assert config.runtime.args == ["--useMemoisation=true"]
    assert config.runtime.resources.cpu == "1"
    assert config.runtime.resources.memory == "2Gi"
    variables: Dict[str, apis.models.common.Option] = {x.name: x for x in config.variables}
    assert len(variables) == 2
    assert variables['startIndex'].my_contents == "0"
    assert variables['numberMolecules'].my_contents == "1"

    files_data: Dict[str, apis.models.common.Option] = {x.name: x for x in config.data}
    assert files_data['pag_data.csv'].my_contents == "contents"

    assert len(files_data) == 1

    volumes = config.volumes

    assert len(volumes) == 2

    print(volumes[0].dict())

    assert volumes[0].type.dict() == {
        'persistentVolumeClaim': {
            'claimName': 'foo-pvc',
            'readOnly': False,
        },
    }
    assert volumes[1].type.dict() == {
        'dataset': {
            'name': 'bar-dataset',
            'readOnly': True,
        }
    }


def test_package_workflow_git_plain(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({})
    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    spec = package.construct_k8s_workflow()

    print(yaml.dump(spec))

    # VV: Digest format is "${digest algorithm}x", find the first x (delimiter) and keep 6 chars after that
    digest = ve_sum_numbers.metadata.registry.digest.split('x', 1)[1][:6]
    constructed_name = f"{ve_sum_numbers.metadata.package.name}-{digest}"
    assert spec['metadata']['name'].rsplit('-', 1)[0] == constructed_name

    spec['metadata']['name'] = constructed_name

    # VV: This contains a timestamp
    instance_dir_name = f"{package.instance_name}.instance"

    assert spec == {'apiVersion': 'st4sd.ibm.com/v1alpha1',
                    'kind': 'Workflow',
                    'metadata': {'labels': {'rest-uid': package.rest_uid,
                                            'workflow': package.rest_uid,
                                            'st4sd-package-name': ve_sum_numbers.metadata.package.name,
                                            'st4sd-package-digest': ve_sum_numbers.metadata.registry.digest},
                                 'name': constructed_name},
                    'spec': {'additionalOptions': package.runtime_args,
                             'data': [],
                             'env': [{'name': 'INSTANCE_DIR_NAME',
                                      'value': instance_dir_name}],
                             'image': 'res-st4sd-team-official-base-docker-local.artifactory.'
                                      'swg-devops.com/st4sd-runtime-core',
                             'imagePullSecrets': [],
                             'inputs': [],
                             'package': {'branch': 'main',
                                         'url': 'https://github.ibm.com/st4sd/sum-numbers.git',
                                         'fromPath': None,
                                         'withManifest': None},
                             'resources': {'elaunchPrimary': {'cpu': '1', 'memory': '1Gi'}},
                             'variables': [],
                             'volumeMounts': [],
                             'volumes': [],
                             'workingVolume': {'name': 'working-volume',
                                               'persistentVolumeClaim': {'claimName': package.pvc_working_volume}}
                             }}



def test_package_workflow_s3_plain(
    ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage,
    output_dir: str,
):
    ve_sum_numbers = ve_sum_numbers.copy(deep=True)

    # VV: Echo the steps that the runtime-service follows to generate the base package for an internal experiment
    db_secrets = apis.db.secrets.DatabaseSecrets(db_path=os.path.join(output_dir, "secrets.db"))

    with db_secrets:
        db_secrets.secret_create(
            apis.db.secrets.Secret(
                name="default-s3-secret",
                data={
                    "S3_BUCKET": "a-bucket",
                    "S3_ENDPOINT": "https://my.endpoint",
                    "S3_ACCESS_KEY_ID": "access-key-id",
                    "S3_SECRET_ACCESS_KEY": "secret-access-key",
                    "S3_REGION": "region"
                }
            )
        )

    package_source = apis.kernel.internal_experiments.generate_s3_package_source_from_secret(
        secret_name="default-s3-secret",
        db_secrets=db_secrets
    )
    ve_sum_numbers = apis.kernel.internal_experiments.point_base_package_to_s3_storage(
        pvep=ve_sum_numbers,
        credentials=package_source.security.credentials,
        location=package_source.location,
    )

    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({})
    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    spec = package.construct_k8s_workflow()

    print(yaml.dump(spec))

    # VV: Digest format is "${digest algorithm}x", find the first x (delimiter) and keep 6 chars after that
    digest = ve_sum_numbers.metadata.registry.digest.split('x', 1)[1][:6]
    constructed_name = f"{ve_sum_numbers.metadata.package.name}-{digest}"
    assert spec['metadata']['name'].rsplit('-', 1)[0] == constructed_name

    spec['metadata']['name'] = constructed_name

    # VV: This contains a timestamp
    instance_dir_name = f"{package.instance_name}.instance"

    expected_package = {
        'fromPath': 'experiments/http-sum-numbers',
        's3': {
            'bucket': {'value': 'a-bucket'},
            'endpoint': {'value': 'https://my.endpoint'},
            'region': {'value': 'region'},
            'accessKeyID': {
                'valueFrom': {
                    'secretKeyRef': {
                        'key': 'S3_ACCESS_KEY_ID',
                        'name': 'default-s3-secret'
                    }
                }
            },
            'secretAccessKey': {
                'valueFrom': {
                    'secretKeyRef': {
                        'key': 'S3_SECRET_ACCESS_KEY',
                        'name': 'default-s3-secret'
                    }
                }
            }
        }
    }

    assert spec == {'apiVersion': 'st4sd.ibm.com/v1alpha1',
                    'kind': 'Workflow',
                    'metadata': {'labels': {'rest-uid': package.rest_uid,
                                            'workflow': package.rest_uid,
                                            'st4sd-package-name': ve_sum_numbers.metadata.package.name,
                                            'st4sd-package-digest': ve_sum_numbers.metadata.registry.digest},
                                 'name': constructed_name},
                    'spec': {'additionalOptions': package.runtime_args,
                             'data': [],
                             'env': [{'name': 'INSTANCE_DIR_NAME',
                                      'value': instance_dir_name}],
                             'image': 'res-st4sd-team-official-base-docker-local.artifactory.'
                                      'swg-devops.com/st4sd-runtime-core',
                             'imagePullSecrets': [],
                             'inputs': [],
                             'package': expected_package,
                             'resources': {'elaunchPrimary': {'cpu': '1', 'memory': '1Gi'}},
                             'variables': [],
                             'volumeMounts': [],
                             'volumes': [],
                             'workingVolume': {'name': 'working-volume',
                                               'persistentVolumeClaim': {'claimName': package.pvc_working_volume}}
                             }}


def test_package_workflow_dataset_plain(
        sum_numbers_ve_dataset: apis.models.virtual_experiment.ParameterisedPackage, mock_list_dataset
):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({})
    package = apis.runtime.package.NamedPackage(
        sum_numbers_ve_dataset,
        namespace_presets,
        payload_config)

    spec = package.construct_k8s_workflow()

    print(yaml.dump(spec))

    # VV: Digest format is "${digest algorithm}x", find the first x (delimiter) and keep 6 chars after that
    digest = sum_numbers_ve_dataset.metadata.registry.digest.split('x', 1)[1][:6]
    constructed_name = f"{sum_numbers_ve_dataset.metadata.package.name}-{digest}"
    assert spec['metadata']['name'].rsplit('-', 1)[0] == constructed_name

    spec['metadata']['name'] = constructed_name

    # VV: This contains a timestamp
    instance_dir_name = f"{package.instance_name}.instance"

    assert spec == {
        'apiVersion': 'st4sd.ibm.com/v1alpha1',
        'kind': 'Workflow',
        'metadata': {'labels': {'rest-uid': package.rest_uid,
                                'workflow': package.rest_uid,
                                'st4sd-package-name': sum_numbers_ve_dataset.metadata.package.name,
                                'st4sd-package-digest': sum_numbers_ve_dataset.metadata.registry.digest},
                     'name': constructed_name},
        'spec': {'additionalOptions': package.runtime_args,
                 'data': [],
                 'env': [{'name': 'INSTANCE_DIR_NAME',
                          'value': instance_dir_name}],
                 'image': 'res-st4sd-team-official-base-docker-local.artifactory.'
                          'swg-devops.com/st4sd-runtime-core',
                 'imagePullSecrets': [],
                 'inputs': [],
                 'package': {'fromPath': "/tmp/st4sd-workflow-definitions/main", 'withManifest': None},
                 'resources': {'elaunchPrimary': {'cpu': '1', 'memory': '1Gi'}},
                 'variables': [],
                 'volumeMounts': [
                     {'mountPath': '/tmp/st4sd-workflow-definitions/main', 'name': 'base-main'}
                 ],
                 'volumes': [
                     {
                         'name': 'base-main',
                         'persistentVolumeClaim': {'claimName': 'my-test'}}
                 ],
                 'workingVolume': {'name': 'working-volume',
                                   'persistentVolumeClaim': {'claimName': package.pvc_working_volume}}
                 }}


def test_package_workflow_git_embedded_data(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'data': [
            {
                'name': 'cat_me.txt',
                'value': 'custom message',
            }
        ]
    })

    ve_sum_numbers.parameterisation.executionOptions.data = [
        apis.models.common.OptionMany.parse_obj({
            'name': 'cat_me.txt'
        })
    ]

    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    spec = package.construct_k8s_workflow()

    print(yaml.dump(spec))

    # VV: Digest format is "${digest algorithm}x", find the first x (delimiter) and keep 6 chars after that
    digest = ve_sum_numbers.metadata.registry.digest.split('x', 1)[1][:6]
    constructed_name = f"{ve_sum_numbers.metadata.package.name}-{digest}"
    assert spec['metadata']['name'].rsplit('-', 1)[0] == constructed_name

    spec['metadata']['name'] = constructed_name

    # VV: This contains a timestamp
    instance_dir_name = f"{package.instance_name}.instance"

    configmap = package.construct_k8s_configmap_embedded_files('hello')
    cm_name = configmap['metadata']['name']

    assert spec == {'apiVersion': 'st4sd.ibm.com/v1alpha1',
                    'kind': 'Workflow',
                    'metadata': {'labels': {'rest-uid': package.rest_uid,
                                            'workflow': package.rest_uid,
                                            'st4sd-package-name': ve_sum_numbers.metadata.package.name,
                                            'st4sd-package-digest': ve_sum_numbers.metadata.registry.digest},
                                 'name': constructed_name},
                    'spec': {'additionalOptions': package.runtime_args,
                             'data': ['/tmp/st4sd-embedded/data/cat_me.txt'],
                             'env': [{'name': 'INSTANCE_DIR_NAME',
                                      'value': instance_dir_name}],
                             'image': 'res-st4sd-team-official-base-docker-local.artifactory.'
                                      'swg-devops.com/st4sd-runtime-core',
                             'imagePullSecrets': [],
                             'inputs': [],
                             'package': {'branch': 'main',
                                         'url': 'https://github.ibm.com/st4sd/sum-numbers.git',
                                         'fromPath': None,
                                         'withManifest': None},
                             'resources': {'elaunchPrimary': {'cpu': '1', 'memory': '1Gi'}},
                             'variables': [],
                             'volumeMounts': [{
                                 'name': 'embedded-files',
                                 'mountPath': apis.runtime.package.ROOT_EMBEDDED_FILES
                             }],
                             'volumes': [
                                 {
                                     'name': 'embedded-files',
                                     'configMap': {
                                         'name': cm_name,
                                         'items': [
                                             {
                                                 'key': 'cat_me.txt',
                                                 'path': 'data/cat_me.txt'
                                             }
                                         ],
                                     }
                                 }
                             ],
                             'workingVolume': {'name': 'working-volume',
                                               'persistentVolumeClaim': {'claimName': package.pvc_working_volume}}
                             }}


def test_package_workflow_git_commitid(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({})

    base = ve_sum_numbers.base.packages[0]

    git_source: apis.models.virtual_experiment.BasePackageSourceGit = base.source.git

    git_source.location = apis.models.virtual_experiment.SourceGitLocation(
        url=git_source.location.url,
        branch=None, tag=None, commit="this is a commit id")
    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    spec = package.construct_k8s_workflow()

    print(yaml.dump(spec))

    # VV: Digest format is "${digest algorithm}x", find the first x (delimiter) and keep 6 chars after that
    digest = ve_sum_numbers.metadata.registry.digest.split('x', 1)[1][:6]
    constructed_name = f"{ve_sum_numbers.metadata.package.name}-{digest}"
    assert spec['metadata']['name'].rsplit('-', 1)[0] == constructed_name

    spec['metadata']['name'] = constructed_name

    # VV: This contains a timestamp
    instance_dir_name = f"{package.instance_name}.instance"

    assert spec == {'apiVersion': 'st4sd.ibm.com/v1alpha1',
                    'kind': 'Workflow',
                    'metadata': {'labels': {'rest-uid': package.rest_uid,
                                            'workflow': package.rest_uid,
                                            'st4sd-package-name': ve_sum_numbers.metadata.package.name,
                                            'st4sd-package-digest': ve_sum_numbers.metadata.registry.digest},
                                 'name': constructed_name},
                    'spec': {'additionalOptions': package.runtime_args,
                             'data': [],
                             'env': [{'name': 'INSTANCE_DIR_NAME',
                                      'value': instance_dir_name}],
                             'image': 'res-st4sd-team-official-base-docker-local.artifactory.'
                                      'swg-devops.com/st4sd-runtime-core',
                             'imagePullSecrets': [],
                             'inputs': [],
                             'package': {'commitId': 'this is a commit id',
                                         'url': 'https://github.ibm.com/st4sd/sum-numbers.git',
                                         'fromPath': None,
                                         'withManifest': None},
                             'resources': {'elaunchPrimary': {'cpu': '1', 'memory': '1Gi'}},
                             'variables': [],
                             'volumeMounts': [],
                             'volumes': [],
                             'workingVolume': {'name': 'working-volume',
                                               'persistentVolumeClaim': {'claimName': package.pvc_working_volume}}
                             }}


def test_package_workflow_git_data_s3(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'data': [
            {
                'name': 'cat_me.txt',
                'valueFrom': {
                    's3Ref': {
                        "path": "some/path/cat_me.txt"
                    }
                }
            }
        ],
        'security': {
            's3Input': {
                'valueFrom': {
                    's3Ref': {
                        "accessKeyID": "accessKeyID",
                        "secretAccessKey": "secretAccessKey",
                        "endpoint": "endpoint",
                        "bucket": "bucket",
                    }
                }
            }
        }
    })

    ve_sum_numbers.parameterisation.executionOptions.data = [
        apis.models.common.OptionMany.parse_obj({
            'name': 'cat_me.txt'
        })
    ]

    extra_opts = apis.runtime.package.PackageExtraOptions()

    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config,
        extra_opts)

    spec = package.construct_k8s_workflow()

    print(yaml.dump(spec))

    # VV: Digest format is "${digest algorithm}x", find the first x (delimiter) and keep 6 chars after that
    digest = ve_sum_numbers.metadata.registry.digest.split('x', 1)[1][:6]
    constructed_name = f"{ve_sum_numbers.metadata.package.name}-{digest}"
    assert spec['metadata']['name'].rsplit('-', 1)[0] == constructed_name

    spec['metadata']['name'] = constructed_name

    # VV: This contains a timestamp
    instance_dir_name = f"{package.instance_name}.instance"

    configmap = package.construct_k8s_configmap_embedded_files('hello')

    assert spec == {'apiVersion': 'st4sd.ibm.com/v1alpha1',
                    'kind': 'Workflow',
                    'metadata': {'labels': {'rest-uid': package.rest_uid,
                                            'workflow': package.rest_uid,
                                            'st4sd-package-name': ve_sum_numbers.metadata.package.name,
                                            'st4sd-package-digest': ve_sum_numbers.metadata.registry.digest},
                                 'name': constructed_name},
                    'spec': {'additionalOptions': package.runtime_args,
                             'data': [os.path.join(apis.runtime.package.ROOT_S3_FILES, 'data', 'some/path/cat_me.txt')],
                             'env': [{'name': 'INSTANCE_DIR_NAME',
                                      'value': instance_dir_name}],
                             'image': 'res-st4sd-team-official-base-docker-local.artifactory.'
                                      'swg-devops.com/st4sd-runtime-core',
                             'imagePullSecrets': [],
                             'inputs': [],
                             'package': {'branch': 'main',
                                         'url': 'https://github.ibm.com/st4sd/sum-numbers.git',
                                         'fromPath': None,
                                         'withManifest': None},
                             'resources': {'elaunchPrimary': {'cpu': '1', 'memory': '1Gi'}},
                             'variables': [],
                             'volumeMounts': [],
                             's3BucketInput': {
                                 x: {'valueFrom': {
                                     'secretKeyRef': {
                                         'name': f'env-{package.rest_uid}',
                                         'key': f"ST4SD_S3_IN_{x.upper()}",
                                     }
                                 }} for x in ['accessKeyID', 'secretAccessKey', 'bucket', 'endpoint']
                             },
                             's3FetchFilesImage': extra_opts.image_st4sd_runtime_k8s_input_s3,
                             'volumes': [],
                             'workingVolume': {'name': 'working-volume',
                                               'persistentVolumeClaim': {'claimName': package.pvc_working_volume}}
                             }}

    secret = package.construct_k8s_secret_env_vars('hello')

    # VV: We store the actual credentials in a secret!
    assert secret['data'] == {
        'ST4SD_S3_IN_ACCESSKEYID': b64_str('accessKeyID'),
        'ST4SD_S3_IN_BUCKET': b64_str('bucket'),
        'ST4SD_S3_IN_ENDPOINT': b64_str('endpoint'),
        'ST4SD_S3_IN_SECRETACCESSKEY': b64_str('secretAccessKey')
    }


def test_package_workflow_git_data_s3_with_deprecated_payload(
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage
):
    experiment_start_obj = {
        "s3": {
            "accessKeyID": "accessKeyID",
            "secretAccessKey": "secretAccessKey",
            "endpoint": "endpoint",
            "bucket": "bucket",
        },
        "data": [{
            # The contents of this data fill will be read from S3
            "filename": "some/path/cat_me.txt"
        }],
        'inputs': [
            {"filename": "hello.txt", "content": "embed me"}
        ]
    }
    deprecated = apis.models.virtual_experiment.DeprecatedExperimentStartPayload.parse_obj(experiment_start_obj)
    from_deprecated = apis.models.virtual_experiment.PayloadExecutionOptions.from_old_payload(deprecated)

    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'data': [
            {
                'name': 'cat_me.txt',
                'valueFrom': {
                    # VV: We expect the bucket name to be in security.s3Input
                    's3Ref': {
                        "path": "some/path/cat_me.txt",
                    }
                }
            }
        ],
        'inputs': [{
            'name': 'hello.txt',
            'value': 'embed me',
        }],
        'security': {
            's3Input': {
                'valueFrom': {
                    's3Ref': {
                        'accessKeyID': "accessKeyID",
                        'secretAccessKey': "secretAccessKey",
                        'endpoint': "endpoint",
                        'bucket': "bucket"
                    }
                }
            }
        }
    })

    assert len(from_deprecated.data) == len(payload_config.data)
    assert from_deprecated.data[0].dict() == payload_config.data[0].dict()
    assert from_deprecated.security.s3Input.my_contents.dict() == payload_config.security.s3Input.my_contents.dict()

    assert len(from_deprecated.inputs) == 1
    assert from_deprecated.inputs[0].dict() == payload_config.inputs[0].dict()


def test_package_payload_extract_user_metadata():
    experiment_start_obj = {
        'metadata': {
            'hello': 'world',
        }
    }
    deprecated = apis.models.virtual_experiment.DeprecatedExperimentStartPayload.parse_obj(experiment_start_obj)
    from_deprecated = apis.models.virtual_experiment.PayloadExecutionOptions.from_old_payload(deprecated)

    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'userMetadata': [
            {'name': 'hello', 'value': 'world'}
        ],
        'runtime': {
            'resources': {
                'cpu': '1',
                'memory': '500Mi',
            }
        }
    })

    assert from_deprecated.userMetadata == [
        apis.models.common.Option(name='hello', value='world')
    ]
    assert from_deprecated.dict() == payload_config.dict()


def test_package_hide_s3_input_creds(
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage
):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'security': {
            's3Input': {
                'valueFrom': {
                    's3Ref': {
                        "accessKeyID": "accessKeyID",
                        "secretAccessKey": "secretAccessKey",
                        "endpoint": "endpoint",
                        "bucket": "bucket",
                    }
                }
            }
        }
    })

    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    secret = package.construct_k8s_secret_env_vars('hello')

    assert secret['data'] == {
        'ST4SD_S3_IN_ACCESSKEYID': b64_str('accessKeyID'),
        'ST4SD_S3_IN_BUCKET': b64_str('bucket'),
        'ST4SD_S3_IN_ENDPOINT': b64_str('endpoint'),
        'ST4SD_S3_IN_SECRETACCESSKEY': b64_str('secretAccessKey')
    }


def test_package_hide_s3_output_creds(
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage
):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'security': {
            's3Output': {
                'valueFrom': {
                    's3Ref': {
                        "accessKeyID": "accessKeyID",
                        "secretAccessKey": "secretAccessKey",
                        "endpoint": "endpoint",
                        'bucket': 'my-bucket',
                    }
                }
            }
        }
    })

    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    secret = package.construct_k8s_secret_env_vars('hello')

    assert secret['data'] == {
        'S3_ACCESS_KEY_ID': b64_str('accessKeyID'),
        'S3_END_POINT': b64_str('endpoint'),
        'S3_SECRET_ACCESS_KEY': b64_str('secretAccessKey')
    }


def test_package_store_outputs_s3(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        's3Output': {
            'valueFrom':
                {
                    's3Ref': {
                        'path': 'location'
                    }
                }
        },
        'security': {
            's3Output': {
                'valueFrom': {
                    's3Ref': {
                        "accessKeyID": "accessKeyID",
                        "secretAccessKey": "secretAccessKey",
                        'bucket': 'my-bucket',
                        "endpoint": "endpoint",
                    }
                }
            }
        }
    })

    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    runtime_args = package.runtime_args

    _ = runtime_args.index("--s3AuthWithEnvVars")
    _ = runtime_args.index("--s3StoreToURI=s3://my-bucket/location")


def test_read_s3_files_rename(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()

    ve_sum_numbers.metadata.registry.inputs.append(apis.models.common.Option(name="renamed.txt"))
    old = apis.models.virtual_experiment.DeprecatedExperimentStartPayload.parse_obj({
        'inputs': [
            {
                'sourceFilename': '/dir/file.txt',
                'targetFilename': 'renamed.txt'
            }
        ],
        's3': {
            'accessKeyID': "accessKeyID",
            'secretAccessKey': "secretAccessKey",
            'bucket': "bucket",
            'endpoint': "endpoint"
        }
    })
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.from_old_payload(old)
    package = apis.runtime.package.NamedPackage(ve_sum_numbers, namespace_presets, payload_config)
    wf = package.construct_k8s_workflow()

    assert payload_config.inputs[0].name == "renamed.txt"

    # VV: st4sd-runtime-k8s will ask st4sd-runtime-k8s-input-s3 to download
    # the file "/dir/file.txt" from the S3 bucket and store it under "/tmp/s3-root-dir/input/".
    # Then it will configure elaunch.py (st4sd-runtime-core) to:
    # 1. load the input file /tmp/s3-root-dir/input/dir/file.txt
    # 2. store it as ${INSTANCE_DIR}/input/renamed.txt
    assert wf['spec']['inputs'] == ["/tmp/s3-root-dir/input/dir/file.txt:renamed.txt"]


def test_read_dataset_files_rename(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()

    ve_sum_numbers.metadata.registry.inputs.append(apis.models.common.Option(name="renamed.txt"))

    old = apis.models.virtual_experiment.DeprecatedExperimentStartPayload.parse_obj({
        'inputs': [
            {
                'sourceFilename': '/dir/file.txt',
                'targetFilename': 'renamed.txt'
            }
        ],
        's3': {
            'dataset': "ds-foo"
        }
    })
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.from_old_payload(old)
    package = apis.runtime.package.NamedPackage(ve_sum_numbers, namespace_presets, payload_config)
    wf = package.construct_k8s_workflow()

    assert payload_config.inputs[0].name == "renamed.txt"

    # VV: st4sd-runtime-k8s will ask st4sd-runtime-k8s-input-s3 to download
    # the file "/dir/file.txt" from the S3 bucket that the dataset object wraps.
    # st4sd-runtime-k8s-input-s3 will store the file under "/tmp/s3-root-dir/input/".
    # Then it will configure elaunch.py (st4sd-runtime-core) to:
    # 1. load the input file /tmp/s3-root-dir/input/dir/file.txt
    # 2. store it as ${INSTANCE_DIR}/input/renamed.txt
    # This process is similar to test_read_s3_files_rename(), the difference is that
    # st4sd-runtime-k8s will extract the s3 credentials from the Dataset object.
    # i.e. it is not st4sd-runtime-service to provide the S3 credentials in the workflow definition.

    assert wf['spec']['inputs'] == ["/tmp/s3-root-dir/input/dir/file.txt:renamed.txt"]


def test_invalid_payload_s3_missing_target_filename():
    with pytest.raises(pydantic.error_wrappers.ValidationError):
        old = apis.models.virtual_experiment.DeprecatedExperimentStartPayload.parse_obj({
            'inputs': [
                {
                    'sourceFilename': '/dir/file.txt',
                }
            ]
        })


def test_invalid_payload_s3_both_filename_and_target_filename():
    with pytest.raises(pydantic.error_wrappers.ValidationError):
        old = apis.models.virtual_experiment.DeprecatedExperimentStartPayload.parse_obj({
            'inputs': [
                {
                    'sourceFilename': '/dir/file.txt',
                    'filename': "other-file.txt"
                }
            ]
        })


def test_read_s3_files_simple(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()

    ve_sum_numbers.metadata.registry.inputs.append(apis.models.common.Option(name="file.txt"))

    old = apis.models.virtual_experiment.DeprecatedExperimentStartPayload.parse_obj({
        'inputs': [
            {'filename': '/dir/file.txt'}
        ],
        's3': {
            'accessKeyID': "accessKeyID",
            'secretAccessKey': "secretAccessKey",
            'bucket': "bucket",
            'endpoint': "endpoint"
        }
    })
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.from_old_payload(old)
    package = apis.runtime.package.NamedPackage(ve_sum_numbers, namespace_presets, payload_config)
    wf = package.construct_k8s_workflow()

    assert payload_config.inputs[0].name == "file.txt"

    assert wf['spec']['inputs'] == ["/tmp/s3-root-dir/input/dir/file.txt"]


def test_experiment_id_usermetadata(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions()

    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    runtime_args = package.runtime_args

    experiment_id = apis.models.common.PackageIdentifier.from_parts(
        package_name=ve_sum_numbers.metadata.package.name,
        tag=None,
        digest=ve_sum_numbers.metadata.registry.digest).identifier

    _ = runtime_args.index(f"-mexperiment-id:{experiment_id}")


def test_package_with_user_metadata(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        'userMetadata': [
            {'name': 'hello', 'value': 'world'}
        ]
    })

    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    package.runtime_args.index('-mhello:world')


def test_packate_inject_generated_user_metadata(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions()

    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    labels = {
        'rest-uid': package.rest_uid,
        'workflow': package.rest_uid,
        'st4sd-package-name': ve_sum_numbers.metadata.package.name,
        'st4sd-package-digest': ve_sum_numbers.metadata.registry.digest,
    }

    args = package.runtime_args

    print(args)

    for name, value in labels.items():
        args.index(f'-m{name}:{value}')


def test_package_store_outputs_s3_from_deprecated(
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage
):
    experiment_start_obj = {
        's3Store': {
            'credentials': {
                'accessKeyID': "accessKeyID",
                'secretAccessKey': "secretAccessKey",
                'endpoint': "endpoint",
                'region': "region",
                'bucket': "my-bucket"
            },
            'bucketPath': "location"
        },
    }
    deprecated = apis.models.virtual_experiment.DeprecatedExperimentStartPayload.parse_obj(experiment_start_obj)
    from_deprecated = apis.models.virtual_experiment.PayloadExecutionOptions.from_old_payload(deprecated)

    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        's3Output': {
            'valueFrom':
                {
                    's3Ref': {
                        'path': "location"
                    }
                }
        },
        'security': {
            's3Output': {
                'valueFrom': {
                    's3Ref': {
                        'accessKeyID': "accessKeyID",
                        'secretAccessKey': "secretAccessKey",
                        'endpoint': "endpoint",
                        'region': "region",
                        'bucket': "my-bucket",
                    }
                }
            }
        }
    })

    print("From Deprecated", from_deprecated.security.s3Output.my_contents.dict())
    print("From PayloadConfig", payload_config.security.s3Output.my_contents.dict())

    assert from_deprecated.security.s3Output.my_contents.dict() == payload_config.security.s3Output.my_contents.dict()


def test_package_store_outputs_dataset(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    # VV: Unfortunately we cannot FULLY test the "datasetRef" approach because that involves querying
    # Kubernetes for a Dataset object and then extracting its S3 credentials to convert it into 's3Ref'

    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.parse_obj({
        's3Output': {
            'valueFrom':
                {
                    'datasetRef': {
                        'name': 'replace me',
                        'path': 'location',
                    }
                }
        },
        'security': {
            's3Output': {
                'valueFrom': {
                    'datasetRef': {
                        "name": 'replace me'
                    }
                }
            }
        }
    })

    # VV: this happens inside apis.experiments.ExperimentStart.post()
    s3_security = apis.models.common.OptionFromS3Values.parse_obj({
        "accessKeyID": "accessKeyID",
        "secretAccessKey": "secretAccessKey",
        "endpoint": "endpoint",
        "bucket": "my-bucket"
    })

    payload_config.configure_output_s3('location', s3_security)

    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    runtime_args = package.runtime_args

    print(runtime_args)

    _ = runtime_args.index("--s3AuthWithEnvVars")
    _ = runtime_args.index("--s3StoreToURI=s3://my-bucket/location")

    secret = package.construct_k8s_secret_env_vars('hello')

    assert secret['data'] == {
        'S3_ACCESS_KEY_ID': b64_str('accessKeyID'),
        'S3_END_POINT': b64_str('endpoint'),
        'S3_SECRET_ACCESS_KEY': b64_str('secretAccessKey')
    }

    wf = package.construct_k8s_workflow()

    keys = ['S3_ACCESS_KEY_ID', 'S3_END_POINT', 'S3_SECRET_ACCESS_KEY']
    envs = sorted([x for x in wf['spec']['env'] if x['name'] in keys], key=lambda x: x['name'])
    secret_name = f'env-{package.rest_uid}'

    assert envs == [
        {
            'name': what,
            'valueFrom': {
                'secretKeyRef': {
                    'name': secret_name,
                    'key': what,
                }
            }
        } for what in sorted(keys)
    ]


def test_derived_package(derived_ve: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions()
    package = apis.runtime.package.NamedPackage(derived_ve, namespace_presets, payload_config)

    spec = package.construct_k8s_workflow()

    print(yaml.dump(spec))

    # VV: Digest format is "${digest algorithm}x", find the first x (delimiter) and keep 6 chars after that
    digest = derived_ve.metadata.registry.digest.split('x', 1)[1][:6]
    constructed_name = f"{derived_ve.metadata.package.name}-{digest}"
    assert spec['metadata']['name'].rsplit('-', 1)[0] == constructed_name

    spec['metadata']['name'] = constructed_name

    # VV: This contains a timestamp
    instance_dir_name = f"{package.instance_name}.instance"

    from_path = os.path.join(apis.models.constants.ROOT_STORE_DERIVED_PACKAGES,
                             derived_ve.metadata.package.name,
                             derived_ve.get_packages_identifier())

    assert spec == {'apiVersion': 'st4sd.ibm.com/v1alpha1',
                    'kind': 'Workflow',
                    'metadata': {'labels': {'rest-uid': package.rest_uid,
                                            'workflow': package.rest_uid,
                                            'st4sd-package-name': derived_ve.metadata.package.name,
                                            'st4sd-package-digest': derived_ve.metadata.registry.digest},
                                 'name': constructed_name},
                    'spec': {'additionalOptions': package.runtime_args,
                             'data': [],
                             'env': [{'name': 'INSTANCE_DIR_NAME',
                                      'value': instance_dir_name}],
                             'image': 'res-st4sd-team-official-base-docker-local.artifactory.swg-devops.com/st4sd-runtime-core',
                             'imagePullSecrets': [],
                             'inputs': [],
                             'package': {'fromPath': from_path},
                             'resources': {'elaunchPrimary': {'cpu': '1', 'memory': '1Gi'}},
                             'variables': [],
                             'volumeMounts': [],
                             'volumes': [],
                             'workingVolume': {'name': 'working-volume',
                                               'persistentVolumeClaim': {'claimName': package.pvc_working_volume}}
                             }}


def test_package_deprecated_start_with_variables(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    experiment_start_obj = {
        "variables": {
            "numberOfPoints": 1,
        },
    }
    deprecated = apis.models.virtual_experiment.DeprecatedExperimentStartPayload.parse_obj(experiment_start_obj)
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.from_old_payload(deprecated)
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()

    ve_sum_numbers.parameterisation.executionOptions.variables.append(
        apis.models.common.OptionMany(name="numberOfPoints", value="3")
    )

    package = apis.runtime.package.NamedPackage(
        ve_sum_numbers,
        namespace_presets,
        payload_config)

    spec = package.construct_k8s_workflow()

    assert spec['spec']['variables'] == ['/tmp/st4sd-embedded/input/st4sd-variables.yaml']
    assert package.workflow_variables == {'numberOfPoints': '1'}


def test_validate_adapt_and_store_experiment_to_database(
        flowir_psi4: str,
        ve_psi4: apis.models.virtual_experiment.ParameterisedPackage,
        output_dir: str
):
    pkg_location = package_from_files(
        location=os.path.join(output_dir, "psi4"),
        files={
            'bin/aggregate_energies.py': 'expensive',
            'bin/optimize_ff.py': 'expensive',
            'bin/optimize_psi4.py': 'expensive',

            'conf/flowir_package.yaml': flowir_psi4,
        }
    )

    StorageMetadata = apis.models.virtual_experiment.StorageMetadata
    collection = apis.storage.PackageMetadataCollection({
        ve_psi4.base.packages[0].name: StorageMetadata.from_config(
            prefix_paths=pkg_location, config=apis.models.virtual_experiment.BasePackageConfig(),
        )})

    original_created_on = ve_psi4.metadata.registry.createdOn

    # VV: Ensure that validate_parameterised_package() updates createdOn, and digest
    time.sleep(0.1)
    ve_psi4.metadata.registry.digest = "invalid"

    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        db = apis.db.exp_packages.DatabaseExperiments(f.name)

        metadata = apis.runtime.package.access_and_validate_virtual_experiment_packages(ve_psi4, collection, db)
        apis.runtime.package.validate_parameterised_package(ve=ve_psi4, metadata=metadata)

        with db:
            db.push_new_entry(ve_psi4)

        with db:
            doc = db.query()
            assert len(doc) == 1

        x = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(doc[0])
        assert x.metadata.registry.createdOn > original_created_on
        assert x.metadata.registry.digest == x.to_digestable().to_digest()
        assert x.metadata.registry.digest != "invalid"

        ve_psi4.base.packages[0].source.git.location.branch = "other_branch"
        ve_psi4.base.packages[0].source.git.version = "totally-new-version"

        metadata = apis.runtime.package.access_and_validate_virtual_experiment_packages(ve_psi4, collection, db)
        apis.runtime.package.validate_parameterised_package(ve=ve_psi4, metadata=metadata)
        with db:
            db.push_new_entry(ve_psi4)

        with db:
            many_docs = db.query()
            assert len(many_docs) == 2

            ql = db.construct_query(package_name=ve_psi4.metadata.package.name, registry_tag="latest")
            latest = db.query(ql)

        assert len(latest) == 1
        latest = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(latest[0])

        assert latest.metadata.registry.digest != "invalid"
        assert latest.base.packages[0].source.git.location.branch == "other_branch"
        assert latest.base.packages[0].source.git.version == "totally-new-version"

        assert latest.metadata.registry.digest != x.metadata.registry.digest
        assert latest.metadata.registry.createdOn > x.metadata.registry.createdOn

        assert latest.registry_created_on > x.registry_created_on


def test_extract_all_variables_during_validate(
        flowir_psi4: str,
        ve_psi4: apis.models.virtual_experiment.ParameterisedPackage,
        output_dir: str
):
    # VV: inject a brand new variable into the mix which.
    # It exists in 3 platforms, `default`, `openshift`, and `dont-care`. The registry should only display its values
    # for the `default` and `openshift` platforms.

    raw_yaml = yaml.load(flowir_psi4, yaml.FullLoader)

    raw_yaml['variables']['default']['global']['overrideme'] = 'default'
    raw_yaml['variables']['openshift']['global']['overrideme'] = 'openshift'
    raw_yaml['variables']['dont-care'] = {
        'global': {
            'overrideme': 'dont-care'
        }
    }

    flowir_psi4 = yaml.dump(raw_yaml)

    pkg_location = package_from_files(
        location=os.path.join(output_dir, "psi4"),
        files={
            'bin/aggregate_energies.py': 'expensive',
            'bin/optimize_ff.py': 'expensive',
            'bin/optimize_psi4.py': 'expensive',

            'conf/flowir_package.yaml': flowir_psi4,
        }
    )

    StorageMetadata = apis.models.virtual_experiment.StorageMetadata
    collection = apis.storage.PackageMetadataCollection({
        ve_psi4.base.packages[0].name: StorageMetadata.from_config(
            prefix_paths=pkg_location, config=apis.models.virtual_experiment.BasePackageConfig(),
        )})

    original_created_on = ve_psi4.metadata.registry.createdOn

    ve_psi4.metadata.registry.digest = "invalid"

    raw_yaml = yaml.load(flowir_psi4, yaml.FullLoader)

    vars_default: Dict[str, str] = raw_yaml['variables']['default']['global']
    vars_openshift: Dict[str, str] = raw_yaml['variables']['openshift']['global']

    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        db = apis.db.exp_packages.DatabaseExperiments(f.name)
        metadata = apis.runtime.package.access_and_validate_virtual_experiment_packages(ve_psi4, collection, db)
        apis.runtime.package.validate_parameterised_package(ve=ve_psi4, metadata=metadata)

    all_vars = ve_psi4.metadata.registry.executionOptionsDefaults.variables
    logger.info(f"All executionOptionDefaults "
                f"{ve_psi4.metadata.registry.executionOptionsDefaults.model_dump_json(indent=2)}")
    extracted: Dict[str, List[apis.models.virtual_experiment.ValueInPlatform]] = {x.name: x.valueFrom for x in all_vars}

    assert 'overrideme' in extracted

    x = sorted(extracted['overrideme'], key=lambda k: k.platform)

    assert len(x) == 2

    assert x[0].platform == 'default'
    assert x[0].value == 'default'

    assert x[1].platform == 'openshift'
    assert x[1].value == 'openshift'

    for name in vars_default:
        assert name in extracted

        values = [v.value for v in extracted[name] if v.platform == "default"]
        assert values[0] == str(vars_default[name])

    for name in vars_openshift:
        assert name in extracted

        values = [v.value for v in extracted[name] if v.platform == "openshift"]
        assert values[0] == str(vars_openshift[name])


def test_extract_all_variables_during_validate(
        flowir_psi4: str,
        ve_psi4: apis.models.virtual_experiment.ParameterisedPackage,
        output_dir: str
):
    pkg_location = package_from_files(
        location=os.path.join(output_dir, "psi4"),
        files={'conf/flowir_package.yaml': flowir_psi4, }
    )

    StorageMetadata = apis.models.virtual_experiment.StorageMetadata
    collection = apis.storage.PackageMetadataCollection({
        ve_psi4.base.packages[0].name: StorageMetadata.from_config(
            prefix_paths=pkg_location, config=apis.models.virtual_experiment.BasePackageConfig(),
        )})

    original_created_on = ve_psi4.metadata.registry.createdOn

    unknown_variable = "I-am-confident-that-this-variable-does-not-exist"
    ve_psi4.parameterisation.executionOptions.variables.append(
        apis.models.common.OptionMany(name=unknown_variable)
    )

    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        db = apis.db.exp_packages.DatabaseExperiments(f.name)
        with pytest.raises(apis.models.errors.UnknownVariableError) as e:
            metadata = apis.runtime.package.access_and_validate_virtual_experiment_packages(ve_psi4, collection, db)
            apis.runtime.package.validate_parameterised_package(ve=ve_psi4, metadata=metadata)

    assert e.value.variable_name == unknown_variable


def test_missing_input(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()

    ve_sum_numbers.metadata.registry.inputs.append(apis.models.common.Option(name="input_smiles.csv"))

    old = apis.models.virtual_experiment.DeprecatedExperimentStartPayload.parse_obj({})
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.from_old_payload(old)
    with pytest.raises(apis.models.errors.InvalidInputsError) as e:
        apis.runtime.package.NamedPackage(ve_sum_numbers, namespace_presets, payload_config)

    e = e.value

    assert e.missing_inputs == ['input_smiles.csv']


def test_extra_input(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    namespace_presets = apis.models.virtual_experiment.NamespacePresets()

    old = apis.models.virtual_experiment.DeprecatedExperimentStartPayload.parse_obj({
        "inputs": [{"filename": "input_smiles.csv", "content": "hello"}]
    })
    payload_config = apis.models.virtual_experiment.PayloadExecutionOptions.from_old_payload(old)
    with pytest.raises(apis.models.errors.InvalidInputsError) as e:
        apis.runtime.package.NamedPackage(ve_sum_numbers, namespace_presets, payload_config)

    e = e.value

    assert e.extra_inputs == ['input_smiles.csv']
