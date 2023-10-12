# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import datetime
import json
import logging
import os
import pprint
import tempfile
import typing

import experiment.model.errors
import experiment.model.frontends.flowir
import pytest
import yaml

import apis.db.exp_packages
import apis.kernel.experiments
import apis.models.common
import apis.models.errors
import apis.models.virtual_experiment
import apis.runtime.package
import apis.storage
from tests import conftest


def test_parse_simplest_entry(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    # VV: Record when "now" is in the UTC timezone (now = when entry is "created")
    utcnow = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    ve = ve_sum_numbers

    # VV: test metadata
    assert ve.metadata.package.name == "http-sum-numbers"
    assert ve.metadata.package.tags == ["latest"]
    assert ve.metadata.package.maintainer == "st4sd@st4sd.st4sd"

    print("The definition of sum numbers is")
    print(json.dumps(ve.dict(exclude_none=True), sort_keys=True, indent=4, separators=(',', ': ')))

    print("The digest of sumnumbers")
    print(json.dumps(ve.to_digestable().dict(), sort_keys=True, indent=4, separators=(',', ': ')))

    assert ve.metadata.registry.digest == "sha256x16092ca4bb13955b1397bf38cfba45ef11c9933bf796454a81de4f86"

    # VV: It is safe to assume that generating the metadata takes less than 120 seconds
    assert (utcnow - ve.registry_created_on).total_seconds() < 120

    # VV: test "base"
    base = ve.base.packages

    assert len(base) == 1

    base = base[0]

    source: apis.models.virtual_experiment.BasePackageSourceGit = base.source.my_contents
    assert isinstance(source, apis.models.virtual_experiment.BasePackageSourceGit)

    location = source.location
    assert location.url == "https://github.ibm.com/st4sd/sum-numbers.git"
    assert location.commit is None
    assert location.branch == "main"
    assert location.tag is None

    config = base.config

    assert config.path is None
    assert config.manifestPath is None

    assert base.name == "main"

    registries = base.dependencies.imageRegistries
    assert len(registries) == 0

    # VV: test parameterisation.presets
    presets = ve.parameterisation.presets

    assert len(presets.variables) == 0
    assert len(presets.data) == 0
    assert len(presets.environmentVariables) == 0
    runtime = presets.runtime
    assert runtime.args == ["--failSafeDelays=no"]

    # VV: test parameterisation.executionOptions
    exec_opts = ve.parameterisation.executionOptions

    assert len(exec_opts.data) == 0
    assert len(exec_opts.runtime.args) == 0
    assert len(exec_opts.variables) == 0
    assert exec_opts.platform == ['artifactory', 'default']


def test_merge_metadata_registry_ok():
    inputs = {'input0', 'input1'}
    data = {'data0', 'data1'}
    images_one = {'image0', 'image1'}
    images_two = {'image2', 'image3'}

    one = apis.models.virtual_experiment.MetadataRegistry(
        inputs=[apis.models.common.Option(name=name) for name in inputs],
        data=[apis.models.common.Option(name=name) for name in data],
        containerImages=[apis.models.common.Option(name=name) for name in images_one],
        executionOptionsDefaults=apis.models.virtual_experiment.ExecutionOptionDefaults(
            variables=[
                apis.models.virtual_experiment.VariableWithDefaultValues(name='unique-one', valueFrom=[
                    apis.models.virtual_experiment.ValueInPlatform(value='hello-one', platform='one')
                ]),
                apis.models.virtual_experiment.VariableWithDefaultValues(name='common', valueFrom=[
                    apis.models.virtual_experiment.ValueInPlatform(value='common-one', platform='one')
                ])
            ]
        )
    )

    two = apis.models.virtual_experiment.MetadataRegistry(
        inputs=[apis.models.common.Option(name=name) for name in inputs],
        data=[apis.models.common.Option(name=name) for name in data],
        containerImages=[apis.models.common.Option(name=name) for name in images_two],
        executionOptionsDefaults=apis.models.virtual_experiment.ExecutionOptionDefaults(
            variables=[
                apis.models.virtual_experiment.VariableWithDefaultValues(name='unique-two', valueFrom=[
                    apis.models.virtual_experiment.ValueInPlatform(value='hello-two', platform='two')
                ]),
                apis.models.virtual_experiment.VariableWithDefaultValues(name='common', valueFrom=[
                    apis.models.virtual_experiment.ValueInPlatform(value='common-two', platform='two')
                ])
            ]
        )
    )

    merged = apis.models.virtual_experiment.MetadataRegistry.merge(one, two)

    m_inputs = {x.name for x in merged.inputs}
    m_data = {x.name for x in merged.data}
    m_images = {x.name for x in merged.containerImages}

    assert m_inputs == inputs
    assert m_data == data
    assert m_images == images_one.union(images_two)

    unique_one = merged.executionOptionsDefaults.get_variable('unique-one')
    unique_two = merged.executionOptionsDefaults.get_variable('unique-two')
    common = merged.executionOptionsDefaults.get_variable('common')

    assert unique_one.dict() == {'name': 'unique-one', 'valueFrom': [{'platform': 'one', 'value': 'hello-one'}]}
    assert unique_two.dict() == {'name': 'unique-two', 'valueFrom': [{'platform': 'two', 'value': 'hello-two'}]}

    assert len(common.valueFrom) == 2
    assert common.get_platform_value('one') == 'common-one'
    assert common.get_platform_value('two') == 'common-two'


def test_merge_metadata_registry_wrong_inputs():
    inputs = {'input0', 'input1'}
    inputs_wrong = {'input0', 'input2222'}
    data = {'data0', 'data1'}
    images_one = {'image0', 'image1'}
    images_two = {'image2', 'image3'}

    one = apis.models.virtual_experiment.MetadataRegistry(
        inputs=[apis.models.common.Option(name=name) for name in inputs],
        data=[apis.models.common.Option(name=name) for name in data],
        containerImages=[apis.models.common.Option(name=name) for name in images_one],
    )

    two = apis.models.virtual_experiment.MetadataRegistry(
        inputs=[apis.models.common.Option(name=name) for name in inputs_wrong],
        data=[apis.models.common.Option(name=name) for name in data],
        containerImages=[apis.models.common.Option(name=name) for name in images_two],
    )

    with pytest.raises(apis.models.errors.CannotMergeMetadataRegistryError) as e:
        _ = apis.models.virtual_experiment.MetadataRegistry.merge(one, two)

    exc = e.value
    assert exc.key == "inputs"
    assert exc.bad_metadata_registry == two


def test_extract_metadata_registry_useful_information_from_concrete_ok():
    flowir = """
variables:
  default:
    global:
      backend: local
  cloud:
    global:
      backend: kubernetes
  hpc:
    global:
      backend: lsf
            
components:
- name: hello
  command:
    executable: cat
    arguments: data/data.txt:ref input/input.txt:ref data:ref/*
  references:
  - data/data.txt:ref
  - input/input.txt:ref
  - data:ref

  resourceManager:
    config:
      backend: "%(backend)s"
    kubernetes:
      image: st4sd.st4sd/st4sd:kubernetes
    lsf:
      dockerImage: st4sd.st4sd/st4sd:lsf
"""
    flowir = yaml.load(flowir, Loader=yaml.FullLoader)
    concrete = experiment.model.frontends.flowir.FlowIRConcrete(flowir, platform=None, documents=None)

    meta = apis.models.virtual_experiment.MetadataRegistry.from_flowir_concrete_and_data(
        concrete, ['data.txt'], platforms=None, variable_names=[])

    assert {x.name for x in meta.inputs} == {'input.txt'}
    assert {x.name for x in meta.data} == {'data.txt'}
    assert {x.name for x in meta.containerImages} == {'st4sd.st4sd/st4sd:kubernetes', 'st4sd.st4sd/st4sd:lsf'}


def test_extract_metadata_registry_useful_information_from_concrete_bad_inputs():
    flowir = """
variables:
  default:
    global:
      backend: local
      name: from-default
  cloud:
    global:
      backend: kubernetes
      name: from-cloud
  hpc:
    global:
      backend: lsf

components:
- name: hello
  command:
    executable: cat
    arguments: data/data.txt:ref input/input.txt:ref data:ref/*
  references:
  - data/data.txt:ref
  - input/%(name)s.txt:ref
  - data:ref

  resourceManager:
    config:
      backend: "%(backend)s"
    kubernetes:
      image: st4sd.st4sd/st4sd:kubernetes
    lsf:
      dockerImage: st4sd.st4sd/st4sd:lsf
"""
    flowir = yaml.load(flowir, Loader=yaml.FullLoader)
    concrete = experiment.model.frontends.flowir.FlowIRConcrete(flowir, platform=None, documents=None)

    with pytest.raises(apis.models.errors.InconsistentPlatformError) as e:
        apis.models.virtual_experiment.MetadataRegistry.from_flowir_concrete_and_data(
            concrete, ['data.txt'], platforms=None, variable_names=[])

    exc = e.value
    underlying = exc.error

    assert isinstance(underlying, apis.models.errors.CannotMergeMetadataRegistryError)
    assert underlying.key == "inputs"


def test_removing_unknown_fields():
    ve = apis.models.virtual_experiment.ParameterisedPackageDropUnknown.parse_obj({
        'hello': 'world',
        'metadata': {
            'package': {
                'deprecated': 'deprecated'
            }
        }
    })


def test_executionOptionDefaults_variables_without_parameterisation(
        str_sum_numbers: str,
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage,
):
    flowir = yaml.load(str_sum_numbers, Loader=yaml.FullLoader)
    concrete = experiment.model.frontends.flowir.FlowIRConcrete(flowir, platform=None, documents=None)

    ve_sum_numbers.parameterisation.executionOptions.platform = ['openshift', 'scafellpike']
    ve_sum_numbers.parameterisation.executionOptions.variables = [
        apis.models.common.OptionMany(name=v) for v in ['addToSum', 'numberOfPoints']
    ]

    platforms = ve_sum_numbers.parameterisation.get_available_platforms()
    assert platforms == ['openshift', 'scafellpike']

    meta = apis.models.virtual_experiment.MetadataRegistry.from_flowir_concrete_and_data(
        concrete,
        ['cat_me.txt'],
        platforms=platforms,
        variable_names=ve_sum_numbers.parameterisation.get_configurable_variable_names()
    )

    assert ve_sum_numbers.parameterisation.get_configurable_variable_names() == \
           ['addToSum', 'numberOfPoints']

    meta.inherit_defaults(ve_sum_numbers.parameterisation)

    pprint.pprint(meta.dict())
    add_to_sum = meta.executionOptionsDefaults.get_variable('addToSum')
    num_points = meta.executionOptionsDefaults.get_variable('numberOfPoints')

    assert len(add_to_sum.valueFrom) == len(platforms)
    assert add_to_sum.get_platform_value('openshift') == '-5'
    assert add_to_sum.get_platform_value('scafellpike') == '10'

    assert len(num_points.valueFrom) == len(platforms)
    assert num_points.get_platform_value('openshift') == '3'
    assert num_points.get_platform_value('scafellpike') == '3'


def test_executionOptionDefaults_variables_with_parameterisation(
        str_sum_numbers: str,
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage,
):
    flowir = yaml.load(str_sum_numbers, Loader=yaml.FullLoader)
    concrete = experiment.model.frontends.flowir.FlowIRConcrete(flowir, platform=None, documents=None)
    print("Platforms", concrete.platforms)

    ve_sum_numbers.parameterisation.executionOptions.variables = [
        apis.models.common.OptionMany(name=v, value=f"{i}") for i, v in enumerate(['addToSum', 'numberOfPoints'])
    ]
    ve_sum_numbers.parameterisation.executionOptions.platform = ['openshift', 'scafellpike']
    platforms = ve_sum_numbers.parameterisation.get_available_platforms()
    assert platforms == ['openshift', 'scafellpike']

    meta = apis.models.virtual_experiment.MetadataRegistry.from_flowir_concrete_and_data(
        concrete,
        ['cat_me.txt'],
        platforms=platforms,
        variable_names=ve_sum_numbers.parameterisation.get_configurable_variable_names()
    )

    assert ve_sum_numbers.parameterisation.get_configurable_variable_names() == \
           ['addToSum', 'numberOfPoints']

    meta.inherit_defaults(ve_sum_numbers.parameterisation)
    pprint.pprint(meta.dict())
    add_to_sum = meta.executionOptionsDefaults.get_variable('addToSum')
    num_points = meta.executionOptionsDefaults.get_variable('numberOfPoints')

    assert len(add_to_sum.valueFrom) == len(platforms)
    assert add_to_sum.get_platform_value('openshift') == '0'
    assert add_to_sum.get_platform_value('scafellpike') == '0'

    assert len(num_points.valueFrom) == len(platforms)
    assert num_points.get_platform_value('openshift') == '1'
    assert num_points.get_platform_value('scafellpike') == '1'


def test_extract_registry_metadata(
        str_toxicity_pred: str,
        ve_toxicity_pred: apis.models.virtual_experiment.ParameterisedPackage
):
    platforms = ['openshift', 'sandbox']
    assert ve_toxicity_pred.parameterisation.get_available_platforms() == platforms

    flowir = yaml.load(str_toxicity_pred, Loader=yaml.FullLoader)
    concrete = experiment.model.frontends.flowir.FlowIRConcrete(flowir, platform=None, documents=None)
    meta = apis.models.virtual_experiment.MetadataRegistry.from_flowir_concrete_and_data(
        concrete,
        ['input_sdf.sdf'],
        platforms=ve_toxicity_pred.parameterisation.get_available_platforms(),
        variable_names=ve_toxicity_pred.parameterisation.get_configurable_variable_names(),
    )

    assert sorted(meta.containerImages, key=lambda x: x.name) == sorted([
        apis.models.common.Option(
            name='res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/caf-st4sd:1.0.0'),
        apis.models.common.Option(name='alex.python', value=None),
        apis.models.common.Option(
            name='res-st4sd-community-team-applications-docker-virtual.artifactory.swg-devops.com/'
                 'opera-serial-mdlab:2.6.0')], key=lambda x: x.name)
    assert meta.inputs == [apis.models.common.Option(name="input_smiles.csv")]
    assert meta.data == [apis.models.common.Option(name="input_sdf.sdf")]

    assert len(meta.executionOptionsDefaults.variables) == 1

    num_procs = meta.executionOptionsDefaults.get_variable('number-processors')
    assert len(num_procs.valueFrom) == len(platforms)
    assert num_procs.get_platform_value('openshift') == '1'
    assert num_procs.get_platform_value('sandbox') == '10'


def combine_sumnumbers_with_flowir(
        str_sum_numbers: str,
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage,
        output_dir: str
):
    package = conftest.package_from_flowir(str_sum_numbers, output_dir, extra_files={'data/cat_me.txt': 'hello'})
    ve_sum_numbers.base.packages[0].config.path = '.'

    x = apis.models.virtual_experiment.StorageMetadata.from_config(
        ve_sum_numbers.base.packages[0].config, 'default', prefix_paths=package.location)

    assert x.data == ['cat_me.txt']

    platforms = ['openshift', 'scafellpike']

    meta = apis.models.virtual_experiment.MetadataRegistry.from_flowir_concrete_and_data(
        concrete=x.concrete,
        data_files=x.data,
        platforms=platforms,
        variable_names=ve_sum_numbers.parameterisation.get_configurable_variable_names()
    )

    ve_sum_numbers.metadata.registry = meta


def test_check_execution_options_data_valid(
        str_sum_numbers: str,
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage,
        output_dir: str
):
    ve_sum_numbers.parameterisation.executionOptions.data.append(apis.models.common.Option(name='cat_me.txt'))
    combine_sumnumbers_with_flowir(str_sum_numbers, ve_sum_numbers, output_dir)
    ve_sum_numbers.test()


def test_check_execution_options_data_invalid(
        str_sum_numbers: str,
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage,
        output_dir: str,
):
    ve_sum_numbers.parameterisation.executionOptions.data.append(apis.models.common.Option(name='cat_me.txt-not'))
    combine_sumnumbers_with_flowir(str_sum_numbers, ve_sum_numbers, output_dir)

    with pytest.raises(apis.models.errors.ApiError) as e:
        ve_sum_numbers.test()

    logging.getLogger().info(f"ApiError message was {e.value}")
    assert 'cat_me.txt' in str(e.value)


@pytest.mark.parametrize("flowir_fixture_name,ve_fixture_name,expected_platforms", [
    ("str_toxicity_pred", "ve_toxicity_pred_preset_platform", ['default', 'hermes', 'openshift', 'sandbox']),
    ("str_toxicity_pred", "ve_toxicity_pred_one_executionoption_platform",
     ['default', 'hermes', 'openshift', 'sandbox']),
    ("str_sum_numbers", "ve_sum_numbers_executionoptions_platform_no_values",
     ['artifactory', 'default', 'openshift', 'scafellpike']),
])
def test_check_metadata_registry_platforms(flowir_fixture_name: str,
                                           ve_fixture_name: str,
                                           expected_platforms: typing.List[str],
                                           output_dir: str,
                                           request):
    # AP - pytest inbuilt to get fixture by name
    wf_flowir = request.getfixturevalue(flowir_fixture_name)
    wf_ve: apis.models.virtual_experiment.ParameterisedPackage = request.getfixturevalue(ve_fixture_name)

    #
    flowir = yaml.load(wf_flowir, Loader=yaml.FullLoader)
    concrete = experiment.model.frontends.flowir.FlowIRConcrete(flowir, platform=None, documents=None)
    meta = apis.models.virtual_experiment.MetadataRegistry.from_flowir_concrete_and_data(
        concrete,
        ['cat_me.txt'],
        platforms=None,
        variable_names=[]
    )

    assert sorted(meta.platforms) == sorted(expected_platforms)

    # Create a PackageMetadataCollection by hand
    pkg_location = conftest.package_from_files(
        location=os.path.join(output_dir, "current_ve"),
        files={'conf/flowir_package.yaml': wf_flowir, }
    )

    StorageMetadata = apis.models.virtual_experiment.StorageMetadata
    collection = apis.storage.PackageMetadataCollection({
        wf_ve.base.packages[0].name: StorageMetadata.from_config(
            prefix_paths=pkg_location, config=apis.models.virtual_experiment.BasePackageConfig(),
        )})

    wf_ve.metadata.package.keywords.append("internal-experiment")
    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            apis.kernel.experiments.validate_and_store_pvep_in_db(collection, wf_ve, db)
            res = db.query_identifier(wf_ve.metadata.package.name)
            retrieved_pvep = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(res[0])
            assert sorted(retrieved_pvep.metadata.registry.platforms) == sorted(expected_platforms)

            assert "internal-experiment" not in retrieved_pvep.metadata.package.keywords
