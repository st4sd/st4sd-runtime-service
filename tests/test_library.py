# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

import os
import typing

import random
import string

import experiment.model.frontends.dsl
import pytest
import yaml

import apis.kernel.library
import apis.models.errors
import apis.storage.actuators
import apis.storage.actuators.local
import apis.storage.actuators.memory
import apis.storage.actuators.s3

from .test_internal_experiments import (
    simple_dsl2,
    simple_dsl2_with_inputs,
)


library_s3 = pytest.mark.skipif("not config.getoption('library_s3')")

@pytest.fixture()
def dsl_no_workflow() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
    entrypoint:
      entry-instance: main
    
    components:
    - signature:
        name: main
        parameters: []
      command:
        executable: hello
    """)


@pytest.fixture()
def dsl_no_component() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
    entrypoint:
      entry-instance: main

    workflows:
    - signature:
        name: main
        parameters: []
      steps: {}
      execute: []
    """)


@pytest.fixture()
def dsl_no_entrypoint_workflow() -> typing.Dict[str, typing.Any]:
    return yaml.safe_load("""
    entrypoint:
      entry-instance: main

    components:
    - signature:
        name: dummy
        parameters: []
      command:
        executable: hello

    workflows:
    - signature:
        name: main
        parameters: []
      steps:
        hello: not-dummy
      execute:
      - target: <hello>
        args: {}
    """)


@pytest.fixture()
def dsl_invalid_dsl(dsl_no_entrypoint_workflow: typing.Dict[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    dsl_no_entrypoint_workflow["entrypoint"]["execute"]= [
        {
            "target": "<entry-instance>",
            "args": {}
        }
    ]

    return dsl_no_entrypoint_workflow

@pytest.mark.parametrize("the_dsl_fixture_name", ["simple_dsl2", "simple_dsl2_with_inputs"])
def test_simple_dsl_validate_only(the_dsl_fixture_name, request):
    dsl = request.getfixturevalue(the_dsl_fixture_name)
    apis.kernel.library.LibraryClient.validate_graph(dsl)


def test_missing_workflow(dsl_no_workflow: typing.Dict[str, typing.Any]):
    with pytest.raises(apis.models.errors.InvalidModelError) as e:
        apis.kernel.library.LibraryClient.validate_graph(dsl_no_workflow)

    assert e.value.problems == [
        {"message": "There must be at least 1 workflow template"},
        {'message': 'Missing entrypoint workflow template'}
    ]


def test_missing_component(dsl_no_component: typing.Dict[str, typing.Any]):
    with pytest.raises(apis.models.errors.InvalidModelError) as e:
        apis.kernel.library.LibraryClient.validate_graph(dsl_no_component)

    assert e.value.problems == [
        {"message": "There must be at least 1 component template"},
        {'message': 'Missing entrypoint workflow template'}
    ]


def test_missing_entrypoint_workflow_template(dsl_no_entrypoint_workflow: typing.Dict[str, typing.Any]):
    with pytest.raises(apis.models.errors.InvalidModelError) as e:
        apis.kernel.library.LibraryClient.validate_graph(dsl_no_entrypoint_workflow)

    assert e.value.problems == [
        {"message": "Missing entrypoint workflow template"}
    ]


def test_dsl_invalid_dsl(dsl_invalid_dsl: typing.Dict[str, typing.Any]):
    with pytest.raises(apis.models.errors.InvalidModelError) as e:
        apis.kernel.library.LibraryClient.validate_graph(dsl_invalid_dsl)

    exc = e.value

    assert exc.problems == [
        {
            'message': '"Node [\'entry-instance\', \'hello\'] has no matching template"',
            'location': ['workflows', 'main', 'execute', 0]
         }
    ]


def check_basic_library_operations(
    dsl: typing.Dict[str, typing.Any],
    client: apis.kernel.library.LibraryClient,
):
    namespace_orig = client.add(dsl)
    from_library = client.get(namespace_orig.entrypoint.entryInstance)

    namespace_library = experiment.model.frontends.dsl.Namespace(**from_library)

    print(yaml.safe_dump(namespace_library.dict(by_alias=True, exclude_none=True, exclude_defaults=True), sort_keys=False))

    assert namespace_library.dict(
        by_alias=True, exclude_none=True, exclude_defaults=True
    ) == namespace_orig.dict(
        by_alias=True, exclude_none=True, exclude_defaults=True
    )

    graph_names = client.list()

    assert graph_names == [namespace_library.entrypoint.entryInstance]

    with pytest.raises(apis.models.errors.GraphAlreadyExistsError):
        client.add(dsl)

    client.delete(namespace_orig.entrypoint.entryInstance)

    with pytest.raises(apis.models.errors.GraphDoesNotExistError):
        client.get(namespace_orig.entrypoint.entryInstance)


def test_in_memory_library_operations(simple_dsl2: typing.Dict[str, typing.Any]):
    actuator = apis.storage.actuators.memory.InMemoryStorage({})
    client = apis.kernel.library.LibraryClient(actuator=actuator)
    check_basic_library_operations(dsl=simple_dsl2, client=client)

    assert actuator.files == {"/": None, "library/": None}

def test_local_library_operations(
    simple_dsl2: typing.Dict[str, typing.Any],
    output_dir: str,
):
    library_path = os.path.join(output_dir, "library")
    actuator = apis.storage.actuators.local.LocalStorage()
    client = apis.kernel.library.LibraryClient(actuator=actuator, library_path=library_path)
    check_basic_library_operations(dsl=simple_dsl2, client=client)

    assert list(actuator.listdir(library_path)) == []


@library_s3
def test_s3_library_operations(
    simple_dsl2: typing.Dict[str, typing.Any],
):
    lookup = {
        "S3_LIBRARY_BUCKET": "bucket",
        "S3_LIBRARY_ENDPOINT": "endpoint_url",
        "S3_LIBRARY_ACCESS_KEY_ID": "access_key_id",
        "S3_LIBRARY_SECRET_ACCESS_KEY": "secret_access_key",
        "S3_LIBRARY_REGION": "region_name"
    }
    args = {arg_name: os.environ.get(env_var) for env_var, arg_name in lookup.items()}

    rand = random.Random()
    characters = string.ascii_letters + string.digits
    suffix = ''.join((rand.choice(characters) for x in range(10)))

    library_path = f"library-{suffix}"
    actuator = apis.storage.actuators.s3.S3Storage(**args)
    client = apis.kernel.library.LibraryClient(actuator=actuator, library_path=library_path)
    check_basic_library_operations(dsl=simple_dsl2, client=client)

    assert list(actuator.listdir(library_path)) == []
