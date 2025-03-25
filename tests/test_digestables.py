# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import pytest

import apis.models.common


def test_digestable_trivial():
    digestable = apis.models.common.DigestableBase(definition={"hello": "world"})
    print(digestable.model_dump())

    assert digestable.to_digest() == "sha256xed62c74e651cbab7337986dbb9dcff6a35b24565d32ec38736d24479"


def test_digestable_optionmany():
    digestable = apis.models.common.OptionMany.model_validate({
        "name": "hello",
        "valueFrom": [
            {"value": "world1", },
            {"secretKeyRef": {"name": "secret.name", "key": "secret.key"}},
            {"datasetRef": {"name": "dataset.name"}},
            {"usernamePassword": {"username": "usernamePassword.username", "password": "usernamePassword.password"}}
        ]
    })

    print(digestable.model_dump())

    assert digestable.to_digest() == "sha256x96a031f8cdf75edb8de8e6c3b4a69e70b62daa9477c1468a1d2e1418"


def test_digestable_option_value():
    digestable = apis.models.common.Option.model_validate({
        'name': 'hello',
        'value': 'world',
    })

    print(digestable.my_contents)
    print(digestable.model_dump())

    assert digestable.my_contents == "world"
    assert digestable.name == 'hello'

    assert digestable.to_digest() == "sha256x4e1b1c4907f171ae1fc5b0f58ba84898e6fa6fb4edcb6bbc4d29d9dc"


def test_digestable_option_secret():
    digestable: apis.models.common.Option = apis.models.common.Option.model_validate({
        'name': 'hello',
        'valueFrom': {"secretKeyRef": {"name": "secret.name", "key": "secret.key"}},
    })

    value: apis.models.common.TOptionValueFrom = digestable.my_contents

    print(type(value), value)
    print(value.model_dump())

    assert isinstance(value, apis.models.common.OptionFromSecretKeyRef)
    assert value.name == "secret.name"
    assert value.key == "secret.key"

    assert digestable.to_digest() == "sha256x3231b5d449195bb46e45a8a08798ce0ffec21f677f4ad963578d230e"


def test_digestable_option_dataset():
    digestable = apis.models.common.Option.model_validate({
        'name': 'hello',
        'valueFrom': {"datasetRef": {"name": "secret.name"}}
    })

    print(digestable.model_dump())

    assert digestable.to_digest() == "sha256xee917d47c6bacd1d525dc77b3ac47bcd7f9c80b3611313de85288c2e"


def test_digestable_option_username_password():
    digestable = apis.models.common.Option.model_validate({
        'name': 'hello',
        'valueFrom': {
            "usernamePassword": {
                "username": "usernamePassword.username",
                "password": "usernamePassword.password"
            }
        }
    })

    print(digestable.model_dump())
    assert digestable.to_digest() == "sha256x369404f74f029f7ae7ef494780c04bd5880e591a0c7939e3c9c8c97c"


def test_options_many_single_value_only():
    with pytest.raises(ValueError) as e:
        many = apis.models.common.OptionMany.model_validate({
            "name": "hello",
            "value": "this should fail",
            "valueFrom": [
                {"value": "world1", },
                {"secretKeyRef": {"name": "secret.name", "key": "secret.key"}},
                {"datasetRef": {"name": "dataset.name"}},
                {"usernamePassword": {"username": "usernamePassword.username", "password": "usernamePassword.password"}}
            ]
        })
    assert "Cannot provide both value and valueFrom" in str(e.value)
