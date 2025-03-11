# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import argparse
import hashlib
import re as reg_ex
import os
from collections import namedtuple
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
from typing import cast

import pydantic
from pydantic import model_validator, ConfigDict, BaseModel
from six import string_types

import apis.models.errors
import typing_extensions

PRIMITIVE_TYPES = (float, int, bool, string_types)


class DigestableBase(BaseModel):
    """A class which generates a Digest (an embedding) out of dictionaries whose keys are strings and valeus are
    either strings or other Digestable instances"""
    model_config = ConfigDict(extra="allow")

    @classmethod
    def from_list(cls, items: List[Union[float, int, bool, string_types, DigestableBase]]) -> DigestableBase:
        return cls(definition={f"item_{i}": x for i, x in enumerate(items)})

    def to_digest(self) -> str:
        remaining = [self.dict() or {'what': 'empty'}]
        sha256 = hashlib.sha256()

        while remaining:
            obj = remaining.pop(0)
            try:
                if isinstance(obj, Digestable):
                    sha256.update(f"{type(obj)}{obj.to_digest()}".encode('utf-8'))
                elif isinstance(obj, PRIMITIVE_TYPES) or obj is None:
                    sha256.update(f"{type(obj)}_{repr(obj)}".encode('utf-8'))
                elif isinstance(obj, dict):
                    for k in sorted(obj, reverse=True):
                        remaining.insert(0, k)
                        remaining.insert(1, obj[k])
                elif isinstance(obj, (list, tuple)):
                    for k in obj:
                        remaining.insert(0, k)
                else:
                    raise ValueError("Cannot generate hash of %s: %s" % (type(obj), obj))
            except Exception as e:
                raise ValueError(f"Cannot generate hash of {obj}") from e

        digest = f"sha256x{sha256.hexdigest()}"

        # VV: we can only store up to 63 characters in k8s labels so we just truncate the entire "digest" string here
        return digest[:63]

    def get_contents(self, skip=None) -> Digestable:
        skip = skip or ['name', 'definition']

        for (key, value) in self.dict().items():
            if value is not None and key not in skip:
                return getattr(self, key)

    @property
    def my_contents(self) -> Any:
        return self.get_contents()

    def dict(
            self,
            *,
            exclude_none: bool = True,
            by_alias=True,
            **kwargs
    ) -> Dict[str, Any]:
        return super(DigestableBase, self).dict(exclude_none=exclude_none, by_alias=by_alias, **kwargs)


class Digestable(DigestableBase):
    model_config = ConfigDict(extra="forbid")

    def to_digestable(self) -> Digestable:
        return self


class DigestableSingleField(Digestable):
    @model_validator(mode="after")
    def validate_only_one(cls, value: "DigestableSingleField") -> "DigestableSingleField":
        what = value.model_dump(exclude_none=True)
        if len(what) != 1:
            raise ValueError("Must define exactly 1 field")
        return value


class OptionFromSecretKeyRef(Digestable):
    key: Optional[str] = None
    name: str


class OptionFromS3SecretKeyRef(Digestable):
    keyAccessKeyID: Optional[str] = None
    keySecretAccessKey: Optional[str] = None
    keyBucket: str = None
    keyEndpoint: str = None
    keyPath: Optional[str] = None
    objectName: str

class RenamablePath(Digestable):
    path: Optional[str] = None
    rename: Optional[str] = pydantic.Field(
        None, description="If set, and path is not None then this means that the path filename should be renamed "
                          "to match @rename")

    def to_path_instruction(self, default_path: Optional[str] = None) -> str:
        """Produces a path that can be used as an input/data file for elaunch.py

        Args:
            default_path:
                Used as the default vlaue for self.path

        Returns:
            A string which may include rename configuration (i.e. $source:$target)
        """
        path = self.path or default_path
        if path.startswith(os.path.sep):
            # VV: joining ("hello" with "/hi there") produces "/hi there"
            path = path.lstrip(os.path.sep)

        # VV: Escape the \ and : characters - elaunch.py will unescape them
        path = path.replace("\\", "\\\\")
        path = path.replace(":", "\\:")

        if self.rename:
            path = ':'.join((path, self.rename))

        return path


class OptionFromS3Values(RenamablePath):
    accessKeyID: Optional[str] = None
    secretAccessKey: Optional[str] = None
    bucket: Optional[str] = None
    endpoint: Optional[str] = None
    region: Optional[str] = None


class OptionFromVolumeRef(RenamablePath):
    name: str


class OptionFromDatasetRef(OptionFromVolumeRef):
    pass


class OptionFromUsernamePassword(Digestable):
    username: Optional[str] = None
    password: Optional[str] = None


TOptionValueFrom = Union[OptionFromSecretKeyRef, OptionFromDatasetRef, OptionFromUsernamePassword, \
                         OptionFromS3Values, OptionFromS3SecretKeyRef, None]


class OptionValueFrom(Digestable):
    secretKeyRef: Optional[OptionFromSecretKeyRef] = None
    datasetRef: Optional[OptionFromDatasetRef] = None
    usernamePassword: Optional[OptionFromUsernamePassword] = None
    s3Ref: Optional[OptionFromS3Values] = None
    s3SecretKeyRef: Optional[OptionFromS3SecretKeyRef] = None
    volumeRef: Optional[OptionFromVolumeRef] = None

    @property
    def my_contents(self) -> TOptionValueFrom:
        value = cast(TOptionValueFrom, self.get_contents())
        return value


class OptionValueFromMany(Digestable):
    value: Optional[str] = None
    secretKeyRef: Optional[OptionFromSecretKeyRef] = pydantic.Field(
        None,
        description="Value is in a Kubernetes Secret object"
    )
    datasetRef: Optional[OptionFromDatasetRef] = None
    usernamePassword: Optional[OptionFromUsernamePassword] = None
    s3Ref: Optional[OptionFromS3Values] = None
    s3SecretKeyRef: Optional[OptionFromS3SecretKeyRef] = None

    @property
    def my_contents(self) -> TOptionValueFrom:
        value = cast(TOptionValueFrom, self.get_contents())
        return value


def try_convert_to_str(value: typing.Union[str, float, int]) -> str:
    if isinstance(value, (float, int)):
        return str(value)
    return value

MustBeString = typing_extensions.Annotated[str, pydantic.functional_validators.BeforeValidator(try_convert_to_str)]

class Option(Digestable):
    name: Optional[str] = None
    value: Optional[MustBeString] = None
    valueFrom: Optional[OptionValueFrom] = None

    @property
    def my_contents(self) -> Union[TOptionValueFrom, str]:
        value = self.get_contents()
        if isinstance(value, OptionValueFrom):
            return value.my_contents
        return cast(Union[TOptionValueFrom, str], value)

    @classmethod
    def parse_obj(cls, *args, **kwargs) -> Option:
        return cast(Option, super(Option, cls).parse_obj(*args, **kwargs))


class OptionMany(Digestable):
    name: Optional[str] = None
    value: Optional[str] = pydantic.Field(
        None, description="This is the default value of the variable, providing this field means "
                          "that the variable can recieve *any* value")
    valueFrom: Optional[List[OptionValueFromMany]] = None

    @pydantic.model_validator(mode='after')
    def validate_exactly_one_field(cls, field_values: "OptionMany"):
        if field_values.value and field_values.valueFrom:
            raise ValueError(f"Cannot provide both value and valueFrom")
        return field_values

    @classmethod
    def parse_obj(cls, *args, **kwargs) -> OptionMany:
        return cast(cls, super(OptionMany, cls).parse_obj(*args, **kwargs))


PackageIdentifierWithTag = namedtuple("PackageIdentifierWithTag", ["name", "tag"])
PackageIdentifierWithDigest = namedtuple("PackageIdentifierWithDigest", ["name", "digest"])
PackageIdentifierWithEverything = namedtuple("PackageIdentifierWithEverything", ["name", "tag", "digest"])


class PackageIdentifier:
    @classmethod
    def from_parts(cls, package_name: str, tag: str | None, digest: str | None) -> PackageIdentifier:
        if tag:
            return cls(':'.join((package_name, tag)))
        return cls('@'.join((package_name, digest)))

    @classmethod
    def from_everything(cls, everything: PackageIdentifierWithEverything) -> PackageIdentifier:
        return cls.from_parts(package_name=everything.name, tag=everything.tag, digest=everything.digest)

    def __init__(self, identifier: str):
        self._identifier = identifier

        if ':' not in self._identifier and '@' not in self._identifier:
            self._identifier = ":".join((self._identifier, 'latest'))

        if self.is_tag() == self.is_digest():
            raise ValueError(f"{self._identifier} cannot contain both a Tag and a Digest")

    @property
    def identifier(self) -> str:
        return self._identifier

    def is_digest(self):
        return '@' in self._identifier

    def is_tag(self):
        return ':' in self._identifier

    def parse(self) -> PackageIdentifier:
        name = self.name
        try:
            tag = self.tag
        except ValueError:
            tag = None

        try:
            digest = self.digest
        except ValueError:
            digest = None

        return PackageIdentifierWithEverything(name, tag, digest)

    def parse_tag(self) -> PackageIdentifierWithTag:
        if self.is_tag() is False:
            raise ValueError(f"PackageIdentifier {self._identifier} does not contain a tag")
        name, tag = self._identifier.rsplit(':', 1)
        return PackageIdentifierWithTag(name, tag)

    def parse_digest(self) -> PackageIdentifierWithDigest:
        if self.is_digest() is False:
            raise ValueError(f"PackageIdentifier {self._identifier} does not contain a digest")
        name, digest = self._identifier.rsplit('@', 1)
        return PackageIdentifierWithDigest(name, digest)

    @property
    def name(self) -> str:
        if self.is_tag():
            name_tag = self.parse_tag()
            return name_tag.name
        else:
            name_digest = self.parse_digest()
            return name_digest.name

    @property
    def tag(self) -> str:
        name_tag = self.parse_tag()
        return name_tag.tag

    @property
    def digest(self) -> str:
        name_digest = self.parse_digest()
        return name_digest.digest


def parser_important_elaunch_arguments():
    def arg_to_bool(name, val):
        """Converts a str value (positive, negative) to a boolean (case insensitive match)

        Arguments:
            name(str): Name of argument
            val(str): Can be one of the following (yes, no, true, false, y, n)

        Returns
            boolean - True if val is "positive", False if val is "negative

        Raises:
            TypeError - if val is not a string
            ValueError - if val.lower() is not one of [yes, no, true, false, y, n]
        """

        options_positive = ['yes', 'true', 'y']
        options_negative = ['no', 'false', 'n']

        val = val.lower()
        if val in options_negative:
            return False
        elif val in options_positive:
            return True
        else:
            return apis.models.errors.InvalidElaunchParameterChoices(name, val, options_positive + options_negative)

    def decode_value(name):
        def closure(val):
            return arg_to_bool(name, val)

        return closure

    class NoSystemExitParser(argparse.ArgumentParser):
        def error(self, message):
            raise ValueError(message)

        def exit(self, status, message):
            raise ValueError("%s: %s" % (status, message))

        def parse_known_args(self, args=None, namespace=None) -> Tuple[argparse.Namespace, List[str]]:
            """Parses arguments for which the parser has been configured, and also raises descriptive exceptions
            for malformed command-line arguments to elaunch.
            """
            namespace, args = super(NoSystemExitParser, self).parse_known_args(args, namespace)
            problems = []

            for k in vars(namespace):
                # VV: When the `arg_to_bool` method from above detects a problem, instead of raising an
                # exception it returns an InvalidElaunchParameter instance (or an instance of a derived class)
                val = namespace.__getattribute__(k)

                if isinstance(val, apis.models.errors.InvalidElaunchParameter):
                    problems.append(val)

            if len(problems):
                raise apis.models.errors.ManyInvalidElaunchParameters(problems)

            return namespace, args

    parser = NoSystemExitParser()

    # VV: FIXME There're probably more arguments, think of a way to keep this code up-to-date automatically
    boolean_values = [
        'noKubernetesSecurityContext', 'useMemoization', 'registerWorkflow', 'fuzzyMemoization',
        'restageData', 'noRestartHooks', 'ignoreTestExecutablesError', 'ignoreInitializeBackendError',
        'failSafeDelays',
    ]

    string_values = [
        'executionMode'
    ]

    for name in boolean_values:
        parser.add_argument(f'--{name}', required=False, type=decode_value(name), default=None)

    for name in string_values:
        parser.add_argument(f'--{name}', required=False, type=str, default=None)

    return parser


K8S_PATTERN_VALUE = reg_ex.compile(r'(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')
K8S_PATTERN_OBJECT_NAME = reg_ex.compile(r'[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')


def valid_k8s_label(name, value, pattern_name=K8S_PATTERN_VALUE, pattern_value=K8S_PATTERN_VALUE):
    if len(name) > 63 or len(value) > 63:
        return False

    return (pattern_name.fullmatch(name) is not None) and (pattern_value.fullmatch(value) is not None)
