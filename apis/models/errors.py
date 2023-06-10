# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

from typing import Dict
from typing import Any
from typing import List
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import apis.models.virtual_experiment


class ApiError(Exception):
    def __init__(self, msg: str):
        self.message = msg

    def __repr__(self):
        return str(self)

    def __str__(self):
        return self.message


class InvalidModelError(ApiError):
    def __init__(self, msg: str, problems: List[Dict[str, Any]]):
        super().__init__(msg=msg)
        self.problems = problems


class UnknownVariableError(ApiError):
    def __init__(self, variable_name: str, platform: str):
        self.variable_name = variable_name
        self.platform = platform

        super().__init__(f"There is no {variable_name} variable in platform {platform}")


class CannotMergeMetadataRegistryError(ApiError):
    def __init__(
            self,
            key: str,
            value: Any | None,
            reason: str | None =
            None, bad_metadata_registry: apis.models.virtual_experiment.MetadataRegistry | None = None
    ):
        self.key = key
        self.value = value
        self.reason = reason
        self.bad_metadata_registry = bad_metadata_registry

        msg = f"Cannot merge() metadata registries due to key {key} = {value}"
        if reason:
            msg = ". ".join((msg, self.reason))
        super(CannotMergeMetadataRegistryError, self).__init__(msg)


class TransformationError(ApiError):
    def __init__(self, msg: str):
        super().__init__(msg)


class TransformationUnknownVariableError(TransformationError):
    def __init__(self, variable: str, package: str, extra_msg: str):
        self.variable = variable
        self.package = package
        self.extra_msg = extra_msg

        msg = f"The package {package} does not contain the variable {variable}"

        if extra_msg:
            msg = ". ".join((msg, extra_msg))

        super().__init__(msg=msg)


class TransformationManyErrors(TransformationError):
    def __init__(self, problems: List[Exception]):
        self.problems = list(problems)

        msg = f"There are {len(problems)} problems. Problems follow:\n"
        msg += "\n".join((str(e) for e in problems))

        super().__init__(msg=msg)


class InvalidElaunchParameter(ApiError):
    pass


class OverrideResourcesError(ApiError):
    def __init__(self, offending_key: str, overridden_key: str):
        self.offending_key = offending_key
        self.overridden_key = overridden_key

        super(OverrideResourcesError, self).__init__(f"{offending_key} overrides {overridden_key}")


class InconsistentPlatformError(ApiError):
    def __init__(self, platform: str, reason: str, error: Exception | None = None):
        self.platform = platform
        self.reason = reason
        self.error = error

        msg = f"Platform {platform} contains inconsistent information"
        if reason:
            msg = " - ".join((msg, reason))

        super(InconsistentPlatformError, self).__init__(msg)


class InvalidElaunchParameterChoices(InvalidElaunchParameter):
    def __init__(self, name: str, value: str, valid_values: List[str], msg: str | None = None):
        self.name = name
        self.value = value
        self.valid_values = valid_values

        if not msg:
            msg = f"Invalid additionalOption --{self.name}={self.value}, set it to one from {self.valid_values}"

        super(InvalidElaunchParameterChoices, self).__init__(msg)


class ManyInvalidElaunchParameters(InvalidElaunchParameter):
    def __init__(self, exceptions: List[Exception]):
        msg = ': '.join((f"{len(exceptions)} elaunch argument problems", '. '.join((str(x) for x in exceptions))))

        self.exceptions = exceptions
        super(ManyInvalidElaunchParameters, self).__init__(msg)


class OverrideVariableError(ApiError):
    def __init__(self, name: str, value: str, msg: str):
        self.name = name
        self.value = value

        super(OverrideVariableError, self).__init__(msg)


class InvalidPayloadError(ApiError):
    def __init__(self, msg: str):
        super(InvalidPayloadError, self).__init__(msg)


class InvalidPayloadExperimentStartError(ApiError):
    def __init__(self, msg: str):
        super(InvalidPayloadExperimentStartError, self).__init__(msg)


class OverrideDataFilesError(InvalidPayloadExperimentStartError):
    def __init__(self, names: List[str], msg: str):
        self.names = names
        if names:
            msg += f". Filenames are {names}"
        super(OverrideDataFilesError, self).__init__(msg)


class OverridePlatformError(InvalidPayloadExperimentStartError):
    def __init__(self, payload_platform: str, msg: str):
        self.payload_platform = payload_platform

        super(OverridePlatformError, self).__init__(msg)


class InvalidInputsError(InvalidPayloadExperimentStartError):
    def __init__(self, missing_inputs: List[str], extra_inputs: List[str]):
        self.missing_inputs = list(missing_inputs)
        self.extra_inputs = extra_inputs

        msg = "Invalid experiment start payload."
        if missing_inputs:
            msg += f" Missing inputs: {missing_inputs}."
        if extra_inputs:
            msg += f" Extra inputs: {extra_inputs}"

        super().__init__(msg)


class DBError(ApiError):
    pass


class ParameterisedPackageNotFoundError(DBError):
    def __init__(self, identifier: str):
        self.identifier = identifier
        super(ParameterisedPackageNotFoundError, self).__init__(
            f"Cannot find Parameterised Package \"{identifier}\"")


class CannotRemoveLatestTagError(DBError):
    def __init__(self, identifier: str):
        self.identifier = identifier
        super(CannotRemoveLatestTagError, self).__init__(
            f"Cannot remove the latest tag from Parameterised Package \"{identifier}\"")


class RelationshipNotFoundError(DBError):
    def __init__(self, identifier: str):
        self.identifier = identifier
        super().__init__(f"Cannot find relationship {identifier}")
