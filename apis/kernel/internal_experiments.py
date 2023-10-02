# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

import pathlib
import typing

import yaml

import apis.db.secrets
import apis.k8s
import apis.models.errors
import apis.models.virtual_experiment
import apis.runtime.errors
import apis.storage
import apis.storage.actuators
import apis.storage.actuators.s3


def validate_internal_experiment(
    dsl2_definition: typing.Dict[str, typing.Any],
    pvep: apis.models.virtual_experiment.ParameterisedPackage,
):
    """Validates a DSL 2.0 specification against its PVEP

    Args:
        dsl2_definition:
            the DSL 2.0 definition
        pvep:
            The parameterised virtual experiment package definition
    """
    # VV: FIXME Validate DSL 2.0 against the PVEP here
    pass


def store_internal_experiment(dsl2_definition: typing.Dict[str, typing.Any],
        pvep: apis.models.virtual_experiment.ParameterisedPackage,
        dest_storage: typing.Optional[apis.storage.actuators.Storage] = None,
        db_secrets: typing.Optional[apis.db.secrets.DatabaseSecrets] = None,
        dest_path: typing.Union[pathlib.Path, str] = pathlib.Path("experiments"), ):
    """Validates that an experiment is consistent with a PVEP and then stores its definition in the
    storage.

    Args:
        dsl2_definition:
            The DSL 2 definition of the experiment
        dest_storage:
            An actuator for storing the files in the destination (i.e. "internal") storage.
            If None, it gets auto-generating by invoking
            storage_actuator_for_package(pvep.base.packages[0], db_secrets)
        pvep:
            The parameterised virtual experiment package definition.
            It must already be configured to with a single package that points
            to where @dest_storage will copy the files in. The method will validate that the PVEP is
            consistent with the DSL.
        dest_path:
            The location of packages in @dest_storage, the package will actually be stored under
            ${dest_path}/${pvep.metadata.package.name}/
            The method assumes that pvep.base.packages[0] is already configured in way that's compatible
            with the value of dest_path
        db_secrets:
            When @dest_storage is None, and the package requires information stored in a secret, also provide
            a reference to the Secrets database

    Returns:
        The PVEP of the newly created experiment
    """

    if not isinstance(dest_path, pathlib.Path):
        dest_path = pathlib.Path(dest_path)

    if dest_storage is None:
        dest_storage = apis.storage.actuators.storage_actuator_for_package(pvep.base.packages[0], db_secrets=db_secrets)

    root_dir = dest_path / pvep.metadata.package.name
    # VV: FIXME Store DSL 2.0 here
    conf_file = root_dir / "conf/flowir_package.yaml"

    yaml_dsl2_def: str = yaml.safe_dump(dsl2_definition, indent=2)

    try:
        dest_storage.remove(root_dir)
    except FileNotFoundError:
        pass
    dest_storage.write(conf_file, yaml_dsl2_def.encode("utf-8"))

    return pvep


def point_base_package_to_s3_storage(pvep: apis.models.virtual_experiment.ParameterisedPackage,
        credentials: apis.models.virtual_experiment.SourceS3SecurityCredentials,
        location: apis.models.virtual_experiment.BasePackageSourceS3Location,
        dest_path: typing.Union[pathlib.Path, str] = pathlib.Path("experiments"), ):
    """Updates a PVEP by pointing its base package to the s3 storage

    Args:
        pvep:
            The definition of the base package - the method updates this object in place
        credentials:
            The S3 credentials
        location:
            The S3 location
        dest_path:
            The location of packages in S3, the package will actually be stored under
            ${dest_path}/${pvep.metadata.package.name}/

    Returns:
        The PVEP object
    """

    if not isinstance(dest_path, pathlib.Path):
        dest_path = pathlib.Path(dest_path)

    pvep.base.packages = [apis.models.virtual_experiment.BasePackage(source=apis.models.virtual_experiment.BaseSource(
        s3=apis.models.virtual_experiment.BasePackageSourceS3(
            security=apis.models.virtual_experiment.BasePackageSourceS3Security(credentials=credentials, ),
            location=location, )), config=apis.models.virtual_experiment.BasePackageConfig(
        path=(dest_path / pvep.metadata.package.name).as_posix().lstrip("/").rstrip("/")))]

    return pvep
