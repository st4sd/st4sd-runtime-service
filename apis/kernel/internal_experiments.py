# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

import pathlib
import typing

import pydantic
import yaml

import apis.db.exp_packages
import apis.db.secrets
import apis.k8s
import apis.models.errors
import apis.models.virtual_experiment
import apis.runtime.errors
import apis.storage
import apis.storage.actuators
import apis.storage.actuators.s3

import apis.kernel.experiments

import apis.models.common

import experiment.model.frontends.dsl
import experiment.model.frontends.flowir
import experiment.model.errors

import apis.runtime.package


class S3StorageSecret(apis.models.common.Digestable):
    S3_BUCKET: str
    S3_ENDPOINT: str
    S3_ACCESS_KEY_ID: typing.Optional[str] = None
    S3_SECRET_ACCESS_KEY: typing.Optional[str] = None
    S3_REGION: typing.Optional[str] = None


def validate_dsl(
    dsl2_definition: typing.Dict[str, typing.Any],
):
    """Validates a DSl 2.0 definition of a workflow

    Args:
        dsl2_definition:
            the DSL 2.0 definition

    Raises:
        apis.models.errors.InvalidModelError:
            If the DSL is invalid
    """
    try:
        namespace = experiment.model.frontends.dsl.Namespace(**dsl2_definition)
    except pydantic.ValidationError as e:
        raise apis.models.errors.InvalidModelError.from_pydantic("Invalid DSL definition", e)

    try:
        flowir = experiment.model.frontends.dsl.namespace_to_flowir(namespace)
    except experiment.model.errors.DSLInvalidError as e:
        raise apis.models.errors.InvalidModelError.from_pydantic("Invalid DSL definition", e)

    errors = flowir.validate()

    if errors:
        raise apis.models.errors.InvalidModelError("Invalid DSL definition", problems=[{
            "problem": str(e)
        } for e in errors])


class PVEPForDSL:
    def __init__(
        self,
        pvep: apis.models.virtual_experiment.ParameterisedPackage,
        changes: typing.List[typing.Dict[str, typing.Any]]
    ):
        self.pvep = pvep
        self.changes = changes


class PVEPChange:
    def __init__(self, message: str, location: typing.List[typing.Union[str, int]]):
        self.message = message
        self.location = location

    def dict(self) -> typing.Dict[str, typing.Any]:
        return {"message": self.message, "location": self.location}


def _try_mirror_parameterisation(
    source: typing.List[typing.Union[apis.models.common.Option, apis.models.common.OptionMany]],
    destination: typing.List[typing.Union[apis.models.common.Option, apis.models.common.OptionMany]],
    container_type: str,
    setting_type: str,
    available: typing.List[str],
    changes: typing.List[typing.Dict[str, typing.Any]],
):
    for (idx, setting) in enumerate(source):
        if setting.name not in available:
            changes.append(
                PVEPChange(message=f'Removed {container_type} for {setting_type.rstrip("s")} {setting.name}',
                           location=["parameterisation", container_type, setting_type, idx]
                           ).dict())
        else:
            destination.append(setting)
            changes.append(
                PVEPChange(message=f'Inherited {container_type} for {setting_type.rstrip("s")} {setting.name} '
                                   f'from template',
                           location=["parameterisation", container_type, setting_type, idx]
                           ).dict())


def _try_mirror_parameterisation_variable(
    source: typing.List[typing.Union[apis.models.common.Option, apis.models.common.OptionMany]],
    destination: typing.List[typing.Union[apis.models.common.Option, apis.models.common.OptionMany]],
    container_type: str,
    available_variables: typing.List[str],
    changes: typing.List[typing.Dict[str, typing.Any]],
):
    return _try_mirror_parameterisation(
        source=source,
        destination=destination,
        container_type=container_type,
        setting_type="variables",
        available=available_variables,
        changes=changes,
    )


def _try_mirror_parameterisation_data(
    source: typing.List[typing.Union[apis.models.common.Option, apis.models.common.OptionMany]],
    destination: typing.List[typing.Union[apis.models.common.Option, apis.models.common.OptionMany]],
    container_type: str,
    available_data: typing.List[str],
    changes: typing.List[typing.Dict[str, typing.Any]],
):
    return _try_mirror_parameterisation(
        source=source,
        destination=destination,
        container_type=container_type,
        setting_type="data",
        available=available_data,
        changes=changes,
    )



def generate_pvep_for_dsl(
    dsl2_definition: typing.Dict[str, typing.Any],
    template: typing.Optional[apis.models.virtual_experiment.ParameterisedPackage],
) -> PVEPForDSL:
    """Generates the default parameterised virtual experiment package for a DSL 2 workflow using an optional template

    Args:
        dsl2_definition:
            the DSL 2.0 definition
        template:
            the parameterised virtual experiment package template

    Returns:
        A default parameterised virtual experiment package definition along with any changes made to the template
    """
    # VV: This is just a dummy PVEP with a name and a base package, everything, everything else will be auto generated
    pvep = apis.models.virtual_experiment.ParameterisedPackage(
        base=apis.models.virtual_experiment.VirtualExperimentBase(packages=[{ "source": {}}])
    )
    pvep.metadata.package.name = "anonymous"

    new_pvep = validate_internal_experiment(dsl2_definition=dsl2_definition, pvep=pvep)

    new_pvep.metadata.registry.createdOn = None

    changes = []

    available_variables = [x.name for x in new_pvep.metadata.registry.executionOptionsDefaults.variables]
    available_data = [x.name for x in new_pvep.metadata.registry.data]

    if template:
        changes.append(PVEPChange(message="Erased base.packages", location=["base", "packages"]).dict())
        changes.append(PVEPChange(message="Updated metadata.registry", location= ["metadata", "registry"]).dict())
        changes.append(
            PVEPChange(message=f'Inherited metadata.package from template',location=["metadata", "package"]).dict())

        new_pvep.metadata.package = template.metadata.package

        if "internal-experiment" not in new_pvep.metadata.package.keywords:
            new_pvep.metadata.package.keywords.append("internal-experiment")

        _try_mirror_parameterisation_variable(
            source=template.parameterisation.presets.variables,
            destination=new_pvep.parameterisation.presets.variables,
            container_type="presets",
            available_variables=available_variables,
            changes=changes
        )

        _try_mirror_parameterisation_variable(
            source=template.parameterisation.executionOptions.variables,
            destination=new_pvep.parameterisation.executionOptions.variables,
            container_type="executionOptions",
            available_variables=available_variables,
            changes=changes
        )

        _try_mirror_parameterisation_data(
            source=template.parameterisation.presets.data,
            destination=new_pvep.parameterisation.presets.data,
            container_type="presets",
            available_data=available_data,
            changes=changes
        )

        _try_mirror_parameterisation_data(
            source=template.parameterisation.executionOptions.data,
            destination=new_pvep.parameterisation.executionOptions.data,
            container_type="executionOptions",
            available_data=available_data,
            changes=changes
        )

    return PVEPForDSL(
        pvep = new_pvep,
        changes = changes
    )


def validate_internal_experiment(
    dsl2_definition: typing.Dict[str, typing.Any],
    pvep: apis.models.virtual_experiment.ParameterisedPackage,
) -> apis.models.virtual_experiment.ParameterisedPackage:
    """Validates a DSL 2.0 specification against its PVEP

    Args:
        dsl2_definition:
            the DSL 2.0 definition
        pvep:
            The parameterised virtual experiment package definition

    Returns:
        The auto-updated PVEP

    Raises:
        apis.models.errors.InvalidModelError:
            If the DSL is invalid
        apis.models.errors.ApiError:
            If the DSL is not compatible with the PVEP
    """
    try:
        namespace = experiment.model.frontends.dsl.Namespace.validate(dsl2_definition)
    except pydantic.ValidationError as e:
        raise apis.models.errors.InvalidModelError.from_pydantic("Invalid DSL definition", e)

    try:
        concrete = experiment.model.frontends.dsl.namespace_to_flowir(namespace)
        concrete.validate()
    except experiment.model.errors.FlowIRConfigurationErrors as e:
        raise apis.models.errors.InvalidModelError("Invalid DSL definition", [
            {
                "location": [],
                "message": str(e)
            } for e in e.underlyingErrors
        ])
    except experiment.model.errors.DSLInvalidError as e:
        raise apis.models.errors.InvalidModelError.from_pydantic("Invalid DSL definition", e)

    pvep = pvep.copy(deep=True)

    if len(pvep.base.packages) != 1:
        raise apis.models.errors.InvalidPayloadError("The PVEP must point to exactly 1 base package")

    download = apis.storage.PackageMetadataCollection(
        {pvep.base.packages[0].name: apis.models.virtual_experiment.StorageMetadata(
            concrete=concrete,
            manifestData={},
            data=[],
        )}
    )

    # VV: Run all the tests without actually accessing the internal storage to retrieve the source code of the workflow
    metadata = apis.runtime.package.access_and_validate_virtual_experiment_packages(
        ve=pvep,
        packages=download
    )
    apis.runtime.package.validate_parameterised_package(ve=pvep, metadata=metadata)

    return pvep


def store_internal_experiment(
    dsl2_definition: typing.Dict[str, typing.Any],
    pvep: apis.models.virtual_experiment.ParameterisedPackage,
    dest_storage: typing.Optional[apis.storage.actuators.Storage] = None,
    db_secrets: typing.Optional[apis.db.secrets.DatabaseSecrets] = None,
    dest_path: typing.Union[pathlib.Path, str] = pathlib.Path("experiments"),
):
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

    Raises:
        apis.models.errors.ApiError:
            If unable to upload files
    """

    if not isinstance(dest_path, pathlib.Path):
        dest_path = pathlib.Path(dest_path)

    if dest_storage is None:
        dest_storage = apis.storage.actuators.storage_actuator_for_package(pvep.base.packages[0], db_secrets=db_secrets)

    root_dir = dest_path / pvep.metadata.package.name
    # VV: FIXME Store DSL 2.0 here
    conf_file = root_dir / "conf/dsl.yaml"

    yaml_dsl2_def: str = yaml.safe_dump(dsl2_definition, indent=2)

    try:
        dest_storage.remove(root_dir)
    except FileNotFoundError:
        pass
    dest_storage.write(conf_file, yaml_dsl2_def.encode("utf-8"))

    return pvep


def point_base_package_to_s3_storage(
    pvep: apis.models.virtual_experiment.ParameterisedPackage,
    credentials: apis.models.virtual_experiment.SourceS3SecurityCredentials,
    location: apis.models.virtual_experiment.BasePackageSourceS3Location,
    dest_path: typing.Union[pathlib.Path, str] = pathlib.Path("experiments"),
):
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

    pvep.base.packages = [
        apis.models.virtual_experiment.BasePackage(source=apis.models.virtual_experiment.BaseSource(
            s3=apis.models.virtual_experiment.BasePackageSourceS3(
                security=apis.models.virtual_experiment.BasePackageSourceS3Security(credentials=credentials, ),
                location=location, )),
            config=apis.models.virtual_experiment.BasePackageConfig(
                path=(dest_path / pvep.metadata.package.name).as_posix().lstrip("/").rstrip("/")
            )
        )
    ]

    return pvep


def get_s3_internal_storage_secret(
    secret_name: str,
    db_secrets: apis.db.secrets.DatabaseSecrets,
) -> S3StorageSecret:
    """Extracts the S3 Credentials and Location from a Secret in a secret database

    The keys in the Secret are

    - S3_BUCKET: str
    - S3_ENDPOINT: str
    - S3_ACCESS_KEY_ID: typing.Optional[str] = None
    - S3_SECRET_ACCESS_KEY: typing.Optional[str] = None
    - S3_REGION: typing.Optional[str] = None

    Args:
        secret_name:
            The name containing the information
        db_secrets:
            A reference to the Secrets database
    Returns:
        The contents of the secret

    Raises:
        apis.models.errors.DBError:
            When the secret is not found or it contains invalid information
    """

    with db_secrets:
        secret = db_secrets.secret_get(secret_name)

    if not secret:
        raise apis.models.errors.DBError(f"There is no S3 Secret {secret_name}")

    try:
        return S3StorageSecret(**secret.data)
    except pydantic.ValidationError as e:
        problems = apis.models.errors.make_pydantic_errors_jsonable(e)
        raise apis.models.errors.DBError(f"The S3 Secret {secret_name} is invalid. Errors follow: {problems}")

def generate_s3_package_source_from_secret(
    secret_name: str,
    db_secrets: apis.db.secrets.DatabaseSecrets,
) -> apis.models.virtual_experiment.BasePackageSourceS3:
    """Extracts the S3 Credentials and Location from a Secret in a secret database

    The keys in the Secret are

    - S3_BUCKET: str
    - S3_ENDPOINT: str
    - S3_ACCESS_KEY_ID: typing.Optional[str] = None
    - S3_SECRET_ACCESS_KEY: typing.Optional[str] = None
    - S3_REGION: typing.Optional[str] = None

    Args:
        secret_name:
            The name containing the information
        db_secrets:
            A reference to the Secrets database
    Returns:
        The BasePackageSource
    """

    secret =get_s3_internal_storage_secret(secret_name=secret_name, db_secrets=db_secrets)

    return apis.models.virtual_experiment.BasePackageSourceS3(
        security=apis.models.virtual_experiment.BasePackageSourceS3Security(
            credentials=apis.models.virtual_experiment.SourceS3SecurityCredentials(
                valueFrom=apis.models.virtual_experiment.SourceS3SecurityCredentialsValueFrom(
                    secretName=secret_name,
                    keyAccessKeyID="S3_ACCESS_KEY_ID" if secret.S3_ACCESS_KEY_ID else None,
                    keySecretAccessKey="S3_SECRET_ACCESS_KEY" if secret.S3_SECRET_ACCESS_KEY else None,
                )
            )
        ),
        location=apis.models.virtual_experiment.BasePackageSourceS3Location(
            bucket=secret.S3_BUCKET,
            endpoint=secret.S3_ENDPOINT,
            region=secret.S3_REGION
        )
    )

def upsert_internal_experiment(
    dsl2_definition: typing.Dict[str, typing.Any],
    pvep: apis.models.virtual_experiment.ParameterisedPackage,
    db_secrets: apis.db.secrets.DatabaseSecrets,
    db_experiments: apis.db.exp_packages.DatabaseExperiments,
    package_source: typing.Union[apis.models.virtual_experiment.BasePackageSourceS3, str],
    dest_path: typing.Union[pathlib.Path, str] = pathlib.Path("experiments"),
) -> apis.models.virtual_experiment.ParameterisedPackage:
    """Upserts a Parameterised Virtual Experiment Package for a DSL that will also be stored on the internal storage

    Args:
        dsl2_definition:
            The DSL 2 definition of the experiment
        pvep:
            The definition of the base package - the method updates this object in place
        package_source:
            The S3 "location" and "security" if parameter is a string then it is interpreted as the name of a
            Secret in @db_secrets which contains the location and security information.
        dest_path:
            The path inside the S3 bucket under which the workflow definition will be stored
        db_secrets:
            The database containing Secrets
        db_experiments:
            The database containing experiments

    Returns:
        The parameterised virtual experiment package
    """

    if not pvep.metadata.package.name:
        raise apis.models.errors.ApiError('Missing "pvep.metadata.package.name"')

    if isinstance(package_source, str):
        package_source = generate_s3_package_source_from_secret(
            secret_name=package_source,
            db_secrets=db_secrets
        )

    pvep = point_base_package_to_s3_storage(
        pvep=pvep,
        credentials=package_source.security.credentials,
        location=package_source.location,
        dest_path=dest_path
    )

    validate_internal_experiment(
        dsl2_definition=dsl2_definition,
        pvep=pvep,
    )

    store_internal_experiment(
        dsl2_definition=dsl2_definition,
        pvep=pvep,
        db_secrets=db_secrets,
        dest_path=dest_path
    )


    download = apis.storage.PackagesDownloader(pvep, db_secrets=db_secrets)

    return apis.kernel.experiments.validate_and_store_pvep_in_db(
        package_metadata_collection=download,
        parameterised_package=pvep,
        db=db_experiments,
        is_internal_experiment=True
    )
