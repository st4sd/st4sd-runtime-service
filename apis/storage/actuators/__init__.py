import typing

from .base import (
Storage,
PathInfo,
)


if typing.TYPE_CHECKING:
    import apis.models.virtual_experiment
    import apis.db.secrets

def storage_actuator_for_package(
    package: "apis.models.virtual_experiment.BasePackage",
    db_secrets: "apis.db.secrets.DatabaseSecrets",
) -> Storage:
    from .s3 import S3Storage
    import apis.models.errors
    import apis.runtime.errors
    import apis.k8s
    import apis.models.virtual_experiment

    if not package.source:
        raise apis.runtime.errors.RuntimeError(
            f"The package {package.name} does not have a source - cannot instantiate a Storage actuator for it")

    if package.source.s3:
        if (
                package.source.s3.security
                and package.source.s3.security.credentials
                and package.source.s3.security.credentials.value
        ):
            return S3Storage(
                bucket=package.source.s3.location.bucket,
                endpoint_url=package.source.s3.location.endpoint,
                access_key_id=package.source.s3.security.credentials.value.accessKeyID,
                secret_access_key=package.source.s3.security.credentials.value.secretAccessKey,
                region_name=package.source.s3.location.region,
            )
        elif (
                package.source.s3.security
                and package.source.s3.security.credentials
                and package.source.s3.security.credentials.valueFrom
        ):
            info = package.source.s3.security.credentials.valueFrom
            secret = db_secrets.secret_get(info.secretName)

            if not secret:
                raise apis.models.errors.DBError(
                    f"The Secret {info.secretName} containing the S3 credentials of package {package.name} is missing")

            secret = secret.data
            args = {}

            try:
                if info.keySecretAccessKey is not None:
                    args["secret_access_key"] = secret[info.keySecretAccessKey]
                if info.keyAccessKeyID is not None:
                    args["access_key_id"] = secret[info.keyAccessKeyID]
            except KeyError as e:
                raise apis.models.errors.DBError(
                    f"Missing key {e} in the Secret {info.secretName} containing the S3 credentials of "
                    f"package {package.name}")

            return S3Storage(
                bucket=package.source.s3.location.bucket,
                endpoint_url=package.source.s3.location.endpoint,
                region_name=package.source.s3.location.region,
                **args,
            )
        else:
            return S3Storage(
                bucket=package.source.s3.location.bucket,
                endpoint_url=package.source.s3.location.endpoint,
                region_name=package.source.s3.location.region,
                access_key_id=None,
                secret_access_key=None,
            )
    elif package.source.dataset:
        # VV: Convert the dataset into a s3 source and handle that instead
        credentials = apis.k8s.extract_s3_credentials_from_dataset(package.source.dataset.security.dataset)

        mock_package = package.copy(deep=True)

        mock_package.source.dataset = None
        mock_package.source.s3 = apis.models.virtual_experiment.BasePackageSourceS3(
            security=apis.models.virtual_experiment.BasePackageSourceS3Security(
                credentials=apis.models.virtual_experiment.SourceS3SecurityCredentials(
                    value=apis.models.virtual_experiment.SourceS3SecurityCredentialsValue(
                        accessKeyID=credentials.accessKeyID,
                        secretAccessKey=credentials.secretAccessKey,
                    )
                ),
            ),
            location=apis.models.virtual_experiment.BasePackageSourceS3Location(
                bucket=credentials.bucket,
                endpoint=credentials.endpoint,
                region=credentials.region,
            )
        )

        return storage_actuator_for_package(package=mock_package, db_secrets=db_secrets)
    else:
        source = [x for x in package.source.dict(exclude_none=True)]

        raise apis.runtime.errors.RuntimeError(
            f"Cannot create actuator for package {package.name} with source fields {source}")
