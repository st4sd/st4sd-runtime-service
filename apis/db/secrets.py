# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

import base64
from typing import (
    Dict,
    Any,
    Optional,
)

import kubernetes.client
import tinydb.table

import apis.db.base
import apis.k8s
import apis.models.common
import apis.models.errors

import pydantic


class Secret(apis.models.common.DigestableBase):
    name: str
    data: Dict[str, str]


class SecretKubernetes(Secret):
    secretKind: str = "generic"


class S3StorageSecret(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")

    S3_BUCKET: str
    S3_ENDPOINT: str
    S3_ACCESS_KEY_ID: Optional[str] = None
    S3_SECRET_ACCESS_KEY: Optional[str] = None
    S3_REGION: Optional[str] = None


def b64_encode(which):
    # type: (str) -> str
    return base64.standard_b64encode(which.encode('utf-8')).decode('utf-8')


class SecretsStorageTemplate:
    def secret_get(self, name: str) -> Secret:
        raise NotImplementedError()

    def secret_create(self, secret: Secret):
        raise NotImplementedError()

    def secret_delete(self, name: str):
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class DatabaseSecrets(apis.db.base.Database, SecretsStorageTemplate):
    def __init__(self, db_path: str, db_label: str = "db_secrets"):
        super().__init__(db_path=db_path, db_label=db_label)

    @classmethod
    def construct_query(cls, name: str) -> tinydb.table.QueryLike:
        entry = tinydb.Query()
        return entry.name == name

    def secret_get(self, name: str) -> Optional[Secret]:
        docs = self.query(self.construct_query(name=name))

        if len(docs) > 1:
            raise apis.models.errors.DBError(f"Multiple Secrets with name {name}")

        if len(docs) == 1:
            data = {key: base64.standard_b64decode(value).decode() for key, value in docs[0].get("data", {}).items()}
            return Secret(name=name, data=data)

    def secret_create(self, secret: Secret):
        existing = self.secret_get(secret.name)
        if existing is None:
            data = {key: b64_encode(value) for key, value in secret.data.items()}
            self.insert_many([{"name": secret.name, "data": data}])
        else:
            raise apis.models.errors.DBError(f"Cannot create Secret {secret.name} because it already exists")

    def secret_upsert(self, secret: Secret):
        return self.upsert(secret.model_dump(), ql=self.construct_query(secret.name))

    def secret_delete(self, name: str):
        return self.delete(self.construct_query(name))


class KubernetesSecrets(SecretsStorageTemplate):
    def __init__(self, namespace: str):
        self.namespace = namespace

    def secret_get(self, name: str) -> Optional[SecretKubernetes]:
        api = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient())

        secrets: kubernetes.client.V1SecretList = api.list_namespaced_secret(
            self.namespace, field_selector=f'metadata.name={name}')

        if len(secrets.items) == 0:
            return None

        k8s_secret: kubernetes.client.V1Secret = secrets.items[0]

        data = {key: base64.standard_b64decode(k8s_secret.data[key]).decode() for key in k8s_secret.data}

        return SecretKubernetes(name=name, data=data)

    def secret_create(self, secret: SecretKubernetes):
        if not isinstance(secret, SecretKubernetes):
            secret = SecretKubernetes(
                data=secret.data,
                name=secret.name,
            )
        data = {x: b64_encode(secret.data[x]) for x in secret.data}

        api = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient())

        k8s_secret = kubernetes.client.models.V1Secret(
            data=data,
            type=secret.secretKind,
            metadata=kubernetes.client.models.V1ObjectMeta(
                name=secret.name, labels={'creator': 'st4sd-runtime-service'})
        )

        try:
            api.create_namespaced_secret(namespace=self.namespace, body=k8s_secret)
        except kubernetes.client.exceptions.ApiException as e:
            raise apis.models.errors.DBError(
                f"Unable to create K8s Secret {self.namespace}/{secret.name} due to {e}"
            )

    def secret_delete(self, name: str):
        api = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient())

        try:
            api.delete_namespaced_secret(name=name, namespace=self.namespace)
        except kubernetes.client.exceptions.ApiException as e:
            raise apis.models.errors.DBError(
                f"Unable to delete K8s secret {self.namespace}/{name} due to {e}")


def get_s3_secret(
    secret_name: str,
    db_secrets: DatabaseSecrets,
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
        raise apis.models.errors.DBNotFoundError(secret_name)

    try:
        return apis.db.secrets.S3StorageSecret(**secret.data)
    except pydantic.ValidationError as e:
        problems = apis.models.errors.make_pydantic_errors_jsonable(e)
        raise apis.models.errors.DBError(f"The S3 Secret {secret_name} is invalid. Errors follow: {problems}")
