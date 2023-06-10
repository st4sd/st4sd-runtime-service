# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

import tinydb.table

import apis.k8s
import apis.db.base
import apis.models.common
import apis.models.errors
import base64

import kubernetes.client

from typing import (
    Dict,
    Any,
    Optional,
)


class Secret(apis.models.common.DigestableBase):
    name: str
    data: Dict[str, str]


class SecretKubernetes(Secret):
    secretKind: str = "generic"


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

    def secret_get(self, name: str) -> Optional[Dict[str, Any]]:
        docs = self.query(self.construct_query(name=name))

        if len(docs) > 1:
            raise apis.models.errors.DBError(f"Multiple Secrets with name {name}")

        if len(docs) == 1:
            return docs[0]

    def secret_create(self, secret: Secret):
        existing = self.secret_get(secret.name)
        if existing is None:
            self.insert_many([secret.dict()])
        else:
            raise apis.models.errors.DBError(f"Cannot create Secret {secret.name} because it already exists")

    def secret_upsert(self, secret: Secret):
        return self.upsert(secret.dict(), ql=self.construct_query(secret.name))

    def secret_delete(self, name: str):
        return self.delete(self.construct_query(name))


class KubernetesSecrets(SecretsStorageTemplate):
    def __init__(self, namespace: str):
        self.namespace = namespace

    def secret_get(self, name: str) -> Dict[str, Any]:
        api = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient())

        secrets: kubernetes.client.V1SecretList = api.list_namespaced_secret(
            self.namespace, field_selector=f'metadata.name={name}')

        if len(secrets.items) == 0:
            raise apis.k8s.errors.KubernetesObjectNotFound(k8s_kind='secret', k8s_name=name)

        k8s_secret: kubernetes.client.V1Secret = secrets.items[0]

        data = {key: base64.standard_b64decode(k8s_secret.data[key]).decode() for key in k8s_secret.data}

        return {"name": name, "data": data}

    def secret_create(self, secret: SecretKubernetes):
        data = {x: b64_encode(secret.data[x]) for x in secret.data}

        api = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient())

        k8s_secret = kubernetes.client.models.V1Secret(
            data=data,
            type=secret.secretKind,
            metadata=kubernetes.client.models.V1ObjectMeta(
                name=secret.name, labels={'creator': 'st4sd-runtime-service'})
        )

        api.create_namespaced_secret(namespace=self.namespace, body=k8s_secret)

    def secret_delete(self, name: str):
        api = kubernetes.client.CoreV1Api(kubernetes.client.ApiClient())

        try:
            api.delete_namespaced_secret(name=name, namespace=self.namespace)
        except kubernetes.client.exceptions.ApiException as e:
            raise apis.models.errors.DBError(
                f"Unable to delete K8s secret {self.namespace}/{name} due to {e}")
