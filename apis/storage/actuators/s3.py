# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

from __future__ import annotations

import io
import os
import pathlib
import typing

import boto3
import botocore.exceptions

from .base import (
    Storage,
    PathInfo,
)

if typing.TYPE_CHECKING:
    import botocore.client
    import boto3.resources.factory

class S3Storage(Storage):
    def __init__(
            self,
            access_key_id: typing.Optional[str],
            secret_access_key: typing.Optional[str],
            endpoint_url: typing.Optional[str],
            region_name: typing.Optional[str],
            bucket: typing.Optional[str]
    ):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.endpoint_url = endpoint_url
        self.region_name = region_name
        self.bucket = bucket

    def client(self) -> "botocore.client.S3":
        return boto3.client(
            's3',
              aws_access_key_id=self.access_key_id,
              aws_secret_access_key=self.secret_access_key,
              endpoint_url=self.endpoint_url,
              region_name=self.region_name,
        )

    def exists(self, path: typing.Union[pathlib.Path, str]) -> bool:
        # VV: TODO optimize this
        return self.isfile(path) or self.isdir(path)

    def isfile(self, path: typing.Union[pathlib.Path, str]) -> bool:
        client = self.client()

        ret = client.get_object(
            Bucket=self.bucket,
            Key=self.as_posix(path)
        )

        if ret['ResponseMetadata']['HTTPStatusCode'] == 200:
            return True

        return False

    def isdir(self, path: typing.Union[pathlib.Path, str]) -> bool:
        path =self.as_posix(path)

        if not path.endswith("/"):
            path = path + "/"

        for _ in self.listdir(path):
            # VV: if there's a single "file" with said prefix path is indeed a directory
            return True

        return False

    def listdir(self, path: typing.Union[pathlib.Path, str]) -> typing.Iterator[PathInfo]:
        client = self.client()
        path = self.as_posix(path)
        if path.endswith("/"):
            path = path + "/"

        path = path.lstrip("/")

        paginator = client.get_paginator('list_objects_v2')

        generated = set()
        # VV: This should handle "directories" which contain more than 1k files
        for page in paginator.paginate(Bucket=self.bucket, Prefix=path):
            for obj in page['Contents']:
                if not isinstance(obj, dict) or 'Key' not in obj:
                    continue

                relpath = os.path.relpath(obj['Key'], path).rstrip("/")

                if "/" in relpath:
                    relpath = relpath.split("/")[0]

                if relpath == "." or relpath in generated:
                    continue
                generated.add(relpath)

                yield relpath


    def read(self, path: typing.Union[pathlib.Path, str]) -> bytes:
        client = self.client()

        container = io.BytesIO()
        path = self.as_posix(path)
        try:
            client.download_fileobj(self.bucket, path, container)
        except botocore.exceptions.ClientError as e:
            # VV: The status code is a string
            if str(e.response.get("Error", {}).get("Code", None)) in '404':
                raise FileNotFoundError(path)
            else:
                raise

        return container.getvalue()

    def write(self, path: typing.Union[pathlib.Path, str], contents: bytes):
        client = self.client()

        container = io.BytesIO(contents)
        client.upload_fileobj(self.bucket, self.as_posix(path), container)

    def remove(self, path: typing.Union[pathlib.Path, str]):
        client = self.client()
        path = self.as_posix(path)

        to_delete = []

        if self.isdir(path):
            if path.endswith("/"):
                path = path + "/"

            if path.startswith("/"):
                path = path[1:]

            paginator = client.get_paginator('list_objects_v2')

            # VV: This should handle "directories" which contain more than 1k files
            for page in paginator.paginate(Bucket=self.bucket, Prefix=path):
                for obj in page['Contents']:
                    if not isinstance(obj, dict) or 'Key' not in obj:
                        continue
                    to_delete.append(obj['Key'])
        else:
            to_delete = [path]

        client.delete_objects(Bucket=self.bucket, Delete={"Objects": to_delete})
