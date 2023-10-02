# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

from __future__ import annotations

import os
import pathlib
import shutil
import typing

from .base import (
Storage,
PathInfo,
)
class LocalStorage(Storage):

    @classmethod
    def exists(cls, path: typing.Union[pathlib.Path, str]) -> bool:
        return os.path.exists(path)

    @classmethod
    def isfile(cls, path: typing.Union[pathlib.Path, str]) -> bool:
        return os.path.isfile(path)

    @classmethod
    def isdir(cls, path: typing.Union[pathlib.Path, str]) -> bool:
        return os.path.isdir(path)

    @classmethod
    def listdir(cls, path: typing.Union[pathlib.Path, str]) -> typing.Iterator[PathInfo]:
        for p in os.scandir(path):
            yield PathInfo(name=p.name, isfile=p.is_file(), isdir=p.is_dir())

    @classmethod
    def read(cls, path: typing.Union[pathlib.Path, str]) -> bytes:
        with open(path, 'rb') as f:
            return f.read()

    @classmethod
    def write(cls, path: typing.Union[pathlib.Path, str], contents: bytes):
        path_dir = os.path.split(path)[0]

        if path_dir and not os.path.exists(path_dir):
            os.makedirs(path_dir, exist_ok=True)

        with open(path, 'wb') as f:
            return f.write(contents)

    @classmethod
    def remove(cls, path: typing.Union[pathlib.Path, str]):
        shutil.rmtree(path, ignore_errors=True)

    def store_to_file(self, src: typing.Union[pathlib.Path, str], dest: typing.Union[pathlib.Path, str]):
        """Stores a @src to a @dest file on the local storage"""
        if not self.isfile(src):
            raise FileNotFoundError(src)

        shutil.copyfile(src=src, dst=dest, follow_symlinks=True)


    def copy(
            self,
            source: Storage,
            source_path: typing.Union[pathlib.Path, str],
            dest_path: typing.Union[pathlib.Path, str]
    ):
        """Copies files from source into self

        This optimized implementation uses `store_to_file()` instead of calling source.read() and self.write()

        Arguments:
            source:
                the container of the source files
            source_path:
                the path prefix to the source files
            dest_path:
                the path prefix for the destination files
        """

        to_copy = [source_path]

        while to_copy:
            path = to_copy.pop(0)
            path = source.as_posix(path)

            if source.isfile(path):
                new_path = os.path.relpath(path, source_path)
                new_path = os.path.join(dest_path, new_path)

                path_dir = os.path.split(new_path)[0]

                if path_dir and not os.path.exists(path_dir):
                    os.makedirs(path_dir, exist_ok=True)

                source.store_to_file(src=path, dest=new_path)
            else:
                to_copy.extend([os.path.join(path, x.name) for x in source.listdir(path)])
