# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

from __future__ import annotations

import os
import pathlib
import typing


class PathInfo:
    def __init__(self, name: typing.Optional[str] = None, isdir: typing.Optional[bool] = None,
                 isfile: typing.Optional[bool] = None, ):
        self.name = name
        self.isdir = isdir
        self.isfile = isfile


class Storage:
    @classmethod
    def as_posix(cls, path: typing.Union[pathlib.Path, str]) -> str:
        if isinstance(path, str):
            return path
        return path.as_posix()

    def copy(
            self,
            source: Storage,
            source_path: typing.Union[pathlib.Path, str],
            dest_path: typing.Union[pathlib.Path, str]
    ):
        """Copies files from source into self

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
                contents = source.read(path)
                self.write(path=new_path, contents=contents)
                del contents
            else:
                to_copy.extend([os.path.join(path, x.name) for x in source.listdir(path)])

    def exists(self, path: typing.Union[pathlib.Path, str]) -> bool:
        raise NotImplementedError()

    def isfile(self, path: typing.Union[pathlib.Path, str]) -> bool:
        raise NotImplementedError()

    def isdir(self, path: typing.Union[pathlib.Path, str]) -> bool:
        raise NotImplementedError()

    def listdir(self, path: typing.Union[pathlib.Path, str]) -> typing.Iterator[PathInfo]:
        """Iterates the paths under a directory, does not recursively visit sub-dirs"""
        raise NotImplementedError()

    def read(self, path: typing.Union[pathlib.Path, str]) -> bytes:
        raise NotImplementedError()

    def write(self, path: typing.Union[pathlib.Path, str], contents: bytes):
        raise NotImplementedError()

    def remove(self, path: typing.Union[pathlib.Path, str]):
        """Removes files and directories"""
        raise NotImplementedError()

    def store_to_file(self, src: typing.Union[pathlib.Path, str], dest: typing.Union[pathlib.Path, str]):
        """Stores a @src to a @dest file on the local storage"""
        raise NotImplementedError()