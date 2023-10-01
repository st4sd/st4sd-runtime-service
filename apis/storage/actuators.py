# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

from __future__ import annotations

import copy
import os
import pathlib
import shutil
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


class InMemoryStorage(Storage):
    def __init__(self, files: typing.Dict[typing.Union[pathlib.Path, str], typing.Optional[bytes]]):
        self.files = {self._tidy_path(p, v is None): v for p, v in files.items()}

        # VV: For each path/to/file: contents all directories (e.g. path/ and path/to/) must also exist
        #   path/: None
        #   path/to/ None
        self.files['/'] = None

        for path in list(self.files):
            self._ensure_dirs_to_path(path)

    def _tidy_path(self, p: typing.Union[pathlib.Path, str], is_dir: bool) -> str:
        p = self.as_posix(p)

        if is_dir and not p.endswith("/"):
            p = p + "/"

        if p.startswith("/") is False:
            p = "/" + p

        return p

    def _ensure_dirs_to_path(self, path: typing.Union[pathlib.Path, str]):
        path = self.as_posix(path)
        skip = 0
        while True:
            idx = path.find("/", skip)
            if idx == -1:
                break
            skip = idx + 1

            dir_path = path[:skip]
            self.files[dir_path] = None


    def exists(self, path: typing.Union[pathlib.Path, str]) -> bool:
        path = self.as_posix(path)
        return self.isfile(path) or self.isdir(path)

    def isfile(self, path: typing.Union[pathlib.Path, str]) -> bool:
        return self.as_posix(path) in self.files

    def isdir(self, path: typing.Union[pathlib.Path, str]) -> bool:
        path = self.as_posix(path)

        if not path.endswith("/"):
            path = path + "/"

        if path in self.files:
            return True

        for k in self.files:
            if k.startswith(path):
                return True

        return False


    def listdir(self, path: typing.Union[pathlib.Path, str]) -> typing.Iterator[PathInfo]:
        path = self.as_posix(path)

        if not path.endswith("/"):
            path = path + "/"

        if self.isdir(path):
            # VV: List every direct child of path
            depth = sum((1 for x in path if x == "/"))
            for p, v in self.files.items():
                if not p.startswith(path) or p == path:
                    continue
                this_depth = sum((1 for x in p if x == "/"))
                if p.endswith("/"):
                    name = os.path.split(p[:-1])[1]
                else:
                    name = os.path.split(p)[1]

                if v is None and this_depth == depth+1:
                    yield PathInfo(name=name, isdir=True, isfile=False)
                elif v is not None and this_depth == depth:
                    yield PathInfo(name=name, isdir=False, isfile=True)
        else:
            raise NotADirectoryError(path)


    def read(self, path: typing.Union[pathlib.Path, str]) -> bytes:
        path = self.as_posix(path)

        if path.endswith("/"):
            raise ValueError("Cannot read from a Directory")

        if path not in self.files:
            raise FileNotFoundError(path)

        return self.files[path]

    def write(self, path: typing.Union[pathlib.Path, str], contents: bytes):
        path = self.as_posix(path)

        if path.endswith("/"):
            raise ValueError("Cannot write to a Directory")

        self.files[path] = contents
        self._ensure_dirs_to_path(path)

    def remove(self, path: typing.Union[pathlib.Path, str]):
        path = self.as_posix(path)

        if path.endswith("/"):
            is_dir = True
        elif self.isdir(path):
            path += "/"
            is_dir = True
        else:
            is_dir = False

        if is_dir:
            for k in list(self.files):
                if k.startswith(path):
                    del self.files[k]
        else:
            if path not in self.files:
                raise FileNotFoundError(path)
            del self.files[path]
