# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

from __future__ import annotations

import os
import pathlib
import typing

from .base import (
Storage,
PathInfo,
)

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

                if v is None and this_depth == depth + 1:
                    yield PathInfo(name=name, isdir=True, isfile=False)
                elif v is not None and this_depth == depth:
                    yield PathInfo(name=name, isdir=False, isfile=True)
        else:
            if path in self.files:
                raise NotADirectoryError(path)
            else:
                raise FileNotFoundError(path)

    def read(self, path: typing.Union[pathlib.Path, str]) -> bytes:
        path = self.as_posix(path)

        if path.endswith("/"):
            raise ValueError("Cannot read from a Directory")

        if path not in self.files:
            raise FileNotFoundError(path)

        return self.files[path]

    def write(self, path: typing.Union[pathlib.Path, str], contents: bytes):
        path = self.as_posix(path)

        if not isinstance(contents, bytes):
            raise TypeError(f"a bytes-like object is required, not {type(contents)}")

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

    def store_to_file(self, src: typing.Union[pathlib.Path, str], dest: typing.Union[pathlib.Path, str]):
        """Stores a @src to a @dest file on the local storage"""
        if not self.isfile(src):
            raise FileNotFoundError(src)

        dest = self.as_posix(dest)
        path_dir = os.path.split(dest)[0]

        if path_dir and not os.path.exists(path_dir):
            os.makedirs(path_dir, exist_ok=True)

        with open(dest, 'wb') as f:
            f.write(self.read(src))
