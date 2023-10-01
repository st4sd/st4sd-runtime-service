# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


import io
import os
import pathlib
import zipfile

import apis.storage
import apis.storage.actuators
import apis.storage.actuators.memory
import apis.storage.actuators.local

def test_stream_zip(output_dir: str):
    os.makedirs(os.path.join(output_dir, "in", "a", "b"))

    with open(os.path.join(output_dir, "in", "hello"), 'w') as f:
        f.write("hello")

    with open(os.path.join(output_dir, "in", "a", "b", "world"), 'w') as f:
        f.write("world")

    x = apis.storage.IterableStreamZipOfDirectory(os.path.join(output_dir, "in"))

    y = b""
    for chunk in x:
        y += chunk

    z = zipfile.ZipFile(io.BytesIO(y), mode='r')

    files = {k: z.read(k) for k in z.namelist()}

    assert files['/hello'] == b"hello"
    assert files['/a/b/world'] == b"world"


def test_inmemory_initialize():
    contents = "hello".encode()
    memory = apis.storage.actuators.memory.InMemoryStorage({"path/to/file": contents, "to/dir": None})

    assert memory.files == {
        "/": None,
        "/path/": None,
        "/path/to/": None,
        "/path/to/file": contents,
        "/to/": None,
        "/to/dir/": None,
    }

def test_inmemory_initialize_empty():
    memory = apis.storage.actuators.memory.InMemoryStorage({})

    assert memory.files == {
        "/": None,
    }

def test_inmemory_copy_to_inmemory():
    contents = "hello".encode()
    source = apis.storage.actuators.memory.InMemoryStorage({"path/to/file": contents, "to/dir": None})

    dest = apis.storage.actuators.memory.InMemoryStorage({})

    dest.copy(source=source, source_path="/path", dest_path="/")

    assert dest.files == {
        "/": None,
        "/to/": None,
        "/to/file": contents
    }


def test_inmemory_copy_to_local(output_dir: str):
    contents = "hello".encode()
    source = apis.storage.actuators.memory.InMemoryStorage({"path/to/file": contents, "to/dir": None})

    dest = apis.storage.actuators.local.LocalStorage()
    dest.copy(source=source, source_path="/path", dest_path=output_dir)

    top_level = [p.name for p in dest.listdir(output_dir)]
    assert top_level == ["to"]

    actual_dir = [p.name for p in dest.listdir(os.path.join(output_dir, "to"))]
    assert actual_dir == ["file"]

    assert dest.read(pathlib.Path(output_dir)/"to/file") == contents


def test_local_copy_to_inmemory(output_dir: str):
    contents = "hello".encode()

    source = apis.storage.actuators.local.LocalStorage()
    dest = apis.storage.actuators.memory.InMemoryStorage({})

    os.makedirs(os.path.join(output_dir, "path/to"), exist_ok=True)

    with open(pathlib.Path(output_dir)/"path/to/file", 'wb') as f:
        f.write(contents)

    dest.copy(source=source, source_path=pathlib.Path(output_dir)/"path", dest_path="/")

    assert dest.files == {
        "/": None,
        "/to/": None,
        "/to/file": contents
    }