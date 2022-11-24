# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


import io
import os
import zipfile

import apis.storage


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
