#!/usr/bin/env python
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Theo Kanakis

import yaml

from app import api, app
from pathlib import Path


app.config["SERVER_NAME"] = "localhost"

with app.app_context():
    schema = api.__schema__
    schema.pop('host')

    with open(Path.cwd()/'docs/openapi.yaml','w') as filestream:
        filestream.write(yaml.safe_dump(schema))
