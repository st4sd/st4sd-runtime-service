# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis
#   Yiannis Gkoufas


from __future__ import annotations

import os
from typing import List
from typing import Union, TYPE_CHECKING

import tinydb

import apis.models.common
from utils import setup_config

if TYPE_CHECKING:
    import tinydb.table


def populate_from(file):
    configuration = setup_config()
    input_directory = configuration["inputdatadir"]
    outputTxt = os.path.join(input_directory, file)
    db = tinydb.TinyDB(outputTxt)
    return db.all()


def append_to(file, documents):
    configuration = setup_config()
    input_directory = configuration["inputdatadir"]
    outputTxt = os.path.join(input_directory, file)
    db = tinydb.TinyDB(outputTxt)
    for obj in documents:
        db.insert(obj)
    db.close()


def update(file, obj):
    configuration = setup_config()
    input_directory = configuration["inputdatadir"]
    outputTxt = os.path.join(input_directory, file)
    db = tinydb.TinyDB(outputTxt)

    db.remove(tinydb.where('id') == obj['id'])
    db.upsert(obj, tinydb.Query().id == obj['id'])
    db.close()


def generate_query(
        package_name: str,
        registry_tag: str | None = None,
        user_specified_tag: str | None = None,
        registry_digest: str | None = None) -> tinydb.table.QueryLike:
    entry = tinydb.Query()
    ql = entry.metadata.package.name == package_name

    if registry_tag:
        ql &= entry.metadata.registry.tags.any(registry_tag)

    if user_specified_tag:
        ql &= entry.metadata.package.tags.any(user_specified_tag)

    if registry_digest:
        ql &= entry.metadata.registry.digest == registry_digest

    return ql


def query(
        file: str,
        package_name: str,
        registry_tag: str | None = None,
        user_specified_tag: str | None = None,
        registry_digest: str | None = None) -> List[tinydb.table.Document]:
    configuration = setup_config()
    input_directory = configuration["inputdatadir"]
    outputTxt = os.path.join(input_directory, file)
    db = tinydb.TinyDB(outputTxt)

    ql = generate_query(package_name, registry_tag, user_specified_tag, registry_digest)

    return db.search(ql)


def query_for_identifier(file: str, identifier: str) -> List[tinydb.table.Document]:
    identifier = apis.models.common.PackageIdentifier(identifier)
    everything = identifier.parse()
    return query(file, package_name=everything.name, registry_tag=everything.tag, registry_digest=everything.digest)


def delete(
        file: str,
        package_name: str,
        registry_tag: str | None = None,
        user_specified_tag: str | None = None,
        registry_digest: str | None = None):
    """Deletes an entry with id==@obj_id

    Args:


    Returns:
        True when at least 1 entry is deleted, False when 0 entries are deleted
    """
    configuration = setup_config()
    input_directory = configuration["inputdatadir"]
    outputTxt = os.path.join(input_directory, file)
    db = tinydb.TinyDB(outputTxt)  # type: Union[tinydb.TinyDB, tinydb.table.Table]

    ql = generate_query(package_name, registry_tag, user_specified_tag, registry_digest)
    deleted = db.remove(cond=ql)

    return len(deleted) > 0


def delete_identifier(file: str, identifier: str) -> bool:
    identifier = apis.models.common.PackageIdentifier(identifier)
    everything = identifier.parse()
    return delete(file, package_name=everything.name, registry_tag=everything.tag, registry_digest=everything.digest)