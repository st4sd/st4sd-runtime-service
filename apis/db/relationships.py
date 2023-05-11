# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

from typing import TYPE_CHECKING

import tinydb
import tinydb.table

import apis.db.base
import apis.models.errors

import apis.models.query_relationship

if TYPE_CHECKING:
    import apis.models.relationships


class DatabaseRelationships(apis.db.base.Database):
    """The pattern is:

    with DatabaseRelationships(Path to db) as db:
        db.query(...)
    """

    def __init__(self, db_path: str):
        super(DatabaseRelationships, self).__init__(db_path, db_label="rels")

    @classmethod
    def construct_query(
            cls,
            identifier: str,
    ) -> tinydb.table.QueryLike:
        entry = tinydb.Query()
        ql = entry.identifier == identifier
        return ql

    @classmethod
    def construct_complex_query(
            cls,
            query: apis.models.query_relationship.QueryRelationship,
    ) -> tinydb.table.QueryLike:
        entry = tinydb.Query()

        ql = ...

        if query.identifier:
            ql = entry.identifier.matches(query.identifier)

        if query.transform is not None:
            if query.transform.inputGraph is not None and query.transform.inputGraph.identifier:
                q = entry.transform.inputGraph.identifier.matches(query.transform.inputGraph.identifier)
                ql = q if ql is ... else ql & q

            if query.transform.outputGraph is not None and query.transform.outputGraph.identifier:
                q = entry.transform.outputGraph.identifier.matches(query.transform.outputGraph.identifier)
                ql = q if ql is ... else ql & q

        return ql
