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
