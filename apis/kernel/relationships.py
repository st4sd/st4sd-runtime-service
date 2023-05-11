# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

from typing import (
    Dict,
    Any,
    List,
    Optional
)

import apis.models.query_relationship
import apis.db.relationships

import utils


def api_list_queries(
        query: apis.models.query_relationship.QueryRelationship,
        db: Optional[apis.db.relationships.DatabaseRelationships] = None,
) -> List[Dict[str, Any]]:
    if db is None:
        db = utils.database_relationships_open()

    with db:
        query = db.construct_complex_query(query)
        return db.query(query)
