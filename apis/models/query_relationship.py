# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

from typing import (
    Optional,
)

import pydantic

import apis.models.common
import apis.models.errors


class QueryRelationshipTransformInputGraph(apis.models.common.Digestable):
    identifier: Optional[str] = pydantic.Field(
        None, description="Regular expression to match the identifiers of inputGraphs in transform relationships")


class QueryRelationshipTransformOutputGraph(apis.models.common.Digestable):
    identifier: Optional[str] = pydantic.Field(
        None, description="Regular expression to match the identifiers of outputGraphs in transform relationships")


class QueryRelationshipTransform(apis.models.common.Digestable):
    inputGraph: Optional[QueryRelationshipTransformInputGraph] = pydantic.Field(
        None, description="Match transform.inputGraph")
    outputGraph: Optional[QueryRelationshipTransformOutputGraph] = pydantic.Field(
        None, description="Match transform.outputGraph")


class QueryRelationship(apis.models.common.Digestable):
    identifier: Optional[str] = pydantic.Field(None, description="Regular expression to match names of relationships")
    transform: Optional[QueryRelationshipTransform] = pydantic.Field(None, description="Match transform")
