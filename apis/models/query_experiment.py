# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

from typing import Optional

import apis.models.common
import apis.models.errors
import apis.models.virtual_experiment


class QueryRelationshipTransform(apis.models.common.Digestable):
    matchInputGraph: bool = False
    matchOutputGraph: bool = False


class QueryRelationship(apis.models.common.Digestable):
    identifier: str
    transform: Optional[QueryRelationshipTransform] = None


class QueryPackage(apis.models.common.Digestable):
    definition: apis.models.virtual_experiment.BasePackage = apis.models.virtual_experiment.BasePackage()


class QueryCommonConfig(apis.models.common.Digestable):
    matchPackageVersion: bool = False
    mustHaveOnePackage: bool = True


class QueryExperiment(apis.models.common.Digestable):
    relationship: Optional[QueryRelationship] = None
    package: Optional[QueryPackage] = None
    common: QueryCommonConfig = QueryCommonConfig()
