# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import experiment.model.graph
import pydantic

import apis.models.common
import apis.models.virtual_experiment

validator = pydantic.validator


class GraphDescription(apis.models.common.Digestable):
    package: Optional[apis.models.virtual_experiment.BasePackage] = None
    identifier: str
    components: List[str] = []

    @validator("identifier")
    def correct_identifier(cls, value: str) -> str:
        if not value:
            raise ValueError("Identifier cannot be empty")
        identifier = apis.models.common.PackageIdentifier(value)
        return identifier.identifier

    @validator("components", each_item=True)
    def correct_reference(cls, value: str) -> str:
        cid = experiment.model.graph.ComponentIdentifier(value, index=0)

        if cid.stageIndex is None or not cid.componentName or '/' in cid.componentName:
            raise ValueError(f"invalid [stage<index>.]<componentName> = \"{value}\"")

        return cid.identifier

    @validator("components")
    def unique_component_references(cls, value: List[str]) -> List[str]:
        unique = set()
        duplicates = set()
        for x in value:
            if x in unique:
                duplicates.add(x)
            unique.add(x)

        if duplicates:
            raise ValueError(f"multiple references to components {list(duplicates)}")

        return value


class GraphValue(apis.models.common.Digestable):
    name: str
    default: Optional[str]


class RelationshipParameters(apis.models.common.Digestable):
    outputGraphParameter: Optional[GraphValue] = None
    inputGraphParameter: Optional[GraphValue] = None


class RelationshipResults(apis.models.common.Digestable):
    outputGraphResult: Optional[GraphValue] = None
    inputGraphResult: Optional[GraphValue] = None


class TransformRelationship(apis.models.common.Digestable):
    graphParameters: List[RelationshipParameters] = []
    graphResults: List[RelationshipResults] = []
    inferParameters: bool = True
    inferResults: bool = True

    def get_parameter_relationship_by_name_output(self, name: str) -> RelationshipParameters:
        for p in self.graphParameters:
            if p.outputGraphParameter and p.outputGraphParameter.name == name:
                return p
        raise KeyError(f"No matching outputGraphParameter with name \"{name}\" - available names are "
                       f"{[x.outputGraphParameter.name for x in self.graphParameters if x.outputGraphParameter]}", name)

    def get_parameter_relationship_by_name_input(self, name: str) -> RelationshipParameters:
        for p in self.graphParameters:
            if p.inputGraphParameter and p.inputGraphParameter.name == name:
                return p
        raise KeyError(f"No matching inputGraphParameter with name \"{name}\" - available names are "
                       f"{[x.inputGraphParameter.name for x in self.graphParameters if x.inputGraphParameter]}", name)

    def get_result_relationship_by_name_output(self, name: str) -> RelationshipResults:
        for p in self.graphResults:
            if p.outputGraphResult and p.outputGraphResult.name == name:
                return p
        raise KeyError(f"No matching outputGraphResult with name \"{name}\" - available names are "
                       f"{[x.outputGraphResult.name for x in self.graphResults if x.outputGraphResult]}",
                       name)

    def get_result_relationship_by_name_input(self, name: str) -> RelationshipResults:
        for p in self.graphResults:
            if p.inputGraphResult and p.inputGraphResult.name == name:
                return p
        raise KeyError(f"No matching inputGraphResult with \"{name}\" - available names are "
                       f"{[x.inputGraphResult.name for x in self.graphResults if x.inputGraphResult]}",
                       name)


class Transform(apis.models.common.Digestable):
    """Instructions to transform inputGraph into outputGraph.

    This is the mathematical operation ::

        outputGraph = Transform(inputGraph)

    For example, it is valid to substitute an occurrence of `outputGraph` in a graph for `transform(inputGraph)`.
    """

    outputGraph: GraphDescription
    inputGraph: GraphDescription
    relationship: TransformRelationship = TransformRelationship()

    @pydantic.root_validator()
    def unique_graph_identifiers(cls, values: Dict[str, Any]):
        inputGraph: GraphDescription = values['inputGraph']
        outputGraph: GraphDescription = values['outputGraph']

        if inputGraph.identifier == outputGraph.identifier:
            raise ValueError("Identifiers of inputGraph and outputGraph must be different")

        return values


class Relationship(apis.models.common.Digestable):
    identifier: str = pydantic.Field(..., description="Unique identifier of this relationship")
    transform: Optional[Transform] = None


class PayloadSynthesize(apis.models.common.Digestable):
    parameterisation: apis.models.virtual_experiment.Parameterisation = pydantic.Field(
        apis.models.virtual_experiment.Parameterisation()
    )
