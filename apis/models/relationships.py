# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import typing_extensions
from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import experiment.model.graph
import pydantic

import apis.models.errors
import apis.models.common
import apis.models.virtual_experiment



def correct_reference(value: str) -> str:
    cid = experiment.model.graph.ComponentIdentifier(value, index=0)

    if cid.stageIndex is None or not cid.componentName or '/' in cid.componentName:
        raise ValueError(f"invalid [stage<index>.]<componentName> = \"{value}\"")

    return cid.identifier

class GraphDescription(apis.models.common.Digestable):
    package: Optional[apis.models.virtual_experiment.BasePackage] = None
    identifier: str
    components: List[typing_extensions.Annotated[str, pydantic.functional_validators.AfterValidator(correct_reference)]] = []

    @pydantic.field_validator("identifier")
    def correct_identifier(cls, value: str) -> str:
        if not value:
            raise ValueError("Identifier cannot be empty")
        identifier = apis.models.common.PackageIdentifier(value)
        return identifier.identifier

    @pydantic.field_validator("components")
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
    name: Optional[str] = None
    value: Optional[str] = None


class RelationshipParameters(apis.models.common.Digestable):
    outputGraphParameter: Optional[GraphValue] = None
    inputGraphParameter: Optional[GraphValue] = None


class RelationshipResults(apis.models.common.Digestable):
    outputGraphResult: Optional[GraphValue] = None
    inputGraphResult: Optional[GraphValue] = None


class VariablesMergePolicy(Enum):
    # VV: This is the default (variables in the Parent of the OutputGraph override
    # those in the InputGraph fragment)
    OutputGraphOverridesInputGraph = 'outputGraphOverridesInputGraph'
    InputGraphOverridesOutputGraph = 'inputGraphOverridesOutputGraph'


class TransformRelationship(apis.models.common.Digestable):
    graphParameters: List[RelationshipParameters] = []
    graphResults: List[RelationshipResults] = []
    inferParameters: bool = True
    inferResults: bool = True
    variablesMergePolicy: str = pydantic.Field(
        VariablesMergePolicy.OutputGraphOverridesInputGraph.value,
        description="How to merge the variables of InputGraph and OutputGraph"
    )

    @pydantic.validator("variablesMergePolicy")
    def validate_variables_merge_policy(cls, policy: str) -> str:
        VariablesMergePolicy(policy)
        return policy

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

    @pydantic.model_validator(mode="after")
    def unique_graph_identifiers(cls, value: "Transform") -> "Transform":
        inputGraph: GraphDescription = value.inputGraph
        outputGraph: GraphDescription = value.outputGraph

        if inputGraph.identifier == outputGraph.identifier:
            raise ValueError("Identifiers of inputGraph and outputGraph must be different")

        return value


class Relationship(apis.models.common.Digestable):
    identifier: str = pydantic.Field(..., description="Unique identifier of this relationship")
    description: Optional[str] = pydantic.Field(
        None, description="Human readable description of transformation relationship")
    transform: Optional[Transform] = None


class SynthesizeOptions(apis.models.common.Digestable):
    generateParameterisation: bool = pydantic.Field(True, description="Auto generate parameterisation options")


class PayloadSynthesize(apis.models.common.Digestable):
    parameterisation: apis.models.virtual_experiment.Parameterisation = pydantic.Field(
        default_factory=apis.models.virtual_experiment.Parameterisation
    )
    options: SynthesizeOptions = pydantic.Field(
        default_factory=SynthesizeOptions, description="Options to guide synthesis"
    )
