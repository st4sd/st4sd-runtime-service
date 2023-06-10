# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import re
import logging
import traceback
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
from typing import Any
from typing import Iterable

from typing import Callable
import pydantic
import experiment.model.errors
import experiment.model.frontends.flowir
import experiment.model.graph

import apis.db.exp_packages
import apis.models.common
import apis.models.constants
import apis.models.errors
import apis.models.from_core
import apis.models.relationships
import apis.models.virtual_experiment
import apis.runtime.errors
import apis.runtime.package_derived
import apis.storage


def get_parameters_of_component(
        component: experiment.model.frontends.flowir.DictFlowIRComponent
) -> List[str]:
    """Let's assume that `references` are graphParameters"""
    return sorted(component.get('references', []))


class ManyParameters(pydantic.BaseModel):
    class Config:
        arbitrary_types_allowed = True

    references: List[apis.models.from_core.DataReference] = []
    variables: List[str] = []


def get_workflow_parameter_names(
        concrete: experiment.model.frontends.flowir.FlowIRConcrete,
        cb_filter: Callable[[str], bool] | None = None,
        comp_ids: Optional[List[Tuple[int, str]]] = None,
) -> ManyParameters:
    """Extracts the top-level parameters in the workflow (assumes that references are the only parameters)"""
    aggregate = set()

    if comp_ids is None:
        comp_ids = concrete.get_component_identifiers(recompute=False)

    platform_vars = {
        p: {
            str(k): str(v) for (k, v) in concrete.get_platform_variables(p)['global'].items()
        } for p in concrete.platforms
    }

    problems: List[str] = []

    for cid in comp_ids:
        comp = concrete.get_component(cid)
        parameters = get_parameters_of_component(comp)

        if cb_filter is not None:
            parameters = [p for p in parameters if cb_filter(p)]

        aggregate.update(parameters)

        def record_references_to_variables(what: Any, label: str):
            if not isinstance(what, str):
                return

            more_variables = []

            for p in concrete.platforms:
                try:
                    variables = experiment.model.frontends.flowir.FlowIR.discover_indirect_dependencies_to_variables(
                        what, context=platform_vars[p], out_missing_variables=more_variables)
                    aggregate.update({f'%({x})s' for x in variables + more_variables})
                except Exception as e:
                    logging.getLogger("visit").warning(f"Exception while extracting variable reference: {e} "
                                                       f"- {traceback.format_exc()}")
                    problems.append(experiment.model.graph.ComponentIdentifier(cid[1], cid[0]).identifier)

        experiment.model.frontends.flowir.FlowIR.visit_all(comp, record_references_to_variables)

    if problems:
        raise apis.runtime.errors.RuntimeError(f"Error while extracting variable parameters for components {problems}")

    ret = ManyParameters()
    for w in sorted(aggregate):
        if w.startswith('%('):
            ret.variables.append(w[2:-2])
        else:
            ret.references.append(apis.models.from_core.DataReference(w))

    return ret


def references_cmp(ref1: str, ref2: str) -> int:
    """Returns how much the @ref1 references matches @ref2 - references may be in different graphs

    Arguments:
        ref1: An absolute reference
        ref2: Another absolute reference

    Returns:
        0 if references do not match at all, 1 if the producers match, 2 if they are identical
    """

    if ref1 == ref2:
        return 2

    d1 = apis.models.from_core.DataReference(ref1)
    d2 = apis.models.from_core.DataReference(ref2)

    return 1 if d1.producerIdentifier.identifier == d2.producerIdentifier.identifier else 0


class TransformRelationship:
    def __init__(
            self,
            transformation: apis.models.relationships.Transform,
            output_graph_name: str = "outputGraph",
            input_graph_name: str = "inputGraph",
    ):
        self._transform = transformation
        self._input_graph_name = input_graph_name
        self._output_graph_name = output_graph_name
        self._log = logging.getLogger('transform')

        self._ve_inputgraph: Union[apis.models.virtual_experiment.ParameterisedPackage, None] = None
        self._ve_outputgraph: Union[apis.models.virtual_experiment.ParameterisedPackage, None] = None

    def discover_parameterised_packages(
            self,
            db_packages: apis.db.exp_packages.DatabaseExperiments,
    ):

        for kind, identifier in [('inputGraph', self._transform.inputGraph.identifier),
                                 ('outputGraph', self._transform.outputGraph.identifier)]:
            docs = db_packages.query_identifier(identifier)
            if len(docs) == 0:
                raise apis.models.errors.ParameterisedPackageNotFoundError(identifier)
            elif len(docs) > 1:
                raise apis.models.errors.ApiError(f"Database contains multiple parameterised virtual experiment "
                                                  f"packages with the identifier \"{identifier}\"")
            try:
                pvep = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(docs[0])
            except pydantic.error_wrappers.ValidationError as e:
                raise apis.models.errors.InvalidModelError(f"{kind} {identifier} is invalid",
                                                           problems=e.errors())
            if len(pvep.base.packages) != 1:
                raise apis.models.errors.ApiError(
                    f"Cannot use parameterised virtual experiment package {identifier} "
                    f"because it does not have exactly 1 base package "
                    f"(it has {len(pvep.base.packages)} base packages)")
            if identifier == self._transform.inputGraph.identifier:
                self._transform.inputGraph.package = pvep.base.packages[0]
                self._ve_inputgraph = pvep
            else:
                self._transform.outputGraph.package = pvep.base.packages[0]
                self._ve_outputgraph = pvep

    def get_ve_inputgraph(self) -> Union[apis.models.virtual_experiment.ParameterisedPackage, None]:
        return self._ve_inputgraph

    def get_ve_outputgraph(self) -> Union[apis.models.virtual_experiment.ParameterisedPackage, None]:
        return self._ve_outputgraph

    def guess_reference_parameter_of_surrogate(
            self,
            foundation: experiment.model.frontends.flowir.DictFlowIRComponent,
            surrogate: experiment.model.frontends.flowir.DictFlowIRComponent,
            surrogate_dref: apis.models.from_core.DataReference,
    ) -> str:
        surrogate_name = experiment.model.graph.ComponentIdentifier(surrogate['name']).identifier
        foundation_params = get_parameters_of_component(foundation)
        parameter_name = surrogate_dref.absoluteReference

        surrogate_pathref = surrogate_dref.pathRef
        matching = []
        self._log.info(f"Guessing parameter {parameter_name} of surrogate {surrogate_name}")

        for param_foundation in foundation_params:
            try:
                foundation_dref = apis.models.from_core.DataReference(param_foundation)
            except ValueError:
                # VV: This parameter is actually a variable not a reference
                continue

            if foundation_dref.pathRef == surrogate_pathref:
                self._log.info(
                    f"   Matching {foundation_dref.absoluteReference} with "
                    f"{surrogate_dref.absoluteReference} on pathRef= {surrogate_pathref}")
                foundation_dref.method = surrogate_dref.method
                matching.append(foundation_dref.absoluteReference)

        if len(matching) == 1:
            self._log.info(f"  Parameter {parameter_name} of surrogate {surrogate_name} "
                           f"is \"{matching[0]}\", candidates were {foundation_params}")
            return matching[0]
        else:
            self._log.info(f"  Could not infer parameter {parameter_name} "
                           f"for surrogate {surrogate_name}. Possible values are {matching}, "
                           f"candidates were {foundation_params}, surrogate file reference was "
                           f"{surrogate_pathref}")


    def _infer_relationship_identical_parameter_names(
            self,
            packages_metadata: apis.storage.PackageMetadataCollection,
    ):
        """Infers relationships between 2 components in outputGraph and inputGraph when they have the same name

        The relationships can be either graphParameters-type or graphResults-type.
        """
        transform = self._transform
        self._log.info("Inferring $in->$out parameter mappings for parameters with identical names")

        # VV: Foundation (i.e. output) and Surrogate (i.e. input) FlowIRConcrete instances
        concrete_found = packages_metadata.get_concrete_of_package(transform.outputGraph.identifier)
        concrete_surr = packages_metadata.get_concrete_of_package(transform.inputGraph.identifier)

        def exclude_components(components: List[str]):
            """Generates a filter that rejects certain components AND references to external data
            such as input, application dependencies, etc
            """
            excl_components = list(components or [])

            def filter_out_components(parameter: str) -> bool:
                try:
                    dref = apis.models.from_core.DataReference(parameter)
                except ValueError:
                    # VV: This parameter is not a reference, must be a variable
                    return True

                return dref.externalProducerName is None and \
                    (dref.producerIdentifier.identifier not in excl_components)

            return filter_out_components

        cids_surr = concrete_surr.get_component_identifiers(recompute=False)
        not_in_inputgraph = []

        for cid in cids_surr:
            ref = experiment.model.graph.ComponentIdentifier(cid[1], cid[0]).identifier
            if ref not in transform.inputGraph.components:
                not_in_inputgraph.append(ref)

        # VV: Foundation (i.e. output) and Surrogate (i.e. input) parameters
        try:
            parameters_found = get_workflow_parameter_names(
                concrete_found, cb_filter=exclude_components(transform.outputGraph.components))
        except Exception as e:
            raise apis.runtime.errors.RuntimeError(f"Could not extract parameters of outputGraph "
                                                   f"- underlying error {e}")

        variables_found_all = set()
        for p in concrete_found.platforms:
            variables_found_all.update(concrete_found.get_platform_global_variables(p))

        self._log.info(f"Parameters Foundation {parameters_found.references} and {parameters_found.variables}")

        known_mappings_surr = set()
        for m in transform.relationship.graphParameters:
            if m.inputGraphParameter.name:
                known_mappings_surr.add(m.inputGraphParameter.name)

        # VV: First prepare the mappings between graph parameters. If we find an inputGraph parameter for which
        # there is no mapping to an outputGraph then try to find if there's an outputGraph component which:
        # 1. does not get removed by transform
        # 2. has the same name as the inputGraph parameter
        # If there's such an outputGraph then generate a graphParameters mapping between the 2 components

        comp_ids = []
        for x in transform.inputGraph.components:
            cid = experiment.model.graph.ComponentIdentifier(x)
            comp_ids.append((cid.stageIndex, cid.componentName))
        try:
            parameters_surr = get_workflow_parameter_names(concrete_surr, cb_filter=None, comp_ids=comp_ids)
        except Exception as e:
            raise apis.runtime.errors.RuntimeError(f"Could not extract parameters of inputGraph "
                                                   f"- underlying error {e}")

        self._log.info(f"Parameters surrogate {parameters_surr.references} and {parameters_surr.variables}")

        # VV: The 2 graphs may contain variables with the same name. variablesMergePolicy dictates how to handle this:
        # If variablesMergePolicy is OutputGraphOverridesInputGraph (default) (and inferParameters is True)
        #   Use the outputGraph variables as parameters of the inputGraph
        # If variablesMergePolicy is InputGraphOverridesOutputGraph (and inferResults is True)
        #   Use the inputGraph variables as results that the 1-outputGraph consumes
        all_graph_output_results = set()
        for x in transform.relationship.graphResults:
            all_graph_output_results.add(x.outputGraphResult.name)

        var_policy = apis.models.relationships.VariablesMergePolicy
        for name in parameters_surr.variables:
            if name not in variables_found_all:
                # VV: This variable is brought in from the surrogate definition, it's not an actual parameter
                continue

            if self._transform.relationship.variablesMergePolicy == var_policy.OutputGraphOverridesInputGraph.value:
                if name in known_mappings_surr:
                    continue

                if self._transform.relationship.inferParameters:
                    known_mappings_surr.add(name)
                    transform.relationship.graphParameters.append(
                        apis.models.relationships.RelationshipParameters(
                            inputGraphParameter=apis.models.relationships.GraphValue(name=name),
                            outputGraphParameter=apis.models.relationships.GraphValue(name=name)))
            elif self._transform.relationship.variablesMergePolicy == var_policy.InputGraphOverridesOutputGraph.value:
                if name in all_graph_output_results:
                    continue

                if self._transform.relationship.inferResults:
                    all_graph_output_results.add(name)
                    transform.relationship.graphResults.append(
                        apis.models.relationships.RelationshipResults(
                            inputGraphResult=apis.models.relationships.GraphValue(name=name),
                            outputGraphResult=apis.models.relationships.GraphValue(name=name)))
            else:
                raise apis.runtime.errors.RuntimeError("Unknown variablesMergePolicy: "
                                                       f"{self._transform.relationship.variablesMergePolicy}")

        for ref in parameters_surr.references:
            if ref.absoluteReference in known_mappings_surr:
                continue

            # VV: External dependencies (e.g. input, or  application dependency) are special,
            # we don't need to care for them at all. They will just point to a directory under $INSTANCE_DIR
            # We also don't care about parameters which are satisfied by components in the inputGraph
            if ref.externalProducerName \
                    or ref.producerIdentifier.identifier in transform.inputGraph.components:
                continue

            for dref_found in parameters_found.references:
                if dref_found.producerIdentifier == ref.producerIdentifier:
                    # VV: There's a component in the outputGraph that the relationship does not remove
                    # it has the same name as a component that was satisfying the dependency in inputGraph
                    # let's infer that the 2 components are equal

                    # VV: Here we use the foundation stage/index and the expected reference method (from surrogate)
                    ref_surr = apis.models.from_core.DataReference.from_parts(
                        stage=ref.stageIndex, producer=ref.producerName, fileRef='',
                        method=ref.method).absoluteReference

                    if ref_surr not in known_mappings_surr:
                        ref_found = apis.models.from_core.DataReference.from_parts(
                            stage=dref_found.stageIndex, producer=dref_found.producerName, fileRef='',
                            method=ref.method).absoluteReference

                        self._log.info(
                            f"   Matching {dref_found.absoluteReference} with "
                            f"{ref.absoluteReference} on pathRef= {ref.pathRef}")

                        known_mappings_surr.add(ref_surr)

                        transform.relationship.graphParameters.append(
                            apis.models.relationships.RelationshipParameters(
                                inputGraphParameter=apis.models.relationships.GraphValue(name=ref_surr),
                                outputGraphParameter=apis.models.relationships.GraphValue(name=ref_found)))

        # VV: We can do something similar for graphResults DataReferences
        # - If there is a parameter in the outputGraph referencing a component that the transformation removes, AND
        # - there is no graphResult mapping for this parameter, AND
        # - there is a component in the inputGraph with the same name, THEN
        # Use the inputGraph component instead of the one that outputGraph removes

        cids_found = concrete_found.get_component_identifiers(recompute=True, include_documents=False)

        self._log.info(f"Foundation components are: {cids_found} known graphResults are {all_graph_output_results}")

        for cid in cids_found:
            # VV: This component will stick around after the transformation. Let's check its parameters, and apply the
            # logic above to generate a new graphResults entry
            comp = concrete_found.get_component(cid)
            params = get_parameters_of_component(comp)

            # VV: Don't generate graphResults to satisfy parameters of components that the transformation removes
            comp_id = experiment.model.graph.ComponentIdentifier(cid[1], cid[0])
            if comp_id.identifier in transform.outputGraph.components:
                continue

            for p in params:
                dref = apis.models.from_core.DataReference(p)

                if dref.externalProducerName:
                    continue

                for cs in transform.inputGraph.components:
                    stage, name, _ = experiment.model.frontends.flowir.FlowIR.ParseProducerReference(cs)
                    cref = apis.models.from_core.DataReference.from_parts(stage, name, "", "ref")

                    if references_cmp(cref.absoluteReference, dref.absoluteReference) > 0:
                        # VV: Rewrite the outputGraph reference so that it points to the entire Component
                        # this way we can satisfy all references to this component with just 1 graphResults
                        dref = apis.models.from_core.DataReference(
                            experiment.model.frontends.flowir.FlowIR.compile_reference(
                                dref.producerName, filename=None, method="ref", stage_index=dref.stageIndex)
                        )
                        self._log.info(
                            f"   Matching graphResult {dref.absoluteReference} with "
                            f"{cref.absoluteReference} on pathRef= {dref.pathRef}")

                        if dref.absoluteReference not in all_graph_output_results:
                            all_graph_output_results.add(dref.absoluteReference)

                            rel = apis.models.relationships.RelationshipResults(
                                outputGraphResult=apis.models.relationships.GraphValue(name=dref.absoluteReference),
                                inputGraphResult=apis.models.relationships.GraphValue(name=cref.absoluteReference)
                            )
                            transform.relationship.graphResults.append(rel)

    def _infer_relationship_single_component_graphs(
            self,
            packages_metadata: apis.storage.PackageMetadataCollection,
    ):
        transform = self._transform
        self._log.info("Inferring $in->$out relationship because both graphs have 1 component each")

        concrete_foundation = packages_metadata.get_concrete_of_package(transform.outputGraph.identifier)
        concrete_surrogate = packages_metadata.get_concrete_of_package(transform.inputGraph.identifier)

        cid_foundation = experiment.model.graph.ComponentIdentifier(
            transform.outputGraph.components[0])
        cid_surrogate = experiment.model.graph.ComponentIdentifier(
            transform.inputGraph.components[0])

        comp_foundation = concrete_foundation.get_component(
            (cid_foundation.stageIndex, cid_foundation.componentName))
        comp_surrogate = concrete_surrogate.get_component(
            (cid_surrogate.stageIndex, cid_surrogate.componentName))

        parameters_surrogate = get_workflow_parameter_names(
            concrete_surrogate, comp_ids=[(cid_surrogate.stageIndex, cid_surrogate.componentName)])

        if transform.relationship.inferParameters:
            for surr_dref in parameters_surrogate.references:
                try:
                    value = transform.relationship.get_parameter_relationship_by_name_input(surr_dref.absoluteReference)
                    if value.outputGraphParameter.value or value.inputGraphParameter.name:
                        # VV: No need to infer anything for this graphParameter
                        continue
                except KeyError:
                    # VV: This means that there's no relationships of an $in parameter to
                    # this $out one, so we need to infer it
                    pass

                # VV: If we got here, then we need to infer the relationships for inputGraphParameter
                self._log.log(15, f"Try infer relationships of inputGraphParameter.{surr_dref.absoluteReference})")

                inferred = self.guess_reference_parameter_of_surrogate(comp_foundation, comp_surrogate, surr_dref)

                if inferred is not None:
                    transform.relationship.graphParameters.append(
                        apis.models.relationships.RelationshipParameters(
                            outputGraphParameter=apis.models.relationships.GraphValue(
                                name=inferred),
                            inputGraphParameter=apis.models.relationships.GraphValue(
                                name=surr_dref.absoluteReference)
                        ))

        if transform.relationship.inferResults and not transform.relationship.graphResults:
            name_foundation = apis.models.from_core.DataReference(
                ':'.join((cid_foundation.identifier, 'ref'))).absoluteReference
            name_surrogate = apis.models.from_core.DataReference(
                ':'.join((cid_surrogate.identifier, 'ref'))).absoluteReference

            self._log.info(f"Identified single component in both graphs and generating graphResults for "
                           f"{name_foundation} -> {name_surrogate}")
            rel = apis.models.relationships.RelationshipResults(
                outputGraphResult=apis.models.relationships.GraphValue(name=name_foundation),
                inputGraphResult=apis.models.relationships.GraphValue(name=name_surrogate)
            )
            transform.relationship.graphResults.append(rel)

    def _attempt_infer_relationship(
            self,
            packages_metadata: apis.storage.PackageMetadataCollection,
    ):
        """Updates the .relationship field of self._transform"""
        transform = self._transform

        self._log.info(f"Inferring relationships for {transform.json(indent=2)}")

        if transform.relationship.inferParameters or transform.relationship.inferResults:
            # VV: Try to infer relationships of graphParameters
            # VV: TODO Currently we only know how to do this the 2 graphs have exactly 1
            #  component each
            if len(transform.outputGraph.components) == 1 \
                    and len(transform.inputGraph.components) == 1:
                self._log.info("Inferring parameter mappings for 1-to-1 transform")
                self._infer_relationship_single_component_graphs(packages_metadata)
                self._log.info(f"Parameter Mappings (after 1-to-1): {self._transform.json(indent=2)}")

            # VV: After we've handled all the "special" cases, we can assume that if 2 parameters have the same name
            # in the 2 graphs, then they are "equivalent"
            self._infer_relationship_identical_parameter_names(packages_metadata)

    def _test_graph_relationship(
            self,
            packages_metadata: apis.storage.PackageMetadataCollection,
    ):
        transform = self._transform

        problems = []

        concrete_surrogate = packages_metadata.get_concrete_of_package(transform.inputGraph.identifier)

        for ref_surrogate in transform.inputGraph.components:
            cid_surrogate = experiment.model.graph.ComponentIdentifier(ref_surrogate)
            comp_surrogate = concrete_surrogate.get_component(
                (cid_surrogate.stageIndex, cid_surrogate.componentName))

            parameters_surrogate = get_parameters_of_component(comp_surrogate)

            for param in parameters_surrogate:
                try:
                    _value = transform.relationship.get_parameter_relationship_by_name_input(param)
                except KeyError:
                    ref = apis.models.from_core.DataReference(param)
                    # VV: There was no need to specify this parameter mapping because it points to a
                    # component inside the inputGraph
                    if ref.producerIdentifier.identifier in transform.inputGraph.components:
                        continue

                    if ref.externalProducerName:
                        continue

                    problem = (f"Unknown parameter {param} for surrogate component "
                               f"{ref_surrogate} in {transform.inputGraph.identifier}")
                    if problem not in problems:
                        problems.append(problem)

        if problems:
            raise apis.models.errors.ApiError(f"Run into {len(problems)} problems:\n" + "\n".join(problems))

    def try_infer(
            self,
            packages_metadata: apis.storage.PackageMetadataCollection,
    ) -> apis.models.relationships.Transform:
        self._attempt_infer_relationship(packages_metadata)
        self._test_graph_relationship(packages_metadata)
        return self._transform


class TransformRelationshipToDerivedPackage(TransformRelationship):
    def __init__(
            self,
            transformation: apis.models.relationships.Transform,
    ):
        output_graph_name = transformation.outputGraph.identifier or "outputGraph"
        input_graph_name = transformation.inputGraph.identifier or "inputGraph"
        super(TransformRelationshipToDerivedPackage, self).__init__(
            transformation=transformation, output_graph_name=output_graph_name, input_graph_name=input_graph_name)

    def _ensure_base_packages(
            self,
            derived: apis.models.virtual_experiment.ParameterisedPackage,
    ):
        transform = self._transform

        if self._transform.inputGraph.package is None:
            raise apis.models.errors.ApiError("Missing transform.inputGraph.package")

        if self._transform.outputGraph.package is None:
            raise apis.models.errors.ApiError("Missing transform.outputGraph.package")

        foundation_package = transform.outputGraph.package
        foundation_package.name = transform.outputGraph.identifier

        if foundation_package is None:
            raise KeyError(f"Missing transform.outputGraph.package")

        surrogate_package = transform.inputGraph.package
        surrogate_package.name = transform.inputGraph.identifier
        if surrogate_package is None:
            raise KeyError(f"Missing transform.inputGraph.package")

        derived.base.packages.append(foundation_package.copy(deep=True))
        derived.base.packages.append(surrogate_package.copy(deep=True))

        self._log.info(f"Base packages: {[x.name for x in derived.base.packages]}")

    def _populate_graph_nodes(
            self,
            concrete: experiment.model.frontends.flowir.FlowIRConcrete,
            graph: apis.models.virtual_experiment.BasePackageGraph,
            components_exclusively_include: List[str] | None = None,
            components_exclude: List[str] | None = None,
    ):
        all_comp_ids = [
            experiment.model.graph.ComponentIdentifier(cid[1], cid[0]).identifier
            for cid in concrete.get_component_identifiers(recompute=False)]
        self._log.info(f"{graph.name} virtual experiment contains {len(all_comp_ids)} components")

        original_comp_ids = list(all_comp_ids)

        if components_exclude is not None:
            all_comp_ids = [x for x in all_comp_ids if x not in components_exclude]
            excluded = [x for x in original_comp_ids if x in components_exclude]
            missing = set(components_exclude).difference(excluded)

            self._log.info(f"Updated {graph.name} virtual experiment to exclude {len(excluded)} components")

            if missing:
                raise apis.models.errors.ApiError(f"{graph.name} cannot exclude components {missing} "
                                                  f"- it does not contain them in the first place")

        if components_exclusively_include is not None:
            all_comp_ids = [x for x in all_comp_ids if x in components_exclusively_include]
            missing = set(components_exclusively_include).difference(all_comp_ids)

            self._log.info(f"Updated {graph.name} virtual experiment to include just "
                           f"{len(components_exclusively_include)} components")

            if missing:
                raise apis.models.errors.ApiError(f"{graph.name} cannot exclusively include components {missing} - "
                                                  f"it does not contain them in the first place")

        all_comp_ids = sorted(all_comp_ids)

        self._log.info(f"Components in {graph.name} are {all_comp_ids}")

        graph.nodes = [
            apis.models.virtual_experiment.BasePackageGraphNode(reference=x) for x in all_comp_ids
        ]

    @classmethod
    def _validate_reference_variable_name_or_string_in_concrete_context(
            cls,
            parameter_name: str,
            value_reference_or_variable: str,
            value_string_with_variable_refs: str,
            context_variables: Iterable[str],
            all_parameter_variables: Iterable[str],
            package_name_of_parameter: str,
            package_name_of_context_vars: str,
            extra_msg_for_exception: str,
    ) -> List[Exception]:
        problems = []

        try:
            apis.models.from_core.DataReference(parameter_name)
        except ValueError:
            # VV: The parameter is not a DatareReference, therefore it *must* be the name of a variable
            if parameter_name not in all_parameter_variables:
                problems.append(
                    apis.models.errors.TransformationUnknownVariableError(
                        variable=parameter_name,
                        package=package_name_of_parameter,
                        extra_msg=extra_msg_for_exception))

        if value_reference_or_variable:
            try:
                apis.models.from_core.DataReference(value_reference_or_variable)
            except ValueError:
                # VV: The value is not a DataReference, it *MUST* be the name of a variable in inputGraph
                if value_reference_or_variable not in context_variables:
                    problems.append(
                        apis.models.errors.TransformationUnknownVariableError(
                            variable=value_reference_or_variable,
                            package=package_name_of_context_vars,
                            extra_msg=extra_msg_for_exception))
        elif value_string_with_variable_refs:
            # VV: This is a string value evaluated in the context of a package, it MAY contain many
            # variable references
            ref_vars = experiment.model.frontends.flowir.FlowIR.discover_references_to_variables(
                value_string_with_variable_refs)
            for unknown_var in set(ref_vars).difference(context_variables):
                problems.append(
                    apis.models.errors.TransformationUnknownVariableError(
                        variable=unknown_var,
                        package=package_name_of_context_vars,
                        extra_msg=extra_msg_for_exception))
        else:
            problems.append(apis.models.errors.TransformationError(
                msg=f"There is no value for parameter {parameter_name}. {extra_msg_for_exception}"))

        return problems

    def _validate_graph_parameters_and_results(
            self,
            outputgraph_concrete: experiment.model.frontends.flowir.FlowIRConcrete,
            inputgraph_concrete: experiment.model.frontends.flowir.FlowIRConcrete,
    ):
        problems = []
        outputgraph_variables = set()
        for p in outputgraph_concrete.platforms:
            outputgraph_variables.update(outputgraph_concrete.get_platform_global_variables(p))

        inputgraph_variables = set()
        for p in inputgraph_concrete.platforms:
            inputgraph_variables.update(inputgraph_concrete.get_platform_global_variables(p))

        for x in self._transform.relationship.graphParameters:
            # VV: graphParameters use the value of either a reference, a variable, or a string containing variables
            # in the outputGraph to set the value of a parameter in the inputGraph
            if not x.inputGraphParameter.name:
                problems.append(apis.models.errors.TransformationError(
                    f"graphParameter {x.dict()} does not contain a valid inputGraphParameter name"))

            problems.extend(self._validate_reference_variable_name_or_string_in_concrete_context(
                parameter_name=x.inputGraphParameter.name,
                value_reference_or_variable=x.outputGraphParameter.name,
                value_string_with_variable_refs=x.outputGraphParameter.value,
                context_variables=outputgraph_variables,
                all_parameter_variables=inputgraph_variables,
                extra_msg_for_exception=f"graphParameters entry {x.dict()} references the variable",
                package_name_of_parameter="inputGraph",
                package_name_of_context_vars="outputGraph",
            ))

        for x in self._transform.relationship.graphResults:
            if not x.outputGraphResult.name:
                problems.append(apis.models.errors.TransformationError(
                    f"graphResult {x.dict()} does not contain a valid outputGraphResult name"))

            problems.extend(self._validate_reference_variable_name_or_string_in_concrete_context(
                parameter_name=x.outputGraphResult.name,
                value_reference_or_variable=x.inputGraphResult.name,
                value_string_with_variable_refs=x.inputGraphResult.value,
                context_variables=inputgraph_variables,
                all_parameter_variables=outputgraph_variables,
                extra_msg_for_exception=f"graphResults entry {x.dict()} references the variable",
                package_name_of_parameter="outputGraph",
                package_name_of_context_vars="inputGraph",
            ))

        problems_unique = {}
        for p in problems:
            if str(p) in problems_unique:
                continue
            problems_unique[str(p)] = p

        if len(problems_unique) > 1:
            raise apis.models.errors.TransformationManyErrors(list(problems_unique.values()))
        elif problems_unique:
            _, exc = problems_unique.popitem()
            raise exc

    def _populate_graph_bindings_for_outputgraph(
            self,
            graph: apis.models.virtual_experiment.BasePackageGraph
    ):
        """Adds graphBindings to connect the components in the Foundation to those in the Surrogate

        The Foundation is the parameterised virtual experiment package whose sub-graph is outputGraph.

        The Surrogate is the parameterised virtual experiment package whose sub-graph is inputGraph
        @self._transform explains how to substitute an occurrence of outputGraph with transform(inputGraph)

        Arguments:
            graph: Foundation graph to populate with bindings (input and output). The method updates @graph in-place.

        Returns:
            The method returns None, it updates @graph in-place.
        """
        # VV: I am the outputGraph which means I need to bind the parameters of the inputGraph
        # to whatever is populating MY input parameters. Therefore, I'll create output "bindings" that point
        # to what's populating MY input parameters
        # I also need to consume the outputs of the inputGraph therefore I need to create inputBindings

        for x in self._transform.relationship.graphParameters:
            # VV: graphParameters use the value of either a reference, a variable, or a string containing variables
            # in the outputGraph to set the value of a parameter in the inputGraph
            v = x.outputGraphParameter

            # VV :"input" refs are external, we do not need to have an output binding for those
            if v.name:
                try:
                    dref = apis.models.from_core.DataReference(v.name)
                except ValueError:
                    # VV: This is not a DataReference, it is the name of a variable which must exist in the outputGraph
                    graph.bindings.output.append(
                        # VV: FIXME FlowIR 2.0 will give us a way to define these bindings better
                        apis.models.virtual_experiment.GraphBinding(name=v.name, text=f'%({v.name})s'))
                else:
                    if dref.externalProducerName:
                        continue
                    graph.bindings.output.append(
                        # VV: FIXME FlowIR 2.0 will give us a way to define these bindings better
                        apis.models.virtual_experiment.GraphBinding(name=v.name, reference=v.name))
            elif v.value:
                # VV: This is a string which may contain a bunch of variable references
                graph.bindings.output.append(
                    # VV: FIXME FlowIR 2.0 will give us a way to define these bindings better
                    apis.models.virtual_experiment.GraphBinding(name=v.value, text=v.value))

        for x in self._transform.relationship.graphResults:
            v = x.outputGraphResult

            # VV: The binding can either be a DataReference or the name of a Variable
            try:
                _ = apis.models.from_core.DataReference(v.name)
            except ValueError:
                reference = None
                text = v.name
            else:
                reference = v.name
                text = None

            graph.bindings.input.append(
                # VV: FIXME FlowIR 2.0 will give us a way to define these bindings better
                apis.models.virtual_experiment.GraphBinding(name=v.name, reference=reference, text=text))

    def _populate_graph_bindings_for_inputgraph(
            self,
            graph: apis.models.virtual_experiment.BasePackageGraph
    ):
        """Adds graphBindings to connect the components in the Surrogate to those in the Foundation

        The Foundation is the parameterised virtual experiment package whose sub-graph is outputGraph.

        The Surrogate is the parameterised virtual experiment package whose sub-graph is inputGraph
        @self._transform explains how to substitute an occurrence of outputGraph with transform(inputGraph)

        Arguments:
            graph: Surrogate graph to populate with bindings (input and output). The method updates the @graph in-place.

        Returns:
            The method returns None, it updates @graph in-place.
        """
        # VV: I am the inputGraph - the foundation is going to populate my inputs with its outputs
        # and my outputs will populate its inputs


        for x in self._transform.relationship.graphParameters:
            v = x.inputGraphParameter
            # VV: The binding can either be a DataReference or the name of a Variable
            try:
                _ = apis.models.from_core.DataReference(v.name)
            except ValueError:
                reference = None
                text = v.name
            else:
                reference = v.name
                text = None

            graph.bindings.input.append(
                # VV: FIXME FlowIR 2.0 will give us a way to define these bindings better
                apis.models.virtual_experiment.GraphBinding(name=v.name, reference=reference, text=text))

        # VV: The inputGraph results are probably consumed by the foundation so create output bindings for those
        for x in self._transform.relationship.graphResults:
            v = x.inputGraphResult
            if v.name:
                try:
                    _ = apis.models.from_core.DataReference(v.name)
                except ValueError:
                    # VV: This is not a DataReference, it is a string which may contain multiple "%(variable references)s"
                    graph.bindings.output.append(
                        # VV: FIXME FlowIR 2.0 will give us a way to define these bindings better
                        apis.models.virtual_experiment.GraphBinding(name=v.name, text=f'%({v.name})s'))
                else:
                    graph.bindings.output.append(
                        # VV: FIXME FlowIR 2.0 will give us a way to define these bindings better
                        apis.models.virtual_experiment.GraphBinding(name=v.name, reference=v.name))
            elif v.value:
                graph.bindings.output.append(
                    # VV: FIXME FlowIR 2.0 will give us a way to define these bindings better
                    apis.models.virtual_experiment.GraphBinding(name=v.value, text=v.value))

    def _add_base_graphs_with_dangling_bindings(
            self,
            derived: apis.models.virtual_experiment.ParameterisedPackage,
            packages_metadata: apis.storage.PackageMetadataCollection,
    ):
        transform = self._transform
        foundation_package = derived.base.get_package(transform.outputGraph.identifier)
        surrogate_package = derived.base.get_package(transform.inputGraph.identifier)
        foundation_concrete = packages_metadata.get_concrete_of_package(foundation_package.name)
        surrogate_concrete = packages_metadata.get_concrete_of_package(surrogate_package.name)

        foundation_graph_name = self._output_graph_name
        foundation_graph = apis.models.virtual_experiment.BasePackageGraph(name=foundation_graph_name)
        self._populate_graph_nodes(
            foundation_concrete, foundation_graph, components_exclude=transform.outputGraph.components)
        foundation_package.graphs.append(foundation_graph)

        surrogate_graph_name = self._input_graph_name
        surrogate_graph = apis.models.virtual_experiment.BasePackageGraph(name=surrogate_graph_name)
        self._populate_graph_nodes(
            surrogate_concrete, surrogate_graph, components_exclusively_include=transform.inputGraph.components)
        surrogate_package.graphs.append(surrogate_graph)

        self._validate_graph_parameters_and_results(
            outputgraph_concrete=foundation_concrete,
            inputgraph_concrete=surrogate_concrete)
        self._populate_graph_bindings_for_outputgraph(graph=foundation_graph)
        self._populate_graph_bindings_for_inputgraph(graph=surrogate_graph)

    def _connect_foundation_and_surrogate_graphs(
            self,
            derived: apis.models.virtual_experiment.ParameterisedPackage,
    ):
        transform = self._transform
        foundation_package = derived.base.get_package(transform.outputGraph.identifier)
        if len(foundation_package.graphs) != 1:
            raise apis.models.errors.ApiError(f"Expected Foundation graph of {foundation_package.name} to "
                                              f"have exactly 1 graph, but it has {len(foundation_package.graphs)}\n"
                                              f"{foundation_package.json(indent=2)}")

        foundation_connections = apis.models.virtual_experiment.BasePackageGraphInstance(
            graph=apis.models.virtual_experiment.BasePackageGraph(
                name="/".join((transform.outputGraph.identifier, self._output_graph_name)),
            ), bindings=[])

        surrogate_connections = apis.models.virtual_experiment.BasePackageGraphInstance(
            graph=apis.models.virtual_experiment.BasePackageGraph(
                name="/".join((transform.inputGraph.identifier, self._input_graph_name)),
            ), bindings=[])

        name_graph_outputgraph = "/".join((transform.outputGraph.identifier, self._output_graph_name))
        name_graph_inputgraph = "/".join((transform.inputGraph.identifier, self._input_graph_name))

        for x in transform.relationship.graphParameters:
            # VV: Connect the outputs of outputGraph to the inputs of the inputGraph
            if x.outputGraphParameter.name:
                # VV: This may be a datareference
                try:
                    dref = apis.models.from_core.DataReference(x.outputGraphParameter.name)
                except ValueError:
                    pass
                else:
                    if dref.externalProducerName == "input":
                        continue

            binding = apis.models.virtual_experiment.BindingOption(
                name=x.inputGraphParameter.name,
                valueFrom=apis.models.virtual_experiment.BindingOptionValueFrom(
                    graph=apis.models.virtual_experiment.BindingOptionValueFromGraph(
                        name=name_graph_outputgraph,
                        binding=apis.models.virtual_experiment.GraphBinding(
                            name=x.outputGraphParameter.name or x.outputGraphParameter.value
                        )
                    )
                )
            )

            surrogate_connections.bindings.append(binding)

        for x in transform.relationship.graphResults:
            # VV: Connect the outputs of inputGraph to the inputs of the outputGraph

            if x.inputGraphResult.name:
                # VV: This may be a datareference
                try:
                    dref = apis.models.from_core.DataReference(x.inputGraphResult.name)
                except ValueError:
                    pass
                else:
                    if dref.externalProducerName == "input":
                        continue

            binding = apis.models.virtual_experiment.BindingOption(
                name=x.outputGraphResult.name,
                valueFrom=apis.models.virtual_experiment.BindingOptionValueFrom(
                    graph=apis.models.virtual_experiment.BindingOptionValueFromGraph(
                        name=name_graph_inputgraph,
                        binding=apis.models.virtual_experiment.GraphBinding(
                            name=x.inputGraphResult.name or x.inputGraphResult.value
                        )
                    )
                )
            )
            foundation_connections.bindings.append(binding)

        surrogate_package = derived.base.get_package(transform.inputGraph.identifier)
        if len(surrogate_package.graphs) != 1:
            raise apis.models.errors.ApiError(
                f"Expected Surrogate graph of {surrogate_package.name} to have exactly 1 "
                f"graph, but it has {len(surrogate_package.graphs)}")

        # VV: The Derived package uses the ordering of `connections` to layer variables.
        # The idea is that each connection explains how to "instantiate" a Graph and instantiating a Graph last
        # indicates that we want to use that Graph's global variables in the final (derived) FlowIR
        # FIXME: This is now redundant when inferParameters/inferResults is set to True
        policy_foundation = apis.models.relationships.VariablesMergePolicy.OutputGraphOverridesInputGraph.value
        policy_surrogate = apis.models.relationships.VariablesMergePolicy.InputGraphOverridesOutputGraph.value

        if self._transform.relationship.variablesMergePolicy == policy_foundation:
            derived.base.connections.extend([surrogate_connections, foundation_connections])
        elif self._transform.relationship.variablesMergePolicy == policy_surrogate:
            derived.base.connections.extend([foundation_connections, surrogate_connections])
        else:
            raise apis.models.errors.ApiError(
                "Cannot interpret relationship because variablesMergePolicy"
                f"={self._transform.relationship.variablesMergePolicy} is not implemented")

    def _populate_include_files(
            self,
            derived: apis.models.virtual_experiment.ParameterisedPackage,
            packages_metadata: apis.storage.PackageMetadataCollection,
    ):
        transform = self._transform

        # VV: We do not want to copy anything under `conf`.
        # In the future we may want to undo this decision so that we can support $import and doWhile.
        # In the future, we're also planning to improve FlowIR so we may end-up undoing anything we do now
        # so let's go for the easy route of not supporting $import and dowhile for now.
        exclude_dirs = ['conf']
        foundation = packages_metadata.get_metadata(transform.outputGraph.identifier)
        surrogate = packages_metadata.get_metadata(transform.inputGraph.identifier)

        # VV: Copy everything from the manifest of `foundation` into the derived package and then
        # layer the directories that the `surrogate` manifest points to on top of the `foundation` dirs
        for package_identifier, metadata in [
            (transform.outputGraph.identifier, foundation),
            (transform.inputGraph.identifier, surrogate),
        ]:
            manifest = metadata.manifestData
            for app_dep_name in sorted(manifest):
                if app_dep_name in exclude_dirs:
                    continue
                src = manifest[app_dep_name]
                src_path, _method = src.rsplit(':', 1)
                ip = apis.models.virtual_experiment.IncludePath(
                    source=apis.models.virtual_experiment.PathInsidePackage(
                        packageName=package_identifier, path=src_path,
                    ),
                    dest=apis.models.virtual_experiment.PathInsidePackage(path=app_dep_name)
                )
                derived.base.includePaths.append(ip)

    def _populate_key_outputs_and_interface(
            self,
            derived: apis.models.virtual_experiment.ParameterisedPackage,
            packages_metadata: apis.storage.PackageMetadataCollection,
    ):
        transform = self._transform
        foundation = packages_metadata.get_metadata(transform.outputGraph.identifier)
        surrogate = packages_metadata.get_metadata(transform.inputGraph.identifier)

        foundation_key_outputs = foundation.concrete.get_output()

        def add_key_output(name: str, reference: str, stages: Optional[List[int]], from_foundation: bool):
            if from_foundation:
                graph_source = "/".join((transform.outputGraph.identifier, self._output_graph_name))
            else:
                graph_source = "/".join((transform.inputGraph.identifier, self._input_graph_name))

            derived.base.output.append(
                apis.models.virtual_experiment.BindingOption(
                    name=name,
                    valueFrom=apis.models.virtual_experiment.BindingOptionValueFrom(
                        graph=apis.models.virtual_experiment.BindingOptionValueFromGraph(
                            name=graph_source,
                            binding=apis.models.virtual_experiment.GraphBinding(
                                reference=reference, stages=stages
                            )
                        )
                    )
                )
            )

        InstructionRewireSymbol = apis.runtime.package_derived.InstructionRewireSymbol
        for name in foundation_key_outputs:
            ref = foundation_key_outputs[name]['data-in']
            stages = foundation_key_outputs[name].get('stages', [])
            # VV: For the time being let's just support keyOutputs in just 1 stage

            if len(stages) == 1:
                dref = apis.models.from_core.DataReference(ref, stageIndex=stages[0])
            elif len(stages) == 0:
                dref = apis.models.from_core.DataReference(ref)
            else:
                self._log.warning(f"Cannot handle keyOutput {name} of "
                                  f"{transform.outputGraph.identifier} because it references "
                                  f"dataReference {ref} in {len(stages)} stages - "
                                  f"will assume it does not change")
                add_key_output(name, ref, stages, True)
                continue

            # VV: The component that generates the key output no longer exists in the derived package,
            # need to find its surrogate
            if dref.producerIdentifier.identifier in transform.outputGraph.components:
                for result in transform.relationship.graphResults:
                    if not result.outputGraphResult.name or not result.inputGraphResult.name:
                        continue
                    dref_match = apis.models.from_core.DataReference(result.outputGraphResult.name)
                    dref_replace = apis.models.from_core.DataReference(result.inputGraphResult.name)

                    if dref_match.producerIdentifier.identifier == dref.producerIdentifier.identifier:
                        replacement = InstructionRewireSymbol.infer_replace_reference_with_reference(
                            dref.absoluteReference, dref_match.absoluteReference,
                            dref_replace.absoluteReference
                        )
                        if replacement is None:
                            continue
                        self._log.info(f"Rewrote keyOutput {name} of {self._output_graph_name} from "
                                       f"{dref.absoluteReference} to {replacement}")

                        add_key_output(name, replacement, None, False)
                        break
                else:
                    raise apis.models.errors.ApiError(f"Could not rewrite keyOutput {name} of {self._output_graph_name}"
                                                      f"from {dref.absoluteReference} candidates "
                                                      f"{transform.relationship.dict()}")
            else:
                self._log.info(f"Copying keyOutput {name} from {self._output_graph_name}")
                add_key_output(name, dref.absoluteReference, None, True)

        interface = foundation.concrete.get_interface()
        if interface:
            derived.base.interface = apis.models.from_core.FlowIRInterface.parse_obj(interface)

    def prepare_derived_package(
            self,
            derived_package_name: str,
            parameterisation: apis.models.virtual_experiment.Parameterisation
    ) -> apis.models.virtual_experiment.ParameterisedPackage:
        derived = apis.models.virtual_experiment.ParameterisedPackage()
        derived.metadata.package.name = derived_package_name
        derived.parameterisation = parameterisation

        self._ensure_base_packages(derived)
        return derived

    def synthesize_derived_package(
            self,
            packages_metadata: apis.storage.PackageMetadataCollection,
            derived: apis.models.virtual_experiment.ParameterisedPackage,
    ) -> apis.models.virtual_experiment.ParameterisedPackage:
        """

        Updates derived in place, but also returns it

        Algorithm: Extract sub-graphs from Foundation and Surrogate VEs and wire them together ::

            1. Add subGraphs with dangling input/outputs
               1. The foundation sub-graph has all components of the foundation VE minus those
                  in outputGraph.components
               2. The surrogate sub-graph has just the components of inputGraph.components
            2. Use the input/output mappings to wire together the 2 graphs
            3. Copy all paths that the Foundation manifest points to, then layer on top of the resulting files
               the contents of all paths tha the Surrogate manifest points to.
            4. Copy key-outputs/interface of Foundation and patch it with Surrogate (if Transform asks
               to replace Foundation components that produce key-outputs with Surrogate ones)
            5. Inherit parameterisation from Foundation
        """
        # VV: Create the blueprints for the Derived package and do the legwork to have a
        # self._transform that contains all necessary information to connect the 2 graphs
        self._attempt_infer_relationship(packages_metadata)
        self._test_graph_relationship(packages_metadata)

        # VV: Below this point, assume that self._transform is as verbose as it can get and
        # 100% correct. Just wire Graphs together to produce a ParameterisedPackage
        # that executes the aggregate graph

        # VV: Do step 1
        self._add_base_graphs_with_dangling_bindings(derived, packages_metadata)

        # VV: At this point, there should be 2 graphs with dangling references to inputs and outputs.
        # The next step is to wire them together (step 2)
        self._connect_foundation_and_surrogate_graphs(derived)

        # VV: The graphs are connected, figure out which files to use from which VE definition (step 3)
        self._populate_include_files(derived, packages_metadata)

        # VV: Finally populate key outputs and interface of derived VE
        self._populate_key_outputs_and_interface(derived, packages_metadata)

        if "auto-generated" not in derived.metadata.package.keywords:
            derived.metadata.package.keywords.extend(["auto-generated", "from:relationship-transform"])

        derived.update_digest()

        return derived
