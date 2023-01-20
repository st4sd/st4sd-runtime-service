# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import logging
from typing import List
from typing import Optional
from typing import Set
from typing import Dict
from typing import Callable

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
import apis.runtime.package_derived
import apis.storage

def get_parameters_of_component(
        component: experiment.model.frontends.flowir.DictFlowIRComponent
) -> List[str]:
    """Let's assume that `references` are graphParameters"""
    return sorted(component.get('references', []))


def get_workflow_parameter_names(
        concrete: experiment.model.frontends.flowir.FlowIRConcrete,
        cb_filter: Callable[[str], bool] | None = None
) -> Set[str]:
    """Extracts the top-level parameters in the workflow (assumes that references are the only parameters)"""
    ret = set()

    comp_ids = concrete.get_component_identifiers(recompute=False)

    for cid in comp_ids:
        comp = concrete.get_component(cid)
        parameters = get_parameters_of_component(comp)

        if cb_filter is not None:
            parameters = [p for p in parameters if cb_filter(p)]

        ret.update(parameters)

    return ret


class TransformRelationship:
    def __init__(
            self,
            transformation: apis.models.relationships.Transform,
            output_graph_name: str = "Foundation",
            input_graph_name: str = "Surrogate",
    ):
        self._transform = transformation
        self._input_graph_name = input_graph_name
        self._output_graph_name = output_graph_name
        self._log = logging.getLogger('transform')

    def discover_parameterised_packages(
            self,
            db_packages: apis.db.exp_packages.DatabaseExperiments,
    ):

        for identifier in [self._transform.inputGraph.identifier, self._transform.outputGraph.identifier]:
            docs = db_packages.query_identifier(identifier)
            if len(docs) == 0:
                raise apis.models.errors.ApiError(f"Database does not contain "
                                                  f"the parameterised virtual experiment package \"{identifier}\"")
            elif len(docs) > 1:
                raise apis.models.errors.ApiError(f"Database contains multiple parameterised virtual experiment "
                                                  f"packages with the identifier \"{identifier}\"")
            pvep = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(docs[0])
            if len(pvep.base.packages) != 1:
                raise apis.models.errors.ApiError(
                    f"Cannot use parameterised virtual experiment package {identifier} "
                    f"because it does not have exactly 1 base package "
                    f"(it has {len(pvep.base.packages)} base packages)")
            if identifier == self._transform.inputGraph.identifier:
                self._transform.inputGraph.package = pvep.base.packages[0]
            else:
                self._transform.outputGraph.package = pvep.base.packages[0]

    def guess_parameter_of_surrogate(
            self,
            foundation: experiment.model.frontends.flowir.DictFlowIRComponent,
            surrogate: experiment.model.frontends.flowir.DictFlowIRComponent,
            parameter_name: str,
    ) -> str:
        surrogate_name = experiment.model.graph.ComponentIdentifier(surrogate['name']).identifier
        foundation_params = get_parameters_of_component(foundation)
        surrogate_dref = apis.models.from_core.DataReference(parameter_name)

        surrogate_pathref = surrogate_dref.pathRef
        matching = []
        self._log.info(f"Guessing parameter {parameter_name} of surrogate {surrogate_name}")

        for param_foundation in foundation_params:
            foundation_dref = apis.models.from_core.DataReference(param_foundation)

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
                dref = apis.models.from_core.DataReference(parameter)

                return dref.externalProducerName is None and \
                    (dref.producerIdentifier.identifier not in excl_components)

            return filter_out_components

        surrogate_cids = concrete_surr.get_component_identifiers(recompute=False)
        not_in_inputgraph = []

        for cid in surrogate_cids:
            ref = experiment.model.graph.ComponentIdentifier(cid[1], cid[0]).identifier
            if ref not in transform.inputGraph.components:
                not_in_inputgraph.append(ref)

        # VV: Foundation (i.e. output) and Surrogate (i.e. input) parameters
        p_found = get_workflow_parameter_names(
            concrete_found, cb_filter=exclude_components(transform.outputGraph.components))
        p_surr = get_workflow_parameter_names(concrete_surr, cb_filter=exclude_components(not_in_inputgraph))

        parameters_found = [apis.models.from_core.DataReference(ref) for ref in p_found]
        parameters_surr = [apis.models.from_core.DataReference(ref) for ref in p_surr]

        self._log.info(f"Parameters Foundation {parameters_found}")
        self._log.info(f"Parameters Surrogate {parameters_surr}")

        known_mappings = set()
        for m in transform.relationship.graphParameters:
            if m.inputGraphParameter.name and (m.outputGraphParameter.default or m.outputGraphParameter.name):
                known_mappings.update(m.inputGraphParameter.name)

        # VV: First prepare the mappings between graph parameters. If we find an inputGraph parameter for which
        # there is no mapping to an outputGraph then try to find if there's an outputGraph component which:
        # 1. does not get removed by transform
        # 2. has the same name as the inputGraph parameter
        # If there's such an outputGraph then generate a graphParameters mapping between the 2 components
        for surrogate in transform.inputGraph.components:
            cid = experiment.model.graph.ComponentIdentifier(surrogate)
            comp_params = get_parameters_of_component(concrete_surr.get_component((cid.stageIndex, cid.componentName)))

            for p in filter(lambda x: x not in known_mappings, comp_params):
                dref_surr = apis.models.from_core.DataReference(p)
                # VV: External dependencies (e.g. input, or  application dependency) are special,
                # we don't need to care for them at all. They will just point to a directory under $INSTANCE_DIR
                # We also don't care about parameters which are satisfied by components in the inputGraph
                if dref_surr.externalProducerName \
                        or dref_surr.producerIdentifier.identifier in transform.inputGraph.components:
                    continue

                for dref_found in parameters_found:
                    if dref_found.producerIdentifier == dref_surr.producerIdentifier:
                        # VV: There's a component in the outputGraph that the relationship does not remove
                        # it has the same name as a component that was satisfying the dependency in inputGraph
                        # let's infer that the 2 components are equal

                        # VV: Here we use the foundation stage/index and the expected reference method (from surrogate)
                        ref_surr = apis.models.from_core.DataReference.from_parts(
                            stage=dref_surr.stageIndex, producer=dref_surr.producerName, fileRef='',
                            method=dref_surr.method).absoluteReference
                        ref_found = apis.models.from_core.DataReference.from_parts(
                            stage=dref_found.stageIndex, producer=dref_found.producerName, fileRef='',
                            method=dref_surr.method).absoluteReference

                        self._log.info(
                            f"   Matching {dref_found.absoluteReference} with "
                            f"{dref_surr.absoluteReference} on pathRef= {dref_surr.pathRef}")

                        transform.relationship.graphParameters.append(
                            apis.models.relationships.RelationshipParameters(
                                inputGraphParameter=apis.models.relationships.GraphValue(name=ref_surr),
                                outputGraphParameter=apis.models.relationships.GraphValue(name=ref_found)))

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

        parameters_surrogate = get_parameters_of_component(comp_surrogate)

        if transform.relationship.inferParameters:
            for name_in in parameters_surrogate:
                try:
                    value = transform.relationship.get_parameter_relationship_by_name_input(name_in)
                    if value.outputGraphParameter.default or value.inputGraphParameter.name:
                        # VV: No need to infer anything for this graphParameter
                        continue
                except KeyError:
                    # VV: This means that there's no relationships of an $in parameter to
                    # this $out one, so we need to infer it
                    pass

                # VV: If we got here, then we need to infer the relationships for inputGraphParameter
                self._log.log(15, f"Try infer relationships of inputGraphParameter.{name_in})")

                inferred = self.guess_parameter_of_surrogate(comp_foundation, comp_surrogate, name_in)

                if inferred is not None:
                    transform.relationship.graphParameters.append(
                        apis.models.relationships.RelationshipParameters(
                            outputGraphParameter=apis.models.relationships.GraphValue(
                                name=inferred),
                            inputGraphParameter=apis.models.relationships.GraphValue(
                                name=name_in)
                        ))

        if transform.relationship.inferResults and not transform.relationship.graphResults:
            name_foundation = apis.models.from_core.DataReference(
                ':'.join((cid_foundation.identifier, 'ref'))).absoluteReference
            name_surrogate = apis.models.from_core.DataReference(
                ':'.join((cid_surrogate.identifier, 'ref'))).absoluteReference
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
        super(TransformRelationshipToDerivedPackage, self).__init__(transformation=transformation)

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

        derived.base.packages.append(foundation_package)
        derived.base.packages.append(surrogate_package)

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

    def _populate_graph_bindings(
            self,
            graph: apis.models.virtual_experiment.BasePackageGraph,
            is_foundation: bool
    ):
        """Adds graphBindings to connect the components in the Foundation to those in the Surrogate

        The Foundation is the parameterised virtual experiment package whose sub-graph is outputGraph.

        The Surrogate is the parameterised virtual experiment package whose sub-graph is inputGraph
        @self._transform explains how to substitute an occurrence of outputGraph with transform(inputGraph)

        Arguments:
            graph: The graph to populate with bindings (input and output). The method updates the @graph in-place.
            is_foundation: Whether the graph is a representation of Foundation (True) or Surrogate (False)

        Returns:
            The method returns None, it updates @graph in-place.
        """
        if is_foundation:
            # VV: I am the outputGraph which means I need to bind the parameters of the inputGraph
            # to whatever is populating MY input parameters. Therefore, I'll create output "bindings" that point
            # to what's populating MY input parameters
            # I also need to consume the outputs of the inputGraph therefore I need to create inputBindings
            for x in self._transform.relationship.graphParameters:
                v = x.outputGraphParameter

                # VV: For the time being we can assume that parameters are references,
                if not v.name:
                    raise apis.models.errors.ApiError(f"We do not know how to create an output binding for "
                                                      f"{graph.name} using graphParameter {x}")

                # VV :"input" refs are external, we do not need to have an output binding for those
                dref = apis.models.from_core.DataReference(v.name)
                if dref.externalProducerName:
                    continue

                graph.bindings.output.append(
                    # VV: FIXME FlowIR 2.0 will give us a way to define these bindings better
                    apis.models.virtual_experiment.GraphBinding(name=v.name, reference=v.name))

            for x in self._transform.relationship.graphResults:
                v = x.outputGraphResult

                if not v.name:
                    raise apis.models.errors.ApiError(f"We do not know how to create an input binding for "
                                                      f"{graph.name} using graphResult {x}")
                graph.bindings.input.append(
                    # VV: FIXME FlowIR 2.0 will give us a way to define these bindings better
                    apis.models.virtual_experiment.GraphBinding(name=v.name, reference=v.name))
        else:
            # VV: I am the inputGraph - the foundation is going to populate my inputs with its outputs
            # and my outputs will populate its inputs
            for x in self._transform.relationship.graphParameters:
                v = x.inputGraphParameter

                if not v.name:
                    raise apis.models.errors.ApiError(f"We do not know how to create an input binding for "
                                                      f"{graph.name} using graphParameter {x}")

                graph.bindings.input.append(
                    # VV: FIXME FlowIR 2.0 will give us a way to define these bindings better
                    apis.models.virtual_experiment.GraphBinding(name=v.name, reference=v.name))

            # VV: The inputGraph results are probably consumed by the foundation so create output bindings for those
            for x in self._transform.relationship.graphResults:
                v = x.outputGraphResult if is_foundation else x.inputGraphResult

                if not v.name:
                    raise apis.models.errors.ApiError(f"We do not know how to create an output binding for "
                                                      f"{graph.name} using graphParameter {x}")

                graph.bindings.output.append(
                    # VV: FIXME FlowIR 2.0 will give us a way to define these bindings better
                    apis.models.virtual_experiment.GraphBinding(name=v.name, reference=v.name))

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

        self._populate_graph_bindings(foundation_graph, is_foundation=True)
        self._populate_graph_bindings(surrogate_graph, is_foundation=False)

    def _connect_foundation_and_surrogate_graphs(
            self,
            derived: apis.models.virtual_experiment.ParameterisedPackage,
    ):
        transform = self._transform
        foundation_package = derived.base.get_package(transform.outputGraph.identifier)
        if len(foundation_package.graphs) != 1:
            raise apis.models.errors.ApiError(f"Expected Foundation graph of {foundation_package.name} to "
                                              f"have exactly 1 graph, but it has {len(foundation_package.graphs)}")

        foundation_graph = foundation_package.graphs[0]
        foundation_connection = apis.models.virtual_experiment.BasePackageGraphInstance(
            graph=apis.models.virtual_experiment.BasePackageGraph(
                name="/".join((transform.outputGraph.identifier, self._output_graph_name)),
            ),
            bindings=[
                apis.models.virtual_experiment.BindingOption(
                    name=x.name,
                    valueFrom=apis.models.virtual_experiment.BindingOptionValueFrom(
                        graph=apis.models.virtual_experiment.BindingOptionValueFromGraph(
                            name="/".join((transform.inputGraph.identifier, self._input_graph_name)),
                            binding=apis.models.virtual_experiment.GraphBinding(
                                name=transform.relationship
                                .get_result_relationship_by_name_output(x.name)
                                .inputGraphResult.name
                            )
                        )
                    )
                )
                for x in foundation_graph.bindings.input
            ]
        )

        derived.base.connections.append(foundation_connection)

        surrogate_package = derived.base.get_package(transform.inputGraph.identifier)
        if len(surrogate_package.graphs) != 1:
            raise apis.models.errors.ApiError(
                f"Expected Surrogate graph of {surrogate_package.name} to have exactly 1 "
                f"graph, but it has {len(surrogate_package.graphs)}")

        surrogate_graph = surrogate_package.graphs[0]
        surrogate_connections = apis.models.virtual_experiment.BasePackageGraphInstance(
            graph=apis.models.virtual_experiment.BasePackageGraph(
                name="/".join((transform.inputGraph.identifier, self._input_graph_name)),
            ),
            bindings=[
                apis.models.virtual_experiment.BindingOption(
                    name=x.name,
                    valueFrom=apis.models.virtual_experiment.BindingOptionValueFrom(
                        graph=apis.models.virtual_experiment.BindingOptionValueFromGraph(
                            name="/".join((transform.outputGraph.identifier, self._output_graph_name)),
                            binding=apis.models.virtual_experiment.GraphBinding(
                                name=transform.relationship
                                .get_parameter_relationship_by_name_input(x.name)
                                .outputGraphParameter.name
                            )
                        )
                    )
                )
                # VV: Parameters to surrogate that end up pointing to `input` application-dependency should NOT
                # be wired to the foundation graph
                for x in surrogate_graph.bindings.input if apis.models.from_core.DataReference(
                    transform.relationship
                    .get_parameter_relationship_by_name_input(x.name)
                    .outputGraphParameter.name
                ).externalProducerName != "input"
            ]
        )

        derived.base.connections.append(surrogate_connections)

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

        DerivedPackage = apis.runtime.package_derived.DerivedPackage
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
                        replacement = DerivedPackage.check_if_can_replace_reference(
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
            5. Inherit parameterisation from Foundation, but trim Platforms to just those that
               are common between the 2 VEs
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
