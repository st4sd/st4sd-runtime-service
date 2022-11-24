# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import distutils.dir_util
import logging
import os
import shutil
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union

import experiment.model.errors
import experiment.model.frontends.flowir
import experiment.model.graph
import pydantic
import six
import yaml

import apis.models.common
import apis.models.constants
import apis.models.errors
import apis.models.from_core
import apis.models.virtual_experiment
import apis.storage


class PackageConflictMetadata(apis.models.common.Digestable):
    name: str
    value: Any  # ellipsis indicates that there is no value


class _book(pydantic.BaseModel):
    location: List[Union[str, int]]
    package: str
    value: Any


def extract_top_level_directory(path: str) -> str:
    if '/' in path:
        return path.split('/', 1)[0]

    return path


class PackageConflict(apis.models.common.Digestable):
    location: List[Union[str, int]]
    packages: List[PackageConflictMetadata]

    def get_package(self, name: str) -> PackageConflictMetadata:
        for x in self.packages:
            if x.name == name:
                return x
        else:
            raise KeyError(f"Unknown package {name}")

    @classmethod
    def find_conflicts(cls, packages: Dict[str, Any]) -> List[PackageConflict]:
        if len(packages) < 2:
            raise ValueError("Need at least 2 packages to find conclicts")

        def extract_field(objects: Dict[str, Dict[str, Any]] | Dict[str, List[Any]], key: str | int,
                          location: List[str | int]) -> Tuple[_book]:
            location = location + [key]

            def get_key_or_ellipsis(package: Dict[str, Any] | List[Any] | ellipsis) -> Any | ellipsis:
                try:
                    return package[key]
                except (IndexError, KeyError, TypeError):
                    return ...

            return tuple(_book(package=x, value=get_key_or_ellipsis(objects[x]), location=location) for x in objects)

        def is_primitive(x: Any) -> bool:
            return x is None or x == ... or isinstance(x, (bool, int, float) + six.string_types)

        remaining = [
            tuple(_book(package=x, value=packages[x], location=[]) for x in packages)
        ]

        conflicts = []

        def kernel(what: Tuple[_book]):
            aggregate = {
                x.package: x.value for x in what if x.value is not ...
            }
            all_primitive = all(map(is_primitive, aggregate.values()))

            if all_primitive:
                unique_values = set(aggregate.values())
                if len(unique_values) > 1:
                    conflicts.append(
                        PackageConflict(location=what[0].location, packages=[
                            PackageConflictMetadata(name=x.package, value=x.value) for x in what])
                    )
            else:
                keys = set()
                for x in what:
                    if is_primitive(x.value) is False:
                        if isinstance(x.value, dict):
                            keys.update(x.value.keys())
                        elif isinstance(x.value, list):
                            keys.update(range(0, len(x.value)))
                        else:
                            raise NotImplementedError(f"Cannot extract keys from type {type(x.value)}={x.value}")
                remaining.extend([
                    extract_field({x.package: x.value for x in what}, key=key, location=what[0].location)
                    for key in keys
                ])

        while remaining:
            what = remaining.pop(0)
            kernel(what)

        return conflicts


class PlatformVariables(apis.models.common.Digestable):
    # VV: `global` is a reserved python word and we cannot use it here
    vGlobal: Dict[str, str] = pydantic.Field(
        {}, description="Global variables", alias="global")
    stages: Dict[int, Dict[str, str]] = pydantic.Field({}, description="Variables in stages")


class VariableCollection(apis.models.common.Digestable):
    platforms: Dict[str, PlatformVariables] = {
        'default': PlatformVariables()
    }


class PlatformBlueprint(apis.models.common.Digestable):
    vGlobal: Dict[str, Dict[str, Any]] = pydantic.Field(
        {}, description="Global blueprint", alias="global")
    stages: Dict[int, Dict[str, Dict[str, Any]]] = pydantic.Field({}, description="Blueprint of stages")

    def dict(
            self,
            *,
            exclude_none: bool = True,
            **kwargs
    ) -> Dict[str, Any]:
        to_rename = super(PlatformBlueprint, self).dict(exclude_none=exclude_none, **kwargs)

        return {
            {'vGlobal': 'global'}.get(x, x): to_rename[x] for x in to_rename
        }


class BlueprintCollection(apis.models.common.Digestable):
    platforms: Dict[str, PlatformBlueprint] = {
        'default': PlatformBlueprint()
    }


class GraphsFromManyPackagesMetadata(apis.models.common.Digestable):
    """Dictionaries have schema : {name: <some collection of values>}"""
    variables: Dict[str, VariableCollection] = {}
    blueprints: Dict[str, BlueprintCollection] = {}
    components: Dict[str, Dict[str, experiment.model.frontends.flowir.DictFlowIRComponent]] = {}

    aggregate_variables: VariableCollection
    aggregate_blueprints: BlueprintCollection
    aggregate_components: List[experiment.model.frontends.flowir.DictFlowIRComponent]
    aggregate_environments: Any


class DerivedPackage:
    """Putting this here because I don't know what else I'll need, I'll move around this code later

    Stuff to contemplate ::

        1. Cannot instantiate graph multiple times because we cannot reference to specific instance of graph when
           referencing Outputs
        2. We assume that we are wiring together the "default" platform
        3. We need to identify variables that components in each graph use and somehow end up with an aggregate graph
           that uses the correct variables of each "sub-graph" in the appropriate places
        4. Need to merge the bin files
        5. How do we handle references to application dependencies?
        6. How do we handle a component in stage 4 ending up consuming outputs of a component in stage 6?
    """

    def __init__(
            self,
            ve: apis.models.virtual_experiment.ParameterisedPackage,
            directory_to_place_derived: str | None = None
    ):
        # VV: We may want to change this directory when unit-testing the functionality
        self._root_derived = directory_to_place_derived or apis.models.constants.ROOT_STORE_DERIVED_PACKAGES
        self._ve = ve

        self._synthesized_concrete = experiment.model.frontends.flowir.FlowIRConcrete({}, platform=None, documents={})
        self._synthesized_top_level_dirs: Dict[str, List[str]] = {}
        self._data_files: List[str] = []

        self._log = logging.getLogger("Derive")

    @property
    def data_files(self) -> List[str]:
        return list(self._data_files)

    @property
    def concrete_synthesized(self) -> experiment.model.frontends.flowir.FlowIRConcrete:
        return self._synthesized_concrete.copy()

    @classmethod
    def check_if_can_replace_reference(
            cls,
            ref: str,
            rule_match: str,
            rule_replace: str,
    ) -> str | None:
        if rule_match == ref:
            return rule_replace

        dref = apis.models.from_core.DataReference(ref)
        dref_replace = apis.models.from_core.DataReference(rule_replace)
        dref_match = apis.models.from_core.DataReference(rule_match)

        if dref_replace.pathRef in ['/', ''] and dref_match.trueProducer == dref.trueProducer:
            replacement = apis.models.from_core.DataReference.from_parts(
                stage=dref_replace.stageIndex, producer=dref_replace.trueProducer,
                fileRef=dref.pathRef, method=dref.method
            )
            return replacement.absoluteReference

        if (dref_match.trueProducer == dref.trueProducer and
                dref_match.pathRef == dref.pathRef):
            replacement = apis.models.from_core.DataReference.from_parts(
                stage=dref_replace.stageIndex, producer=dref_replace.trueProducer,
                fileRef=dref_match.pathRef, method=dref.method
            )
            return replacement.absoluteReference

    @classmethod
    def try_replace_all_references_in_component(
            cls,
            conf: experiment.model.frontends.flowir.DictFlowIRComponent,
            rule_match: str,
            rule_replace: str,
            pkg_name: str,
            logger: logging.Logger | None = None
    ) -> bool:
        if logger is None:
            try:
                logger = cls._log
            except AttributeError:
                logger = logging.getLogger('replace')

        logger.info(f"  Trying to replace {rule_match} with {rule_replace} in "
                    f"{pkg_name}/stage{conf.get('stage', 0)}/{conf['name']}")

        def perform_replace(index: int, replacement: str):
            logger.info(
                f"{pkg_name}/stage{conf.get('stage', 0)}/{conf['name']} --- {rule_match} -> {replacement}")
            old_ref = conf['references'][index]
            conf['references'][index] = replacement

            args: str = conf.get('command', {}).get('arguments', {})
            dref = apis.models.from_core.DataReference(replacement)

            dref_old = apis.models.from_core.DataReference(old_ref, stageIndex=conf.get('stage', 0))

            if args and dref.method not in ['link', 'copy']:
                args = args.replace(dref_old.absoluteReference, dref.absoluteReference)
                args = args.replace(dref_old.absoluteReference, dref.relativeReference)

                args = args.replace(dref_old.relativeReference, dref.absoluteReference)
                args = args.replace(dref_old.relativeReference, dref.relativeReference)

                conf['command']['arguments'] = args

        references: List[str] = conf['references']
        ret = False
        # VV: First try to find a reference that directly matches `rule_match`
        try:
            index = references.index(rule_match)
            perform_replace(index, rule_replace)
            ret = True
        except ValueError:
            pass

        # VV: Regardless of whether we found the `rule_match` as is or not, we can try out a
        # couple more things
        for index, ref in enumerate(references):
            replacement = cls.check_if_can_replace_reference(ref, rule_match, rule_replace)
            if replacement is not None:
                perform_replace(index, replacement)
                ret = True

        return ret

    def extract_graphs_and_metadata(
            self,
            package_metadata: apis.storage.PackageMetadataCollection,
            platforms: List[str] | None
    ) -> GraphsFromManyPackagesMetadata:
        if platforms is None:
            platforms = package_metadata.get_common_platforms()

        if not platforms:
            raise apis.models.errors.ApiError("Missing a list of platforms for which to synthesize the derived package")

        # VV: format is {pkgName: <something>}
        all_vars: Dict[str, VariableCollection] = {}
        all_blueprints: Dict[str, BlueprintCollection] = {}
        # VV: Format is {pkgName: { componentName: componentDict} }
        all_components: Dict[
            str, Dict[str, experiment.model.frontends.flowir.DictFlowIRComponent]] = {}

        # VV: The challenge here is that we can be instantiating multiple graphs from multiple
        # base packages. These graphs COULD have conflicting blueprints and conflicting variables.
        # E.g. graph 1 could use simulationThreshold = 0.1 and another simulationThreshold=100
        # because the 2 graphs use different simulation algorithms which produce "equivalent" outputs.
        # Given that we want the "instantiated" graph to "function the way it wants" we should resolve
        # the blueprint/variables conflict by picking whatever the "instantiated" graph dictates.
        # It follows that in this first MVP the eventual blueprints/variables are dependent on the
        # order we traverse the graph instantiations in VirtualExperiment.base.connections - for now
        # we just follow the order they appear in Virtual Experiment entry (base.connections field).
        aggregate_vars = VariableCollection()

        # VV: Schema is {platform: {environmentName:  {key: value}} }
        aggregate_environments: Dict[str, Dict[str, Dict[str, str]]] = {}

        # VV: This one is tricky to merge because deeply nested fields may contain Lists, let's keep it around
        # as a Dictionary and use experiment.model.frontends.flowir.FlowIR.override_object() to handle overriding
        # the deeply nested dictionaries
        aggregate_bps = {
            'platforms': {
                platform: {
                    'global': {},
                    'stages': {
                    }
                } for platform in platforms
            }

        }

        for c in self._ve.base.connections:
            pkg_name, graph_name = c.graph.partition_name()
            concrete = package_metadata.get_concrete_of_package(pkg_name)

            package = self._ve.base.get_package(pkg_name)
            graph_template = package.get_graph(graph_name)
            num_stages = concrete.get_stage_number()

            if pkg_name not in all_blueprints:
                all_blueprints[pkg_name] = BlueprintCollection()
                all_vars[pkg_name] = VariableCollection()
                all_components[pkg_name] = {}

            all_referenced_variables = set()

            # VV: Pretty sure, I'm missing something here.
            # VV: TODO how do we handle conflicting component names?
            # VV: TODO how do handle missing stages? e.g. derived package contains stages 0 and 28 - what do we do?

            for node in graph_template.nodes:
                cid = experiment.model.graph.ComponentIdentifier(node.reference)
                comp_id = (cid.stageIndex, cid.componentName)
                conf = concrete.get_component(comp_id)
                all_components[pkg_name][node.reference] = conf
                self._log.info(f"Adding {pkg_name}/{node.reference}={yaml.dump(conf)}")

                # VV: Rewrite references to point them to where the associated inputBindings are pointing to
                # VV: References in .references are always in "absolute" form however, references in .command.arguments
                #     MAY be in relative form!

                if conf.get('references', {}):
                    for b in c.bindings:
                        self._log.info(f"Processing input binding {b.name} for {conf['name']}")
                        input_binding = graph_template.bindings.get_input_binding(b.name)
                        if not input_binding.reference:
                            raise NotImplementedError(f"Input binding of {pkg_name}/{graph_name} "
                                                      f"does not point to a reference")
                        rule_match = input_binding.reference

                        if b.valueFrom.graph:
                            other_pkg_name, other_graph_name = b.valueFrom.graph.partition_name()
                            other_pkg = self._ve.base.get_package(other_pkg_name)
                            other_graph_template = other_pkg.get_graph(other_graph_name)

                            if b.valueFrom.graph.binding.type != "output":
                                raise NotImplementedError(f"Expected input binding to point to output binding a name "
                                                          f"but input binding definition is {b.dict()}")

                            if not b.valueFrom.graph.binding.name:
                                raise NotImplementedError(
                                    f"Expected input binding to contain a name but it is {b.dict()}")
                            binding = other_graph_template.bindings.get_output_binding(b.valueFrom.graph.binding.name)
                            rule_replace = binding.reference
                            if not rule_replace:
                                raise NotImplementedError(f"Binding {b.name} of graph {b.valueFrom.graph.name} "
                                                          f"does not point to a reference")
                        elif b.valueFrom.applicationDependency:
                            rule_replace = b.valueFrom.applicationDependency.reference
                        else:
                            raise NotImplementedError(
                                f"Expected input binding to contain valueFrom.[graph,applicationDependency] "
                                f"but it is {b.dict()}")
                        self.try_replace_all_references_in_component(
                            conf, rule_match, rule_replace, pkg_name)

                missing_variables = []

                for platform in platforms:
                    conf_with_bp = concrete.get_component_configuration(
                        comp_id, raw=True, include_default=True, inject_missing_fields=True, platform=platform,
                        is_primitive=True)

                    env_name = conf_with_bp['command'].get('environment')
                    if env_name:
                        env = concrete.get_environment(env_name, platform=platform)
                        if platform not in aggregate_environments:
                            aggregate_environments[platform] = {}
                        aggregate_environments[platform][env_name] = env

                    str_rep = yaml.dump(conf_with_bp, Dumper=yaml.SafeDumper)

                    source_variables = concrete.get_component_variables(comp_id=comp_id, platform=platform)
                    source_variables = {str(x): str(source_variables[x]) for x in source_variables}

                    ref_vars = experiment.model.frontends.flowir.FlowIR.discover_indirect_dependencies_to_variables(
                        text=str_rep, context=source_variables, out_missing_variables=missing_variables
                    )
                    all_referenced_variables.update(ref_vars)

            # VV: Here we decide the aggregate blueprints/variables that the instantiated graph uses
            for platform in platforms:
                if platform not in aggregate_vars.platforms:
                    aggregate_vars.platforms[platform] = PlatformVariables()

                all_blueprints[pkg_name].platforms[platform] = PlatformBlueprint().parse_obj({
                    'global': concrete.get_platform_blueprint(platform)
                })

                aggregate_bps['platforms'][platform][
                    'global'] = experiment.model.frontends.flowir.FlowIR.override_object(
                    aggregate_bps['platforms'][platform]['global'], concrete.get_platform_blueprint(platform)
                )

                for stage_idx in range(num_stages):
                    bp_stage = concrete.get_platform_stage_blueprint(stage_idx, platform)
                    all_blueprints[pkg_name].platforms[platform].stages[stage_idx] = bp_stage
                    if stage_idx not in aggregate_bps['platforms'][platform]['stages']:
                        aggregate_bps['platforms'][platform]['stages'][stage_idx] = {}
                    aggregate_bps['platforms'][platform]['stages'][
                        stage_idx] = experiment.model.frontends.flowir.FlowIR.override_object(
                        aggregate_bps['platforms'][platform]['stages'][stage_idx],
                        concrete.get_platform_stage_blueprint(stage_idx, platform)
                    )

                vars_platform = PlatformVariables.parse_obj(concrete.get_platform_variables(platform))
                all_vars[pkg_name].platforms[platform] = vars_platform
                all_global = all_vars[pkg_name].platforms[platform].vGlobal
                all_stages = all_vars[pkg_name].platforms[platform].stages
                all_vars[pkg_name].platforms[platform].vGlobal = {
                    str(x): all_global[x] for x in all_global if x in all_referenced_variables}

                aggregate_vars.platforms[platform].vGlobal.update(all_vars[pkg_name].platforms[platform].vGlobal)
                for idx in all_stages:
                    stage = all_stages[idx]
                    all_stages[idx] = {str(x): str(stage[x]) for x in stage if x in all_referenced_variables}

                    if idx not in aggregate_vars.platforms[platform].stages:
                        aggregate_vars.platforms[platform].stages[idx] = {}
                    aggregate_vars.platforms[platform].stages[idx].update(all_stages[idx])

        aggregate_components: List[experiment.model.frontends.flowir.DictFlowIRComponent] = []
        for pkg_name in all_components:
            for comp_name in all_components[pkg_name]:
                conf = all_components[pkg_name][comp_name]
                aggregate_components.append(conf)

        return GraphsFromManyPackagesMetadata(
            variables=all_vars,
            blueprints=all_blueprints,
            components=all_components,
            aggregate_variables=aggregate_vars,
            aggregate_blueprints=BlueprintCollection.parse_obj(aggregate_bps),
            aggregate_components=aggregate_components,
            aggregate_environments=aggregate_environments
        )

    def synthesize_output(self) -> Dict[str, apis.models.from_core.FlowIROutputEntry]:
        """Synthesizes the key-outputs of this derived virtual experiment"""
        synthetic = apis.models.from_core.FlowIR()

        for o in self._ve.base.output:
            if o.valueFrom.graph:
                graph = o.valueFrom.graph

                if o.valueFrom.graph.binding.type != "output":
                    raise NotImplementedError(f"Expected binding of keyOutput {o.name} to point to output binding "
                                              f"but keyOutput binding definition is {o.dict()}")

                pkg_name, graph_name = graph.partition_name()
                package = self._ve.base.get_package(pkg_name)

                graph_template = package.get_graph(graph_name)
                if graph.binding.name is not None:
                    data_in = graph_template.bindings.get_output_binding(graph.binding.name).reference
                    synthetic.output[o.name] = apis.models.from_core.FlowIROutputEntry(
                        **{"data-in": data_in})
                else:
                    synthetic.output[o.name] = apis.models.from_core.FlowIROutputEntry(
                        **{"data-in": graph.binding.reference, "stages": graph.binding.stages})

            elif o.valueFrom.applicationDependency:
                raise NotImplementedError(f"valueFrom.applicationDependency for key output {o.name} not implemented")
            else:
                raise ValueError(f"Synthetic output {o.name} does not point to a value")

        return synthetic.output

    def synthesize(
            self,
            package_metadata: apis.storage.PackageMetadataCollection,
            platforms: List[str] | None
    ):
        # VV: Step 1 - for each base package, extract:
        #   1. components to use
        #   2. variables that components reference
        #   3. blueprints that platforms/stages contain
        #   4. aggregate components
        #   5. aggregate blueprints (with conflicts resolved - see step 2 for info regarding conflicts)
        #   6. aggregate variables (with conflicts resolved - see step 2 for info regarding conflicts)

        if platforms is None:
            platforms = package_metadata.get_common_platforms()

        if not platforms:
            raise apis.models.errors.ApiError("Missing list of platforms for which to synthesize the derived package")

        self._log.info(f"Synthesizing parameterised virtual experiment package for Derived (platforms: {platforms})")
        graphs_meta = self.extract_graphs_and_metadata(package_metadata, platforms)

        self._log.info(f"Extracted graphsMetadata: {graphs_meta.dict()}")

        # VV: Step 2 -  identify conflicts between variables and blueprints in the many base-packages
        # We have a conflict, when more than 1 packages define a variable/blueprintField with more than 1 unique value
        # (the conflicts have ALREADY been resolved in step 1)
        blueprints = {x: graphs_meta.blueprints[x].dict() for x in graphs_meta.blueprints}
        bp_conflicts = PackageConflict.find_conflicts(blueprints)

        variables = {x: graphs_meta.variables[x].dict() for x in graphs_meta.variables}
        var_conflicts = PackageConflict.find_conflicts(variables)

        if bp_conflicts:
            self._log.info("Graphs define conflicting Blueprints - will layer the blueprints in the same order as "
                           "the graphs with the input bindings in VirtualExperiment.base.connections")
            self._log.info(f"Blueprint Conflicts are\n:{yaml.dump([x.dict(exclude_none=False) for x in bp_conflicts])}")

        if var_conflicts:
            self._log.info("Graphs define conflicting Variables - will layer the variables in the same order as "
                           "the graphs with the input bindings in VirtualExperiment.base.connections")
            self._log.info(f"Variable Conflicts are\n:{yaml.dump([x.dict(exclude_none=False) for x in var_conflicts])}")

        # VV: Step 3 - put aggregate_blueprints, aggregate_variables, and all_components in a single FlowIRConcrete
        blueprints = graphs_meta.aggregate_blueprints.dict(exclude_none=True)['platforms']
        variables = graphs_meta.aggregate_variables.dict(exclude_none=True)['platforms']

        top_level_directories = sorted({extract_top_level_directory(x.dest.path) for x in self._ve.base.includePaths})

        output = self.synthesize_output()

        flowir = {
            experiment.model.frontends.flowir.FlowIR.FieldBlueprint: blueprints,
            experiment.model.frontends.flowir.FlowIR.FieldVariables: variables,
            experiment.model.frontends.flowir.FlowIR.FieldComponents: graphs_meta.aggregate_components,
            experiment.model.frontends.flowir.FlowIR.FieldEnvironments: graphs_meta.aggregate_environments,
            experiment.model.frontends.flowir.FlowIR.FieldOutput: {x: output[x].dict(by_alias=True) for x in output},
        }

        interface = self._ve.base.interface
        if interface is not None:
            flowir[experiment.model.frontends.flowir.FlowIR.FieldInterface] = interface.dict(by_alias=True)

        self._synthesized_concrete = experiment.model.frontends.flowir.FlowIRConcrete(
            flowir, platform=None, documents={})

        pretty = experiment.model.frontends.flowir.FlowIR.pretty_flowir_sort(flowir)

        self._log.info(f"Resulting FlowIR:\n{experiment.model.frontends.flowir.yaml_dump(pretty, indent=2)}")

        # VV: Step 4 - Validate all platforms in derived package
        for platform in platforms:
            self._log.info(f"Validating platform {platform} of derived package")
            self._synthesized_concrete.configure_platform(platform)
            errors = self._synthesized_concrete.validate(top_level_directories)

            if errors:
                self._log.warning(
                    f"Derived package fails the validation tests for platform {platform} with {len(errors)}")
                raise experiment.model.errors.FlowIRConfigurationErrors(errors)
            self._log.info(f"Platform {platform} is valid")

    def persist_to_directory(self, path: str, packages_metadata: apis.storage.PackageMetadataCollection):
        path = os.path.abspath(os.path.normpath(path))
        self._log.info(f"Persisting derived package of {self._ve.metadata.package.name} to {path}")

        if path.startswith(self._root_derived) is False:
            raise ValueError(f"Must store derived packages in directory under "
                             f"{self._root_derived}")
        if os.path.exists(path):
            self._log.warning(f"Directory {path} already exists - will delete it")
            shutil.rmtree(path)

        for ip in self._ve.base.includePaths:
            metadata = packages_metadata.get_metadata(ip.source.packageName)
            src_path = metadata.path_offset_location(ip.source.path)
            dst_path = os.path.abspath(os.path.normpath(os.path.join(path, ip.dest.path)))

            self._log.info(f"Copying {src_path}")

            if src_path.startswith(metadata.rootDirectory) is False:
                raise ValueError(f"Must Read IncludePaths from package directory "
                                 f"{metadata.rootDirectory} not {src_path}")

            if dst_path.startswith(self._root_derived) is False:
                raise ValueError(f"Must store IncludePaths in directory under "
                                 f"{self._root_derived} not {dst_path}")

            if os.path.exists(src_path) is False:
                raise apis.models.errors.ApiError(
                    f"The source path in IncludePath {ip.json(indent=2)} does not exist")

            if os.path.isdir(src_path):
                dirname = dst_path
            else:
                dirname = os.path.dirname(dst_path)

            self._log.info(f"Copying {src_path} to {dst_path}")

            if os.path.exists(dirname) is False:
                os.makedirs(dirname, exist_ok=True)

            if os.path.isdir(src_path):
                # VV: shutil.copytree in python 3.7 does not have dirs_exist_ok flag and raises
                # exception when trying to override contents of a directory that already exists.
                # e.g. we want to run ```shutil.copytree(src_path, dst_path, dirs_exist_ok=True)```

                distutils.dir_util.copy_tree(src_path, dst_path, preserve_symlinks=0)
            else:
                shutil.copy(src_path, dst_path)

        conf_path = os.path.join(path, "conf")
        if os.path.exists(conf_path) is False:
            os.makedirs(conf_path)

        pretty_flowir = experiment.model.frontends.flowir.FlowIR.pretty_flowir_sort(self._synthesized_concrete.raw())

        with open(os.path.join(conf_path, 'flowir_package.yaml'), 'w') as f:
            experiment.model.frontends.flowir.yaml_dump(pretty_flowir, f)
