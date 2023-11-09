# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import distutils.dir_util
import logging
import os
import shutil
import re

from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import Union
from typing import Optional

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
import apis.runtime.errors


class DerivedVirtualExperimentMetadata(apis.models.virtual_experiment.VirtualExperimentMetadata):
    derived: DerivedPackage


class PackageConflictMetadata(apis.models.common.Digestable):
    name: str
    value: Any = None  # ellipsis indicates that there is no value


class _book(pydantic.BaseModel):
    location: List[Union[str, int]]
    package: str
    value: Any = None


class VariableOverride(apis.models.common.Digestable):
    variableName: str
    ownerPackageName: str


class RewireSymbol(apis.models.common.Digestable):
    reference: Optional[str] = pydantic.Field(None, description="The reference that this parameter points to")
    text: Optional[str] = pydantic.Field(None, description="The literal text that this parameter points to, "
                                                           "can contain %(parameters of caller)s")
    ownerGraphName: Optional[str] = pydantic.Field(
        None, description="The identifier of the graph that this symbol belongs to")


class RewireResults(apis.models.common.Digestable):
    variables: Dict[str, RewireSymbol] = {}
    references: Dict[str, RewireSymbol] = {}


class InstructionRewireSymbol(apis.models.common.Digestable):
    """Describes an instruction to rewire a parameter from @source into @destination"""
    source: Optional[RewireSymbol] = None
    destination: Optional[RewireSymbol] = None

    @classmethod
    def generate_instruction_to_rewire_parameter(
            cls,
            ve: apis.models.virtual_experiment.ParameterisedPackage,
            connection: apis.models.virtual_experiment.BasePackageGraphInstance,
            dest_symbol: apis.models.virtual_experiment.BindingOption,
    ) -> InstructionRewireSymbol:
        dest_pkg_name, dest_graph_name = connection.graph.partition_name()
        dest_package = ve.base.get_package(dest_pkg_name)
        dest_graph = dest_package.get_graph(dest_graph_name)

        try:
            resolved_dest_symbol = dest_graph.bindings.get_input_binding(dest_symbol.name)
        except KeyError as e:
            raise apis.runtime.errors.RuntimeError(
                f"The binding {dest_symbol.name} does not exist for graph {connection.graph.name}. "
                f"Underlying error: {e}")

        rewire_symbol_dest = RewireSymbol(reference=resolved_dest_symbol.reference, text=resolved_dest_symbol.text)
        rewire_symbol_source = RewireSymbol()

        rewire = InstructionRewireSymbol(source=rewire_symbol_source, destination=rewire_symbol_dest)

        if dest_symbol.valueFrom.graph:
            source_pkg_name, source_graph_name = dest_symbol.valueFrom.graph.partition_name()
            source_package = ve.base.get_package(source_pkg_name)
            source_graph = source_package.get_graph(source_graph_name)

            if dest_symbol.valueFrom.graph.binding.type != "output":
                raise apis.runtime.errors.RuntimeError(
                    f"Expected input binding to point to output binding a name "
                    f"but input binding definition is {dest_symbol.dict()}")

            if not dest_symbol.valueFrom.graph.binding.name:
                raise apis.runtime.errors.RuntimeError(
                    f"Expected input binding to contain a name but it is {dest_symbol.dict()}")

            try:
                bind = source_graph.bindings.get_output_binding(dest_symbol.valueFrom.graph.binding.name)
                rewire_symbol_source.text = bind.text
                rewire_symbol_source.reference = bind.reference
            except KeyError:
                # VV: The outputGraph parameter does not exist, this is problematic if the producer
                # of the reference is a component. However, if the producer is an application-dependency
                # or an input, data file then this is fine because the synthesized virtual experiment
                # will contain the producer
                dref = apis.models.from_core.DataReference(dest_symbol.valueFrom.graph.binding.name)
                if dref.externalProducerName:
                    rewire_symbol_source.reference = dref.absoluteReference
                else:
                    raise apis.runtime.errors.RuntimeError(
                        f"The parameter {dest_symbol.valueFrom.graph.binding.name} does not exist for"
                        f"the graph {dest_symbol.valueFrom.graph.name}")
        elif dest_symbol.valueFrom.applicationDependency:
            rewire_symbol_source.reference = dest_symbol.valueFrom.applicationDependency.reference
        else:
            raise apis.runtime.errors.RuntimeError(f"Cannot rewire symbols using {dest_symbol.model_dump_json(indent=2)}")

        return rewire

    @classmethod
    def rewrite_reference_in_arguments_of_component(
            cls,
            component: experiment.model.frontends.flowir.DictFlowIRComponent,
            reference: str,
            new: str
    ):
        """Replaces a @reference in the arguments of a component with some @new text

        Arguments:
            component: The DSL of the component to update
            reference: The string representation of a DataReference to replace with @new
            new: The text to use when replacing the @reference in the arguments of the @component
        """
        args: str = component.get('command', {}).get('arguments', {})

        if args:
            dref_old = apis.models.from_core.DataReference(reference, stageIndex=component.get('stage', 0))
            for old in [dref_old.absoluteReference, dref_old.relativeReference]:
                if old in args:
                    args = args.replace(old, new)
                    break

            component['command']['arguments'] = args

    @classmethod
    def infer_replace_reference_with_text(
            cls,
            ref: str,
            rule_match: str,
            text: str,
    ) -> str | None:
        """Returns a string that could replace a reference that matches a rule

        A rule_match "matches" ref if they both point to the same producer and they reference the exact same path
        (i.e. :<method> could be the only difference between the 2 DataReferences)

        Arguments:
            ref: The reference that could be replaced
            rule_match: The rule to use - it is a string representation of a DataReference
            text: The string that could replace @ref if @rule_match is a hit

        Returns:
            @text if ref "matches" rule_match otherwise None
        """
        if rule_match == ref:
            return text

        dref = apis.models.from_core.DataReference(ref)
        dref_match = apis.models.from_core.DataReference(rule_match)

        if (dref_match.trueProducer == dref.trueProducer and
                dref_match.pathRef == dref.pathRef):
            return text

    @classmethod
    def infer_replace_reference_with_reference(
            cls,
            ref: str,
            rule_match: str,
            rule_replace: str,
    ) -> str | None:
        """Returns a DataReference string representation that could replace a reference that matches a rule

        A rule_match "matches" ref if they both point to the same producer and:
            they reference the exact same path
                (i.e. :<method> could be the only difference between the 2 DataReferences), OR
            rule_replace points to the root of a producer
                (i.e both <:method> and [/filepath] could differ provided that rule_replace points to root of producer)

        Arguments:
            ref: The reference that could be replaced
            rule_match: The rule to use - it is a string representation of a DataReference
            rule_replace: The DataReference string representation that could replace @ref if @rule_match is a hit

        Returns:
            a DataReference string representation if ref "matches" rule_match otherwise None
        """
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
    def replace_reference_with_reference(
            cls,
            conf: experiment.model.frontends.flowir.DictFlowIRComponent,
            rule_match: str,
            rule_ref_replace: str
    ) -> Dict[str, str]:
        """Replaces references of a component that match a rule, using a replacement rule and returns the mapping
        of old references to new

        Arguments:
            conf: The DSL of the component
            rule_match: A string representation of a DataReference to replace
            rule_ref_replace: A string representation of a DataReference to use as a replacement

        Returns:
            A Dictionary whose keys are old references of the component and values are the replacement references
        """
        references: List[str] = conf.get('references', [])

        ret = {}

        if not references:
            return ret

        # VV: First try to find a reference that directly matches `rule_match`
        try:
            index = references.index(rule_match)
        except ValueError:
            # VV: This component does not have this **EXACT** reference
            index = None

        def replace_index_with(index: int, replacement: str):
            dref = apis.models.from_core.DataReference(replacement)
            old = references[index]
            if dref.method not in ['link', 'copy']:
                cls.rewrite_reference_in_arguments_of_component(conf, references[index], dref.absoluteReference)
            ret[old] = dref.absoluteReference
            conf['references'][index] = ret[old]

        try:
            # VV: We are replacing a specific reference with another reference
            if index is not None:
                replace_index_with(index, rule_ref_replace)
        except Exception:
            raise apis.runtime.errors.RuntimeError(f"Replacement rule {rule_ref_replace} is not a valid DataReference")

        # VV: Regardless of whether we found the `rule_match` as is or not, we can try out a
        # couple more things
        for index, ref in enumerate(references):
            replacement = cls.infer_replace_reference_with_reference(
                ref=ref, rule_match=rule_match, rule_replace=rule_ref_replace)

            if replacement is not None:
                replace_index_with(index, replacement)

        return ret

    @classmethod
    def replace_reference_with_text(
            cls,
            conf: experiment.model.frontends.flowir.DictFlowIRComponent,
            rule_match: str,
            text: str
    ) -> Dict[str, str]:
        """Replaces references of a component that match a rule, using a replacement rule and returns the mapping
        of old references to new

        Arguments:
            conf: The DSL of the component
            rule_match: A string representation of a DataReference to replace
            text: A string, may contain %(references to variables)s

        Returns:
            A Dictionary whose keys are old references of the component and values are the replacement strings
        """
        references: List[str] = conf.get('references', [])

        ret = {}

        if not references:
            return ret

        # VV: First try to find a reference that directly matches `rule_match`
        try:
            index = references.index(rule_match)
        except ValueError:
            # VV: This component does not have this **EXACT** reference
            index = None

        to_remove = set()

        def replace_index_with(index: int, replacement: str):
            old = references[index]
            ret[old] = replacement
            cls.rewrite_reference_in_arguments_of_component(conf, references[index], replacement)
            to_remove.add(index)

        # VV: We are replacing a specific reference with a string
        if index is not None:
            replace_index_with(index, text)

        # VV: Regardless of whether we found the `rule_match` as is or not, we can try out a
        # couple more things
        for index, ref in enumerate(references):
            replacement = cls.infer_replace_reference_with_text(ref=ref, rule_match=rule_match, text=text)

            if replacement is not None:
                replace_index_with(index, replacement)

        for index in sorted(to_remove, reverse=True):
            references.pop(index)

        return ret

    @classmethod
    def replace_variable_with_text(
            cls,
            conf: experiment.model.frontends.flowir.DictFlowIRComponent,
            variable: str,
            text: str,
    ) -> Dict[str, str]:
        """Replaces references to a @variable with a @text

        If the component contains a variable @variable it will be removed

        Arguments:
            conf: The DSL of the component
            variable: The name of the variable for which to replace %(variable)s with @text
            text: A string, may contain %(references to variables)s

        Returns:
            A Dictionary which may have up to 1 key (@variable) and the value will be @text
        """

        variables = conf.get('variables', {})

        try:
            del variables[variable]
        except KeyError:
            pass

        ret = {}

        old = f'%({variable})s'

        def update_references_to_variable(what: Any, label: str):
            if isinstance(what, dict):
                for key in what:
                    value = what[key]

                    if isinstance(value, str) and old in value:
                        value = value.replace(old, text)
                        what[key] = value
                        ret[variable] = value
            elif isinstance(what, list) and not isinstance(what, str):
                for key in range(len(what)):
                    value = what[key]

                    if isinstance(value, str) and old in value:
                        value = value.replace(old, text)
                        what[key] = value
                        ret[variable] = value

        experiment.model.frontends.flowir.FlowIR.visit_all(conf, update_references_to_variable, label="component")

        return ret

    def replace_variable_with_reference(
            cls,
            conf: experiment.model.frontends.flowir.DictFlowIRComponent,
            variable: str,
            reference: str,
    ) -> Dict[str, str]:
        """Replaces references to a @variable with a @reference

        If the component contains a variable @variable it will be removed
        If the component does not contain a reference @reference it will after this method IF it references
            the variable

        Arguments:
            conf: The DSL of the component
            variable: The name of the variable for which to replace %(variable)s with @text
            reference: An absolute string representation of a DataReference

        Returns:
            A Dictionary which may have up to 1 key (@variable) and the value will be @reference
        """

        variables = conf.get('variables', {})

        try:
            del variables[variable]
        except KeyError:
            pass

        ret = {}

        old = f'%({variable})s'

        problems = []

        # VV: We should only replace a variable with a reference if the variable is referenced in the
        # command-line arguments. Everything else should be flagged as a Problem
        def identify_problems(what: Any, label: str):
            if isinstance(what, str):
                if old in what and label != "component.command.arguments":
                    problems.append(label)

        if problems:
            comp_name = f"stage{conf.get('stage', 0)}.{conf.get('name', '**unknown**')}"
            raise apis.runtime.errors.RuntimeError(f"Cannot replace variable {variable} with reference {reference} "
                                                   f"in component {comp_name} because the component uses the "
                                                   f"variable in {sorted(set(problems))}")

        experiment.model.frontends.flowir.FlowIR.visit_all(conf, identify_problems, label="component")
        args = conf.get('command', {}).get('arguments', '')

        if old in args:
            args = args.replace(old, reference)
            conf['command']['arguments'] = args
            ret[variable] = reference

            references = conf.get('references', [])
            # VV: HACK We probably need to find a good way to check both absolute and relative forms of references
            if reference not in references:
                references.append(reference)
            conf['references'] = references

        return ret

    def is_use_reference_for_reference(self) -> bool:
        return bool(self.source.reference) and bool(self.destination.reference)

    def is_use_text_for_reference(self) -> bool:
        if not self.destination.reference:
            return False
        return self.source.text is not None

    def is_use_reference_for_variable(self) -> bool:
        p_variables = re.compile(experiment.model.frontends.flowir.FlowIR.VariablePattern)
        return bool(self.source.reference) and bool(self.destination.text)

    def is_use_text_for_variable(self) -> bool:
        return bool(self.source.text) and bool(self.destination.text)

    def apply_to_component(
            self,
            component: experiment.model.frontends.flowir.DictFlowIRComponent,
            comp_label: Optional[str] = None,
    ) -> RewireResults:
        """Attempts to apply this rewire symbol instruction to 1 component

        Arguments:
            component: The DSL of the component to update
            comp_label: An optional name for the component to use when raising exceptions

        Returns:
            A RewireResults object explaining the changes made to the @component
        """
        comp_label = comp_label or f"stage{component.get('stage', 0)}.{component.get('name', '*unknown*')}"
        logger = logging.getLogger('RewireSymbol')
        ret = RewireResults()

        if self.is_use_reference_for_reference():
            logger.info(f"May rewire REF {self.destination.reference} with REF {self.source.reference} in {comp_label}")
            op = self.replace_reference_with_reference(
                conf=component,
                rule_match=self.destination.reference,
                rule_ref_replace=self.source.reference)
            for (key, value) in op.items():
                ret.references[key] = RewireSymbol(reference=value)
        elif self.is_use_text_for_reference():
            logger.info(f"May rewire REF {self.destination.reference} with TEXT {self.source.text} in {comp_label}")
            op = self.replace_reference_with_text(conf=component, rule_match=self.destination.reference,
                                                  text=self.source.text)
            for (key, value) in op.items():
                ret.references[key] = RewireSymbol(text=value)
        elif self.is_use_text_for_variable():
            logger.info(f"May rewire VAR {self.destination.text} with TEXT {self.source.text} in {comp_label}")
            op = self.replace_variable_with_text(conf=component, variable=self.destination.text,
                                                 text=self.source.text)
            for (key, value) in op.items():
                ret.variables[key] = RewireSymbol(text=value)
        elif self.is_use_reference_for_variable():
            logger.info(
                f"May rewire VAR {self.destination.text} with REF {self.source.reference} in {comp_label}")
            op = self.replace_variable_with_reference(conf=component, variable=self.destination.text,
                                                      reference=self.source.reference)
            for (key, value) in op.items():
                ret.variables[key] = RewireSymbol(reference=value)
        else:
            raise apis.runtime.errors.RuntimeError(f"Unable to rewire symbols of {comp_label} with "
                                                   f"{self.model_dump_json(indent=2)}")

        logger.info(f"Rewiring results: {ret.model_dump_json(indent=2)} {op}")

        return ret


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
            return x is None or x is ... or isinstance(x, (bool, int, float) + six.string_types)

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
    vGlobal: Dict[str, apis.models.common.MustBeString] = pydantic.Field(
        {}, description="Global variables", alias="global")
    stages: Dict[
        int, Dict[str, apis.models.common.MustBeString]
    ] = pydantic.Field({}, description="Variables in stages")


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

    aggregate_variables: VariableCollection = VariableCollection()
    aggregate_blueprints: BlueprintCollection = BlueprintCollection()
    aggregate_components: List[experiment.model.frontends.flowir.DictFlowIRComponent] = []
    aggregate_environments: Any = None


class ExplanationManifestDir(apis.models.common.Digestable):
    name: str = pydantic.Field(..., description="The directory name")
    origin: str = pydantic.Field(..., description="The identifier of the original owner of the directory")
    paths: List[str] = pydantic.Field(..., description="A list of paths relative to the directory")


class VariableValueFromBasePackage(apis.models.common.Digestable):
    fromBasePackage: str = pydantic.Field(..., description="The package from which the variable receive the value")
    value: str = pydantic.Field(..., description="The value of the variable")


class ExplanationVariableValue(apis.models.common.Digestable):
    value: str = pydantic.Field(..., description="The value of the variable")
    platform: str = pydantic.Field(..., description="The name of the platform")
    overrides: Optional[List[VariableValueFromBasePackage]] = pydantic.Field(
        None, description="The values that .value overrides from the other graph. If None then this variable does "
                          "not exist in the other graph for the implied platform")


class ExplanationVariable(apis.models.common.Digestable):
    fromBasePackage: str = pydantic.Field(..., description="The package from which the variable receive these values")
    values: List[ExplanationVariableValue] = pydantic.Field(
        [], description="Explains how the variable receives its value for that platform in the DSL")
    preset: Optional[str] = pydantic.Field(
        None, description="The preset value set in the parameterisation.presets field. Overrides .dslValues")
    executionOptions: Optional[str] = pydantic.Field(
        None, description="The default value from the parameterisation.executionOptions field. Cannot specify this "
                          "if .preset is also defined. Overrides .dslValues")

    def get_platform_value(self, platform: str) -> ExplanationVariableValue:
        platforms = []
        for v in self.values:
            if v.platform == platform:
                return v
            platforms.append(v.platform)

        raise KeyError(f"Unknown platform {platform} - platforms are {platforms}")

    def update_platform_value(self, platform: str, value: ExplanationVariableValue):
        try:
            existing = self.get_platform_value(platform)
            existing.value = value.value
            existing.overrides = value.overrides
            existing.platform = platform
        except KeyError:
            self.values.append(value)

    def get_default_value(self, platform: str) -> str:
        if self.preset is not None:
            return self.preset
        if self.executionOptions is not None:
            return self.executionOptions

        return self.get_platform_value(platform).value


class ExplanationDerived(apis.models.common.Digestable):
    # manifest: List[ExplanationManifestDir] = []
    variables: Dict[str, ExplanationVariable] = {}

    def try_add_overriden_value_for_package(self, variable: str, value: str, platform: str, package: str):
        existing = self.variables[variable]

        for ev in existing.values:
            if ev.platform == platform:
                matching = [x for x in ev.overrides or [] if x.fromBasePackage == package]
                if matching:
                    matching[0].value = ev
                    return
                else:
                    overrides = ev.overrides or []
                    overrides.append(VariableValueFromBasePackage(fromBasePackage=package, value=value))
                    return

        raise KeyError(f"The variable {variable} does not have a value for platform {platform} and therefore "
                       f"it cannot override the value {value} from package {package}")

    def register_override_values(
            self,
            variable: str,
            platform_vars: Dict[str, Dict[str, str]],
            from_base_package: str,
            override_other_base_package: str,
            overriden_concrete: experiment.model.frontends.flowir.FlowIRConcrete,
            preset: Optional[str],
            execution_options: Optional[str],
    ):
        dsl_values = []
        for platform in platform_vars:
            if variable not in platform_vars[platform]:
                continue

            if platform in overriden_concrete.platforms:
                gp = platform
            else:
                gp = 'default'

            val = overriden_concrete.get_platform_variables(gp)['global'].get(variable)
            over = None
            if val is not None:
                over = [VariableValueFromBasePackage(value=val, fromBasePackage=override_other_base_package)]

            e = ExplanationVariableValue(value=platform_vars[platform][variable], platform=platform,
                                         overrides=over)
            # if e.overrides.value == e.value:
            #     e.overrides = None

            dsl_values.append(e)

        expl = ExplanationVariable(
            fromBasePackage=from_base_package, values=dsl_values,
            preset=preset, executionOptions=execution_options)

        if variable not in self.variables:
            self.variables[variable] = expl
        else:
            existing = self.variables[variable]

            # VV: Migrate old overrides into new explanation - IF old overrides did not involve the new fromBasePackage
            for old_value in existing.values:
                matching = [x for x in expl.values if x.platform == old_value.platform]
                if not matching:
                    continue
                matching = matching[0]
                for ov in (old_value.overrides or []):
                    if ov.fromBasePackage != from_base_package:
                        matching.overrides.append(ov)

            self.variables[variable] = expl

            for new_value in expl.values:
                old_value = [old for old in existing.values if old.platform == new_value.platform]
                if old_value:
                    old_value = old_value[0]

                    self.try_add_overriden_value_for_package(
                        variable=variable,
                        value=old_value.value,
                        platform=old_value.platform,
                        package=existing.fromBasePackage
                    )


def explain_choices_in_derived(
        ve: apis.models.virtual_experiment.ParameterisedPackage,
        packages: apis.storage.PackageMetadataCollection,
        all_platforms: Optional[List[str]] = None
) -> ExplanationDerived:
    log = logging.getLogger("explain")
    ret = ExplanationDerived()

    if all_platforms is None:
        all_platforms = ve.get_known_platforms() or []

    if 'default' not in all_platforms:
        all_platforms = list(all_platforms)
        all_platforms.append('default')

    presets = {x.name: x.value for x in ve.parameterisation.presets.variables}

    def get_value(v: apis.models.common.OptionMany):
        if v.value is not None:
            return v.value

        if v.valueFrom:
            return v.valueFrom[0].value

    execution_options = {x.name: get_value(x) for x in ve.parameterisation.executionOptions.variables}

    # VV: Names of variables that graphs do not explicitly define as inputBindings
    # key is variable name, value is the last graph that had this variable in its definition
    implicit_variables: Dict[str, str] = {}

    for connection in ve.base.connections:
        last_package, graph_name = connection.graph.partition_name()
        graph_concrete = packages.get_concrete_of_package(last_package)

        graph_variables = set()

        for platform in all_platforms:
            try:
                platform_vars = graph_concrete.get_platform_variables(platform)
            except experiment.model.errors.FlowIRPlatformUnknown:
                continue
            graph_variables.update(platform_vars['global'])

            for name in platform_vars['global']:
                name = str(name)

                if name not in ret.variables:
                    implicit_variables[name] = last_package

        for dest_symbol in connection.bindings:
            if dest_symbol.valueFrom.applicationDependency:
                # VV: We are setting this variable to a reference i.e. there won't be a variable anymore
                continue

            if dest_symbol.valueFrom.graph is None:
                raise apis.runtime.errors.RuntimeError(f'inputBinding {dest_symbol.model_dump_json(indent=2)} is invalid')

            graph = ve.base.get_package(last_package).get_graph(graph_name)

            input_binding = graph.bindings.get_input_binding(dest_symbol.name)
            if input_binding.reference:
                # VV: The destination symbol is a reference, we don't care about it
                continue

            if input_binding.text not in graph_variables:
                raise apis.runtime.errors.RuntimeError(f'inputBinding {input_binding.model_dump_json(indent=2)} in '
                                                       f'{connection.graph.name} is an invalid Variable definition '
                                                       f'it points to a variable that does not exist')
            dest_name = input_binding.text
            # VV: We are setting the value of `dest_name` based on the values of 0+ variables in another graph
            other_package, other_graph_name = dest_symbol.valueFrom.graph.partition_name()
            other_concrete = packages.get_concrete_of_package(other_package)
            other_graph = ve.base.get_package(other_package).get_graph(other_graph_name)
            source_symbol = other_graph.bindings.get_output_binding(dest_symbol.valueFrom.graph.binding.name)

            if source_symbol.reference:
                continue

            value = source_symbol.text
            if value is None:
                raise apis.runtime.errors.RuntimeError(
                    f'The value to {input_binding.model_dump_json(indent=2)} in {connection.graph.name} does not '
                    f'have .text but {source_symbol.model_dump_json(indent=2)}')

            # VV: FIXME what about indirect variables?
            platform_vars = {
                platform: {
                    str(k): str(v) for k, v in other_concrete.get_platform_variables(platform)['global'].items()
                } for platform in all_platforms if platform in other_concrete.platforms
            }

            other_vars = set()
            for platform in platform_vars:
                missing = []
                ref_vars = experiment.model.frontends.flowir.FlowIR.discover_indirect_dependencies_to_variables(
                    value, platform_vars[platform], missing)
                other_vars.update(ref_vars)
                if missing:
                    log.warning(f"The value {value} references the variables {missing} which do not exist in the "
                                f"platform {platform} of package {other_package} - will ignore them")

            if not other_vars:
                continue

            for dest_name in sorted(other_vars):
                ret.register_override_values(
                    variable=dest_name,
                    platform_vars=platform_vars,
                    from_base_package=other_graph_name,
                    overriden_concrete=graph_concrete,
                    override_other_base_package=last_package,
                    preset=presets.get(dest_name),
                    execution_options=execution_options.get(dest_name),
                )

    return ret


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

    def get_parameterised_package(self) -> apis.models.virtual_experiment.ParameterisedPackage:
        return self._ve

    @property
    def data_files(self) -> List[str]:
        return list(self._data_files)

    @property
    def concrete_synthesized(self) -> experiment.model.frontends.flowir.FlowIRConcrete:
        return self._synthesized_concrete.copy()

    def _rewire_component_parameters(
            self,
            component: experiment.model.frontends.flowir.DictFlowIRComponent,
            connection: apis.models.virtual_experiment.BasePackageGraphInstance,
    ) -> RewireResults:
        comp_label = f"{connection.graph.name}/stage{component.get('stage', 0)}.{component.get('name', '*unknown*')}"

        ret = RewireResults()

        for dest_symbol in connection.bindings:
            instruction = InstructionRewireSymbol.generate_instruction_to_rewire_parameter(
                ve=self._ve, dest_symbol=dest_symbol, connection=connection)

            self._log.info(f"Generated InstructionRewireSymbol {instruction.model_dump_json(indent=2)} "
                           f"from {dest_symbol.model_dump_json(indent=2)}")

            x = instruction.apply_to_component(component=component, comp_label=comp_label)

            if dest_symbol.valueFrom.graph:
                for r in x.references:
                    x.references[r].ownerGraphName = dest_symbol.valueFrom.graph.name
                for v in x.variables:
                    x.variables[v].ownerGraphName = dest_symbol.valueFrom.graph.name

            ret.references.update(x.references)
            ret.variables.update(x.variables)

        return ret


    def extract_graphs(
            self,
            package_metadata: apis.storage.PackageMetadataCollection,
            platforms: List[str] | None
    ) -> GraphsFromManyPackagesMetadata:
        if platforms is None:
            platforms = package_metadata.get_common_platforms()

        if not platforms:
            self._log.warning("Missing list of platforms, will synthesize "
                              "just for 'default' platform")
            platforms = ['default']

        if 'default' not in platforms:
            platforms = ['default'] + platforms

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

        variable_overrides: Dict[str, VariableOverride] = {}

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
            # VV: TODO how do we handle missing stages? e.g. derived package contains stages 0 and 28 - what do we do?

            for node in graph_template.nodes:
                cid = experiment.model.graph.ComponentIdentifier(node.reference)
                comp_id = (cid.stageIndex, cid.componentName)
                conf = concrete.get_component(comp_id)
                all_components[pkg_name][node.reference] = conf
                self._log.info(f"Adding {pkg_name}/{node.reference}={yaml.dump(conf)}")

                # VV: Rewrite parameters (references and variables) to point them to where the associated
                # inputBindings are pointing to
                rewire = self._rewire_component_parameters(component=conf, connection=c)

                for name in rewire.variables:
                    meta = rewire.variables[name]
                    if meta.text:
                        # VV: FIXME what about indirect variables?
                        other_vars = experiment.model.frontends.flowir.FlowIR.discover_references_to_variables(
                            meta.text)

                        if not other_vars:
                            continue

                        if not meta.ownerGraphName:
                            raise apis.runtime.errors.RuntimeError(
                                f"InstructionRewireSymbol results {meta.model_dump_json(indent=2)} does not contain "
                                f"an ownerGraphName")
                        owner_package, _ = meta.ownerGraphName.split('/')
                        for var_name in other_vars:
                            variable_overrides[var_name] = VariableOverride(
                                variableName=var_name, ownerPackageName=owner_package)

                missing_variables = []

                for platform in platforms:
                    if platform not in concrete.platforms:
                        self._log.warning(f"Platform {platform} does not exist in {pkg_name} "
                                          f"- will skip processing components")
                        continue
                    conf_with_bp = concrete.get_component_configuration(
                        comp_id, raw=True, include_default=True, inject_missing_fields=True, platform=platform,
                        is_primitive=True)

                    env_name = conf_with_bp['command'].get('environment')

                    # VV: The special environment "none" or "" is not actually present in the FlowIR
                    # the "environment" (or None) environment can be auto-generated if it doesn't exist
                    # in the FlowIR
                    all_env_names = list(concrete.get_environments(platform=platform).keys())

                    # VV: Components which do not request a specific environment get the "environment"
                    # environment
                    if env_name is None:
                        env_name = "environment"

                    # VV: environment names are case-insensitive
                    env_name = env_name.lower()

                    if env_name in [None, "environment"]:
                        # VV: If the FlowIR contains the "environment" environment then grab it,
                        # otherwise let st4sd-runtime-core auto-generate it at the time of execution
                        extract_env = "environment" in all_env_names
                    else:
                        # VV: "" and "none" will never appear in the FlowIR therefore we won't try to extract them
                        # VV: We expect to have the definitions of all other envs
                        extract_env = env_name.lower() not in ["", "none"]

                    if extract_env:
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

                if platform not in concrete.platforms:
                    self._log.warning(f"Platform {platform} does not exist in {pkg_name} "
                                      f"- will skip processing variables")
                    continue

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

                vars_platform = PlatformVariables.model_validate(concrete.get_platform_variables(platform))
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

        # VV: As a final step apply @variable_overrides
        for v in variable_overrides.values():
            for p in aggregate_vars.platforms:
                concrete = package_metadata.get_concrete_of_package(v.ownerPackageName)

                if p not in concrete.platforms:
                    continue

                platform_vars = concrete.get_platform_variables(p)
                try:
                    aggregate_vars.platforms[p].vGlobal[v.variableName] = platform_vars['global'][v.variableName]
                except KeyError as e:
                    self._log.warning(f"Cannot copy {v.model_dump_json(indent=2)} for platform {p} - will ignore")
                    continue
                self._log.info(f"Copied {v.model_dump_json(indent=2)} = {platform_vars['global'][v.variableName]} "
                               f"from platform {p}")

        # VV: Finally trim the aggregate variables to remove variables whose value is identical to the variable value
        # in the default platform

        default_values = aggregate_vars.platforms['default'].vGlobal

        for platform in aggregate_vars.platforms:
            if platform == 'default':
                continue

            for (name, value) in list(aggregate_vars.platforms[platform].vGlobal.items()):
                if name in default_values and str(default_values[name]) == str(value):
                    del aggregate_vars.platforms[platform].vGlobal[name]

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

        if not platforms:
            raise apis.models.errors.ApiError("Missing list of platforms for which to synthesize the derived package")

        self._log.info(f"Synthesizing parameterised virtual experiment package for Derived (platforms: {platforms})")
        graphs_meta = self.extract_graphs(package_metadata, platforms)

        self._log.info(f"Extracted graphsMetadata: {graphs_meta.model_dump_json(indent=2)}")

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

        logging.getLogger().warning(f"THE BASE IS {self._ve.base.model_dump_json(indent=2)}")

        top_level_directories = sorted({apis.models.virtual_experiment.extract_top_level_directory(x.dest.path)
                                        for x in self._ve.base.includePaths})

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
                    f"The source path in IncludePath {ip.model_dump_json(indent=2)} does not exist")

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


# VV: This is so that DerivedVirtualExperimentMetadata can contain DerivedPackage
DerivedVirtualExperimentMetadata.update_forward_refs()
