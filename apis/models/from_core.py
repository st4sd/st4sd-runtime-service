# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


"""Models in here should really be part of st4sd-runtime-core"""

from __future__ import annotations

import json
import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import experiment.model.frontends.flowir
import experiment.model.graph
import experiment.service.db
import experiment.service.errors
import pydantic
from pydantic import validator

import apis.models.common


class FlowIROutputEntry(apis.models.common.Digestable):
    description: Optional[str] = None
    stages: Optional[List[str]] = None
    type: Optional[str] = None
    data_in: Optional[str] = pydantic.Field(
        ..., description="An absolute reference (if no stages) or relative reference (if stages present) "
                         "to outputs that a component (or components with same name in multiple stages) produces",
        alias="data-in", title="data-in")

    @validator("data_in")
    def is_valid_reference(cls, value: str, values: Dict[str, Any]) -> str:
        producer, filename, method = experiment.model.frontends.flowir.FlowIR.ParseDataReference(value)

        comp_id = experiment.model.graph.ComponentIdentifier(producer)

        stages = values.get('stages', [])

        if stages and comp_id.stageIndex is not None:
            raise ValueError(f"data-in must be relative reference when stages is non-empty, however it was {value}")
        elif not stages and comp_id.stageIndex is None:
            raise ValueError(f"data-in must be absolute reference when stages is empty, however it was {value}")

        return value


class ExtractionMethodSource(apis.models.common.DigestableSingleField):
    path: Optional[str] = None
    pathList: Optional[List[str]] = None
    keyOutput: Optional[str] = None


class InputExtractionMethodCsvColumn(apis.models.common.Digestable):
    # VV: TODO no source.keyOutput
    source: Optional[ExtractionMethodSource] = None
    args: Dict[str, Any] = {}


class InputExtractionMethodHookGetInputIds(apis.models.common.Digestable):
    source: Optional[ExtractionMethodSource] = None


class FlowIRInterfaceInputExtractionMethod(apis.models.common.DigestableSingleField):
    hookGetInputIds: Optional[InputExtractionMethodHookGetInputIds] = None
    csvColumn: Optional[InputExtractionMethodCsvColumn] = None


class FlowIRInterfaceInputSpec(apis.models.common.Digestable):
    namingScheme: str
    inputExtractionMethod: FlowIRInterfaceInputExtractionMethod
    hasAdditionalData: bool = False


class PropertyExtractionMethodHookGetProperties(apis.models.common.Digestable):
    source: Optional[ExtractionMethodSource] = None


class PropertyExtractionMethodCsvDataFrame(apis.models.common.Digestable):
    source: Optional[ExtractionMethodSource] = None
    # VV TODO: `args.columnNames` points to a Dict[str, str]
    args: Dict[str, Any] = {}


class FlowIRInterfacePropertyExtractionMethod(apis.models.common.DigestableSingleField):
    hookGetProperties: Optional[PropertyExtractionMethodHookGetProperties] = None
    csvDataFrame: Optional[PropertyExtractionMethodCsvDataFrame] = None


class FlowIRInterfacePropertySpec(apis.models.common.Digestable):
    name: str
    description: Optional[str] = None
    propertyExtractionMethod: FlowIRInterfacePropertyExtractionMethod


class FlowIRInterface(apis.models.common.Digestable):
    id: Optional[str] = None
    description: Optional[str] = None
    inputSpec: FlowIRInterfaceInputSpec
    propertiesSpec: List[FlowIRInterfacePropertySpec] = []

    # VV: These are ADDED by st4sd-runtime-core
    inputs: Optional[List[str]] = None
    additionalInputData: Optional[Dict[str, List[str]]] = None
    outputFiles: Optional[List[str]] = None


class FlowIR(apis.models.common.Digestable):
    output: Dict[str, FlowIROutputEntry] = {}
    interface: Optional[FlowIRInterface] = None


class DataReference(experiment.model.graph.DataReference):
    @classmethod
    def from_parts(cls, stage: int | None, producer: str, fileRef: str, method: str):
        reference = ""
        if stage is not None:
            reference = f"stage{stage}."
        reference += producer
        if fileRef and fileRef != "/":
            if fileRef.startswith("/") is False:
                fileRef = "/" + fileRef
            reference += fileRef
        reference += ":" + method

        return DataReference(reference, stage)

    @property
    def pathRef(self) -> str:
        fileref = self.fileRef or ''

        # VV: We may join this later with a path OR handle an :output reference, it's best we treat `/` as empty
        if fileref == '/':
            fileref = ''

        if '/' in self.producerName:
            _, fileref_prefix = self.producerName.split('/', 1)
            if fileref_prefix and fileref:
                fileref = '/'.join((fileref_prefix, fileref))
            else:
                fileref = fileref_prefix

        # VV: `:output` implies a reference to the `out.stdout` file ONLY if fileref is empty
        if self.method != type(self).Output:
            return fileref

        # VV: This is an :output reference, so return fileRef (if it's non-empty) OR `out.stdout` if it's empty
        return fileref or 'out.stdout'

    @property
    def externalProducerName(self) -> str | None:
        if '/' in self.producerName:
            return self.producerName.split('/', 1)[0]

    @property
    def method(self) -> str:
        """Returns the reference's method"""

        return self._referenceMethod

    @method.setter
    def method(self, value: str):
        self._referenceMethod = value

    @property
    def trueProducer(self) -> str:
        return self.externalProducerName or self.producerName

    def __repr__(self):
        return self.absoluteReference


class BetaExperimentRestAPI(experiment.service.db.ExperimentRestAPI):

    @classmethod
    def print_experiment(cls, experiments: List[Dict[str, Any]]):
        try:
            logger = cls.log
        except AttributeError:
            logger = logging.getLogger('db')

        first = True
        for exp in experiments:
            if not first:
                logger.info("-----")

            first = False
            package_name: str = exp.get('metadata', {}).get('package', {}).get('name')
            if package_name:
                registry_tag: str = exp.get('metadata', {}).get('registry', {}).get('tag')
                if registry_tag:
                    registry_tag = ":".join((package_name, registry_tag))
                    logger.info(f"Tag: {registry_tag}")

                registry_digest: str = exp.get('metadata', {}).get('registry', {}).get('digest')
                if registry_digest:
                    registry_digest = "@".join((package_name, registry_digest))
                    logger.info(f"Digest: {registry_digest}")

            logger.info(json.dumps(exp, sort_keys=True, indent=2, separators=(',', ': ')))

    def api_experiment_query(
            self,
            query: Dict[str, Any],
            print_too=False,
            treat_problems_as_errors: bool = False,
            _api_verbose=True
    ) -> Dict[str, Dict[str, Any]]:
        """Returns a list of all virtual experiment entries on the runtime service that match query

        Args:
            query: The query (see ST4SD documentation for schema)
            print_too: if True, method will also format and print the reply of the ST4SD Runtime Service REST-API server
            treat_problems_as_errors: If set to True, will raise a ProblematicEntriesError exception if runtime
                service reports that the definition of the parameterised virtual experiment packages contain problems
            _api_verbose: when True print out information about the request

        Returns:
            A dictionary whose keys are `${experimentName}@${experimentDigest}` and values are the experiment
            definitions
        Raises:
            experiment.errors.UnauthorisedRequest: when user is unauthorized to the ST4SD Runtime Service REST-API
            experiment.errors.InvalidHTTPRequest: when response HTTP status is other than 200
            experiment.service.errors.ProblematicEntriesError: if treat_problems_as_error is True and ST4SD Runtime
                Service REST-API reports that the definitions of the parameterised virtual experiment packages
                contain problems
        """
        results: Dict[str, Any] = self.api_request_post(
            'query/experiments/', json_payload=query, _api_verbose=_api_verbose)
        experiments: List[Dict[str, Any]] = results['entries']

        problems: List[Dict[str, Any]] = results.get('problems')

        if problems:
            self.log.warning(f"RestAPI is reporting the following problems with the parameterised "
                             f"virtual experiment packages {problems}")

            if treat_problems_as_errors:
                raise experiment.service.errors.ProblematicEntriesError(
                    problems, "Runtime service reports problematic parameterised virtual experiment package entries")

        with_uids = {
            f"{x['metadata']['package']['name']}@{x['metadata']['registry']['digest']}": x for x in experiments
        }

        if print_too:
            self.print_experiment(experiments)

        return with_uids
