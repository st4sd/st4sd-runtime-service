# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import copy
import pprint
from typing import Any
from typing import Dict
from typing import List
from typing import NamedTuple

import pydantic.error_wrappers

import apis.db.exp_packages
import apis.db.relationships
import apis.models.common
import apis.models.errors
import apis.models.query_experiment
import apis.models.relationships
import apis.models.virtual_experiment
import utils


class FormatOptions(apis.models.common.Digestable):
    outputFormat: str
    hideMetadataRegistry: str
    hideNone: str
    hideBeta: str


class ParameterisedPackageAndProblems(NamedTuple):
    experiment: apis.models.virtual_experiment.ParameterisedPackage
    problems: List[Dict[str, Any]]


# VV: TODO Refactor code to organize codes that APIs call so that the HTTP codes are just a proxy to methods
def api_query_experiments(
        query: apis.models.query_experiment.QueryExperiment,
        db_experiments: apis.db.exp_packages.DatabaseExperiments,
        db_relationships: apis.db.relationships.DatabaseRelationships | None,
) -> List[Dict[str, Any]]:
    package = None

    if query.package:
        package = query.package.definition
    elif query.relationship:
        if db_relationships is None:
            raise apis.models.errors.ApiError(f"Invalid query for parameterised virtual experiments {query.dict()} - "
                                              f"unable to access relationship database")

        with db_relationships:
            ql = db_relationships.construct_query(query.relationship.identifier)
            docs = db_relationships.query(ql)

        if len(docs) != 1:
            raise apis.models.errors.ApiError(f"Unknown relationship \"{query.relationship.identifier}\"")

        try:
            rel: apis.models.relationships.Relationship = apis.models.relationships.Relationship.parse_obj(docs[0])
        except pydantic.error_wrappers.ValidationError:
            return []

        if query.relationship.transform:
            if (query.relationship.transform.matchInputGraph or query.relationship.transform.matchOutputGraph) is False:
                raise apis.models.errors.ApiError(
                    f"Invalid query for parameterised virtual experiments {query.dict()} - must set either "
                    f"relationship.transform.matchInputGraph or relationship.transform.matchOutputGraph to True")

            if query.relationship.transform.matchInputGraph:
                package = rel.transform.inputGraph.package
            elif query.relationship.transform.matchOutputGraph:
                package = rel.transform.outputGraph.package

    if package is None:
        raise apis.models.errors.ApiError(f"Invalid query for parameterised virtual experiments {query.dict()}")

    if query.common.matchPackageVersion is False:
        if package.source.git:
            package.source.git.version = None

    # VV: We only care about things under `source` and `config` here
    maintain_top_level_fields = ["source", "config"]
    for key, _value in package:
        if key not in maintain_top_level_fields:
            setattr(package, key, None)

    ql = db_experiments.construct_query_for_package(
        package=package,
        have_just_one_package=query.common.mustHaveOnePackage)
    with db_experiments:
        return db_experiments.query(ql)


def do_format_parameterised_package(
        package: apis.models.virtual_experiment.ParameterisedPackage | Dict[str, Any],
        format_options: FormatOptions
) -> Any:
    if isinstance(package, apis.models.virtual_experiment.ParameterisedPackage):
        what = package.dict(exclude_none=format_options.hideNone == "y")
    else:
        what = copy.deepcopy(package)

    if format_options.hideMetadataRegistry == "y":
        del what['metadata']['registry']

    if format_options.hideBeta == "y":
        if 'base' in what:
            for x in ['connections', 'includePaths', 'output', 'interface']:
                if x in what['base']:
                    del what['base'][x]

            if 'packages' in what['base']:
                many = what['base']['packages']
                for bp in many:
                    for x in ['graphs']:
                        if x in bp:
                            del bp[x]

    if format_options.outputFormat == "python":
        what = str(what)
    elif format_options.outputFormat == "python-pretty":
        what = pprint.pformat(what, width=120)

    return what


def format_documents(docs: List[Dict[str, Any]], format_options: FormatOptions) -> Dict[str, Any]:
    entries = []
    problems = []

    for doc in docs:
        try:
            obj = apis.models.virtual_experiment.ParameterisedPackageDropUnknown.parse_obj(doc)
        except pydantic.error_wrappers.ValidationError as e:
            package_name = doc.get('metadata', {}).get('package', {}).get('name', '**unknown**')
            digest = doc.get('metadata', {}).get('registry', {}).get('digest', '**unknown**')
            identifier = '@'.join((package_name, digest))

            problems.append({'identifier': identifier, 'problems': e.errors()})
            obj = doc

        entries.append(do_format_parameterised_package(obj, format_options))

    return {
        'entries': entries,
        'problems': problems,
    }


def api_list_queries(request: Dict[str, Any], format_options: FormatOptions):
    db_experiments = utils.database_experiments_open()

    if not request:
        with db_experiments:
            docs = db_experiments.query()
    else:
        try:
            query = apis.models.query_experiment.QueryExperiment.parse_obj(request)
        except pydantic.error_wrappers.ValidationError as e:
            raise apis.models.errors.ApiError(f"Invalid request, problems: {e.json(indent=2)}")
        db_relationships = utils.database_relationships_open()
        docs = api_query_experiments(query=query, db_experiments=db_experiments, db_relationships=db_relationships)

    return format_documents(docs, format_options)


def api_get_experiment(
        identifier: str,
        db_experiment: apis.db.exp_packages.DatabaseExperiments,
        try_drop_unknown: bool = True,
) -> ParameterisedPackageAndProblems:
    identifier = apis.models.common.PackageIdentifier(identifier).identifier

    with db_experiment:
        docs = db_experiment.query_identifier(identifier)

    if len(docs) == 0:
        raise apis.models.errors.ParameterisedPackageNotFoundError(identifier)

    problems = []

    try:
        ve = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(docs[0])
    except pydantic.error_wrappers.ValidationError as e:
        problems = e.errors()

        try:
            if try_drop_unknown:
                ve = apis.models.virtual_experiment.ParameterisedPackageDropUnknown \
                    .parse_obj(docs[0])
            else:
                raise apis.models.errors.InvalidModelError(
                    f"Parameterised virtual experiment package {identifier} is invalid", problems)
        except pydantic.ValidationError:
            # VV: We cannot auto-upgrade the package, just return the original problems so that the devs fix everything
            raise apis.models.errors.InvalidModelError(
                f"Parameterised virtual experiment package {identifier} is invalid", problems)

    return ParameterisedPackageAndProblems(experiment=ve, problems=problems)
