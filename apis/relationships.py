# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

from __future__ import annotations

import logging
import pprint
import traceback
from typing import Any
from typing import Dict

import pydantic.error_wrappers
import werkzeug.exceptions
from flask import request, current_app
from flask_restx import Resource, reqparse

import apis.models.constants
import apis.models.errors
import apis.models.relationships
import apis.models.virtual_experiment
import apis.runtime.package
import apis.runtime.package_transform
import apis.storage
import utils

api = apis.models.api_relationships
m_relationship = apis.models.m_relationship


def do_format_relationship(
        relationship: apis.models.relationships.Relationship | Dict[str, Any],
        parser: reqparse.RequestParser
) -> Any:
    args = parser.parse_args()
    if isinstance(relationship, apis.models.relationships.Relationship):
        what = relationship.dict(exclude_none=args.hideNone == "y")
    else:
        what = relationship

    if args.outputFormat == "python":
        what = str(what)
    elif args.outputFormat == "python-pretty":
        what = pprint.pformat(what, width=120)

    return what


def parser_formatting_relationship() -> reqparse.RequestParser:
    arg_parser = reqparse.RequestParser()
    arg_parser.add_argument(
        "outputFormat",
        choices=["json", "python", "python-pretty"],
        default="json",
        help='Output format',
        location='args')

    arg_parser.add_argument("hideNone", choices=['y', 'n'], default='y', location="args",
                            help="Whether to hide fields whose value is None")

    return arg_parser


@api.route("/")
class Relationships(Resource):
    _my_parser = parser_formatting_relationship()

    @api.expect(_my_parser)
    def get(self):
        entries = []
        problems = []

        with utils.database_relationships_open() as db:
            for doc in db.query():
                try:
                    obj = apis.models.relationships.Relationship.parse_obj(doc)
                except pydantic.error_wrappers.ValidationError as e:
                    identifier = doc.get("identifier", "**unknown**")
                    problems.append({
                        'identifier': identifier,
                        'problems': e.errors()
                    })
                    obj = doc
                entries.append(do_format_relationship(obj, self._my_parser))
        return {
            'entries': entries,
            'problems': problems,
        }

    @api.expect(m_relationship)
    def post(self):
        doc = request.get_json()

        try:
            rel = apis.models.relationships.Relationship.parse_obj(doc)
        except pydantic.error_wrappers.ValidationError as e:
            api.abort(400, "Invalid relationship", problems=e.errors())
            raise  # VV: keep linter happy
        try:
            if rel.transform:
                transform = apis.runtime.package_transform.TransformRelationshipToDerivedPackage(rel.transform)
                if rel.transform.inputGraph.package is None or rel.transform.outputGraph.package is None:
                    with utils.database_experiments_open() as db:
                        transform.discover_parameterised_packages(db)

                ve = transform.prepare_derived_package(
                    rel.identifier, parameterisation=apis.models.virtual_experiment.Parameterisation())
                try:
                    with apis.storage.PackagesDownloader(ve) as download:
                        rel.transform = transform.try_infer(download)
                except Exception as e:
                    api.abort(400, "Incomplete relationship", problem=str(e))
                    raise e  # VV: keep linter happy

            with utils.database_relationships_open() as db:
                db.upsert(rel.dict(exclude_none=False), ql=db.construct_query(rel.identifier))
            return {
                "entry": rel.dict()
            }
        except werkzeug.exceptions.HTTPException:
            raise
        except apis.models.errors.ApiError as e:
            api.abort(400, f"Invalid relationship", problem=str(e))
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while registering relationship. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(500, f"Internal error while registering relationship")


@api.route("/<identifier>/synthesize/<new_package_name>/", doc=False)
@api.route("/<identifier>/synthesize/<new_package_name>")
class TransformSynthesize(Resource):
    @api.expect(apis.models.m_payload_synthesize)
    def post(self, identifier: str, new_package_name: str):
        try:
            doc = request.get_json()
            synthesize = apis.models.relationships.PayloadSynthesize.parse_obj(doc)
        except pydantic.error_wrappers.ValidationError as e:
            api.abort(400, f"Invalid synthesize payload, problems are {e.json()}", problems=e.errors())
            raise e  # VV: Keep linter happy

        try:
            with utils.database_relationships_open() as db:
                ql = db.construct_query(identifier)
                docs = db.query(ql)

            if len(docs) == 0:
                api.abort(404, "Unknown transform relationship", unknownTransformIdentifier=identifier)
            try:
                rel = apis.models.relationships.Relationship.parse_obj(docs[0])
            except pydantic.error_wrappers.ValidationError as e:
                raise apis.models.errors.ApiError(f"Invalid relationship - validations errors: {e.errors()}")

            if not rel.transform:
                raise api.abort(400, "Relationship is not Transform", notATransformRelationship=identifier)

            transform = apis.runtime.package_transform.TransformRelationshipToDerivedPackage(rel.transform)
            derived = transform.prepare_derived_package(new_package_name, synthesize.parameterisation)

            with apis.storage.PackagesDownloader(derived) as download:
                transform.synthesize_derived_package(download, derived)
                logging.getLogger("transform").info(f"Synthesized {derived.json(indent=2)}")
                db = utils.database_experiments_open()
                apis.runtime.package.validate_adapt_and_store_experiment_to_database(derived, download, db)
            return {"result": derived.dict()}
        except werkzeug.exceptions.HTTPException:
            raise
        except apis.models.errors.ApiError as e:
            api.abort(400, f"Invalid payload, reason: {str(e)}", problem=str(e))
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while synthesizing parameterised virtual experiment package "
                                       f"from relationship. Traceback: {traceback.format_exc()}")
            api.abort(500, f"Internal error while synthesizing parameterised virtual experiment "
                           f"package from relationship")


@api.route("/<identifier>/", doc=False)
@api.route("/<identifier>")
class Relationship(Resource):
    def get(self, identifier: str):
        try:
            with utils.database_relationships_open() as db:
                ql = db.construct_query(identifier)
                docs = db.query(ql)

            if len(docs) == 0:
                api.abort(404, "Unknown relationship", unknownRelationship=identifier)
            try:
                rel = apis.models.relationships.Relationship.parse_obj(docs[0]).dict()
                problems = []
            except pydantic.error_wrappers.ValidationError as e:
                problems = e.errors()
                rel = docs[0]

            return {
                "entry": rel,
                "problems": problems
            }
        except werkzeug.exceptions.HTTPException:
            raise
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while retrieving relationship {identifier}. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(500, f"Internal error while getting relationship")

    def delete(self, identifier: str):
        try:
            with utils.database_relationships_open() as db:
                ql = db.construct_query(identifier)
                num_docs = db.delete(ql)
            if num_docs == 0:
                api.abort(404, "Unknown relationship", unknownRelationship=identifier)
            return {"deleted": num_docs}
        except werkzeug.exceptions.HTTPException:
            raise
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while deleting relationship {identifier}."
                                       f" Traceback: {traceback.format_exc()}")
            api.abort(500, f"Internal error while deleting relationship")
