# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

from __future__ import annotations

import os
import logging
import pprint
import traceback
from typing import Any
from typing import Dict

import experiment.model.graph
import pydantic.error_wrappers
import werkzeug.exceptions
from flask import request, current_app
from flask_restx import Resource, reqparse

import apis.kernel.flask_utils
import apis.kernel.relationships
import apis.models.constants
import apis.models.errors
import apis.models.relationships
import apis.models.virtual_experiment
import apis.runtime.package
import apis.runtime.package_derived
import apis.runtime.package_transform
import apis.storage
import utils

api = apis.models.api_relationships

parser_formatting_relationship_preview = apis.kernel.flask_utils.parser_formatting_relationship_preview


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

        with utils.database_relationships_open(apis.models.constants.LOCAL_DEPLOYMENT) as db:
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

    @api.expect(apis.models.m_relationship)
    def post(self):
        doc = request.get_json()

        try:
            rel = apis.kernel.relationships.api_push_relationship(
                rel=doc,
                db_relationships=utils.database_relationships_open(apis.models.constants.LOCAL_DEPLOYMENT),
                db_experiments=utils.database_experiments_open(apis.models.constants.LOCAL_DEPLOYMENT),
                packages=apis.storage.PackagesDownloader(ve=None, db_secrets=utils.secrets_git_open(
                    local_deployment=apis.models.constants.LOCAL_DEPLOYMENT)),
            )
            return {
                "entry": rel.dict()
            }
        except werkzeug.exceptions.HTTPException:
            raise
        except apis.models.errors.InvalidModelError as e:
            current_app.logger.warning(f"Run into {e} while registering relationship. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, e.message, problems=e.problems)
        except apis.models.errors.ApiError as e:
            current_app.logger.warning(f"Run into {e} while registering relationship. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, f"Invalid relationship", problem=str(e))
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while registering relationship. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(500, f"Internal error while registering relationship")


@api.route("/<identifier>/preview/synthesize/dsl", doc=False)
@api.route("/<identifier>/preview/synthesize/dsl/")
class TransformDSLPreview(Resource):
    _my_parser = parser_formatting_relationship_preview()

    @api.expect(_my_parser)
    def get(self, identifier: str):
        """Previews the DSL and parameterised virtual experiment package of a would-be synthesized experiment.

        It returns a Dictionary with the format ::

            {
                "dsl": { the dictionary representing the DSL of the computational graph },
                "experiment": { the dictionary representing the parameterised virtual experiment package that
                                would have been created in the registry },
                "problems": [ a list of potential issues/warnings ]
            }
        """
        try:
            args = self._my_parser.parse_args()

            db_relationships = utils.database_relationships_open(apis.models.constants.LOCAL_DEPLOYMENT)
            db_experiments = utils.database_experiments_open(apis.models.constants.LOCAL_DEPLOYMENT)

            ret = apis.kernel.relationships.api_preview_synthesize_dsl(
                identifier=identifier,
                packages=apis.storage.PackagesDownloader(ve=None, db_secrets=utils.secrets_git_open(
                    local_deployment=apis.models.constants.LOCAL_DEPLOYMENT)),
                db_relationships=db_relationships,
                db_experiments=db_experiments,
                dsl_version=args.dslVersion
            )

            return {
                "dsl": ret.dsl,
                "experiment": ret.package.dict(),
                "problems": []
            }
        except werkzeug.exceptions.HTTPException:
            raise
        except apis.models.errors.RelationshipNotFoundError as e:
            current_app.logger.warning(f"Run into {e} while previewing synthesis from {identifier}. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(404, e.message, relationshipNotFound=e.identifier)
        except apis.models.errors.ApiError as e:
            current_app.logger.warning(f"Run into {e} while previewing synthesis from {identifier}. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, f"Invalid payload, reason: {str(e)}", problem=str(e))
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while previewing the synthesized parameterised "
                                       f"virtual experiment package from relationship. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(500, "Internal error while previewing the synthesized parameterised virtual experiment "
                           "package from relationship")


@api.route("/<identifier>/synthesize/<new_package_name>", doc=False)
@api.route("/<identifier>/synthesize/<new_package_name>/")
class TransformSynthesize(Resource):
    @api.expect(apis.models.m_payload_synthesize)
    def post(self, identifier: str, new_package_name: str):
        """Synthesizes a new experiment and stores it in the registry using the <identifier> relationship"""
        try:
            doc = request.get_json()
            synthesize = apis.models.relationships.PayloadSynthesize.parse_obj(doc)
        except pydantic.error_wrappers.ValidationError as e:
            api.abort(400, f"Invalid synthesize payload, problems are {e.json()}", problems=e.errors())
            raise e  # VV: Keep linter happy

        # VV: TODO FIX ME
        try:
            ret = apis.kernel.relationships.api_synthesize_from_transformation(
                identifier=identifier,
                new_package_name=new_package_name,
                packages=apis.storage.PackagesDownloader(ve=None, db_secrets=utils.secrets_git_open(
                    local_deployment=apis.models.constants.LOCAL_DEPLOYMENT)),
                db_relationships=utils.database_relationships_open(apis.models.constants.LOCAL_DEPLOYMENT),
                db_experiments=utils.database_experiments_open(apis.models.constants.LOCAL_DEPLOYMENT),
                synthesize=synthesize,
                path_multipackage=apis.models.constants.ROOT_STORE_DERIVED_PACKAGES,
            )

            return {"result": ret.package.dict(), "problems": []}
        except werkzeug.exceptions.HTTPException:
            raise
        except apis.models.errors.RelationshipNotFoundError as e:
            current_app.logger.warning(f"Run into {e} while retrieving synthesizing {identifier}. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(404, e.message, relationshipNotFound=e.identifier)
        except apis.models.errors.ParameterisedPackageNotFoundError as e:
            current_app.logger.warning(f"Run into {e} while retrieving synthesizing {identifier}. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(404, e.message, parameterisedPackageNotFound=e.identifier)
        except apis.models.errors.InvalidModelError as e:
            current_app.logger.warning(f"Run into {e} while retrieving synthesizing {identifier}. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, e.message, problems=e.problems)
        except apis.models.errors.ApiError as e:
            current_app.logger.warning(f"Run into {e} while retrieving synthesizing {identifier}. "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, e.message)
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while synthesizing parameterised virtual experiment package "
                                       f"from relationship. Traceback: {traceback.format_exc()}")
            api.abort(500, f"Internal error while synthesizing parameterised virtual experiment "
                           f"package from relationship")


@api.route("/<identifier>", doc=False)
@api.route("/<identifier>/")
class Relationship(Resource):
    def get(self, identifier: str):
        try:
            with utils.database_relationships_open(apis.models.constants.LOCAL_DEPLOYMENT) as db:
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
            with utils.database_relationships_open(apis.models.constants.LOCAL_DEPLOYMENT) as db:
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
