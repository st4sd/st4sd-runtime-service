# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

from __future__ import annotations

import traceback

import werkzeug.exceptions
from flask import request, current_app
from flask_restx import Resource
import flask_restx.reqparse

import apis.kernel.library
import apis.models.constants
import apis.models.errors
import apis.storage.actuators.local
import apis.storage.actuators.s3
import utils

api = apis.models.api_library

def generate_client() -> apis.kernel.library.LibraryClient:
    if apis.models.constants.LOCAL_DEPLOYMENT:
        actuator = apis.storage.actuators.local.LocalStorage()
    else:
        db_secrets = utils.secrets_git_open(local_deployment=apis.models.constants.LOCAL_DEPLOYMENT)
        secret = db_secrets.secret_get(apis.models.constants.S3_GRAPH_LIBRARY_SECRET_NAME)
        if secret is None:
            raise  apis.models.errors.DBError(
                f"Secret {apis.models.constants.S3_GRAPH_LIBRARY_SECRET_NAME} containing the S3 credentials for the Library "
                f"does not exist")

        lookup = {
            "S3_BUCKET": "bucket",
            "S3_ENDPOINT": "endpoint_url",
            "S3_ACCESS_KEY_ID": "access_key_id",
            "S3_SECRET_ACCESS_KEY": "secret_access_key",
            "S3_REGION": "region_name"
        }
        args = {
            arg_name: secret.data.get(env_var) for env_var, arg_name in lookup.items()
        }
        actuator = apis.storage.actuators.s3.S3Storage(**args)

    return apis.kernel.library.LibraryClient(
        actuator=actuator, library_path=apis.models.constants.S3_ROOT_GRAPH_LIBRARY
    )


def parser_formatting_dsl() -> flask_restx.reqparse.RequestParser:
    arg_parser = flask_restx.reqparse.RequestParser()

    arg_parser.add_argument(
        "exclude_unset",
        choices=["y", "n"],
        default="n",
        help='Whether to exclude fields that are unset or None from the output.',
        location='args'
    )

    arg_parser.add_argument(
        "exclude_defaults",
        choices=["y", "n"],
        default="n",
        help='Whether to exclude fields that are unset or None from the output.',
        location='args'
    )

    arg_parser.add_argument(
        "exclude_none",
        choices=["y", "n"],
        default="n",
        help='Whether to exclude fields that have a value of `None` from the output.',
        location='args'
    )

    return arg_parser

@api.route("/")
class GraphLibrary(Resource):
    _my_parser = parser_formatting_dsl()

    @api.expect(apis.models.m_library_graph)
    def post(self):
        """Validates a DSL graph and adds it to the library."""
        if not apis.models.constants.LOCAL_DEPLOYMENT and not apis.models.constants.S3_GRAPH_LIBRARY_SECRET_NAME:
            api.abort(400, "Graph Library is disabled - contact the administrator of this ST4SD deployment")
            raise ValueError()  # VV: keep linter happy

        graph = request.get_json()

        try:
            client = generate_client()
            ret = client.add(apis.kernel.library.Entry(graph=graph))
            return ret.entrypoint.entryInstance
        except werkzeug.exceptions.HTTPException:
            raise
        except apis.models.errors.GraphAlreadyExistsError as e:
            api.abort(
                404, "Graph already exists. To update its definition delete the existing graph first",
                graphName=e.graph_name
            )
        except apis.models.errors.InvalidModelError as e:
            current_app.logger.warning(f"Run into {e} while adding a graph "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, e.message, problems=e.problems)
        except apis.models.errors.DBError as e:
            current_app.logger.warning(f"Run into {e} while adding a graph "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, f"Ran into issue when accessing the Secrets database - "
                           f"contact the administrator of this ST4SD deployment", problem=str(e))
        except apis.models.errors.ApiError as e:
            current_app.logger.warning(f"Run into {e} while adding a graph "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, f"Invalid internal experiment payload", problem=str(e))
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while adding a graph "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(500, f"Internal error while adding a graph "
                           f"- contact the administrator of this ST4SD deployment", problem=str(e))

    @api.expect(_my_parser)
    def get(self):
        """Returns the contents of the Graph library.

        The response contains a dictionary with the following format:

        {
            "entries": [
                {
                   "graph": { the graph }
                }
            ],
            "problems": [
               { a dictionary explaining 1 problem }
            ]
        }
        """

        if not apis.models.constants.LOCAL_DEPLOYMENT and not apis.models.constants.S3_GRAPH_LIBRARY_SECRET_NAME:
            api.abort(400, "Graph Library is disabled - contact the administrator of this ST4SD deployment")
            raise ValueError()  # VV: keep linter happy

        try:
            args = self._my_parser.parse_args()

            problems = []
            entries = []

            client = generate_client()

            for name in client.list():
                try:
                    entry = client.get(
                        name,
                        exclude_defaults=args.exclude_defaults == 'y',
                        exclude_none=args.exclude_none == 'y',
                        exclude_unset=args.exclude_unset == 'y',
                    )

                except Exception as e:
                    problems.append({"message": f"Could not get graph {name} due to {e}"})
                else:
                    entries.append({"graph": entry.graph})

            return {
                "entries": entries,
                "problems": problems
            }
        except werkzeug.exceptions.HTTPException:
            raise
        except apis.models.errors.ApiError as e:
            current_app.logger.warning(f"Run into {e} while returning the contents of the graph library "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, f"Invalid internal experiment payload", problem=str(e))
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while returning the contents of the graph library "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(500, f"Internal error while returning the contents of the graph library "
                           f"- contact the administrator of this ST4SD deployment", problem=str(e))

@api.route("/<name>/")
class SingleGrapy(Resource):
    _my_parser = parser_formatting_dsl()

    def get(self, name: str):
        """Returns 1 Graph from the library"""
        if not apis.models.constants.LOCAL_DEPLOYMENT and not apis.models.constants.S3_GRAPH_LIBRARY_SECRET_NAME:
            api.abort(400, "Graph Library is disabled - contact the administrator of this ST4SD deployment")
            raise ValueError()  # VV: keep linter happy

        try:
            args = self._my_parser.parse_args()
            client = generate_client()

            try:
                entry = client.get(
                    name,
                    exclude_defaults=args.exclude_defaults == 'y',
                    exclude_none=args.exclude_none == 'y',
                    exclude_unset=args.exclude_unset == 'y',
                )

            except apis.models.errors.GraphDoesNotExistError:
                api.abort(400, "Graph does not exist")
                raise  # VV: keeps linter happy
            return {
                "entry": entry.graph
            }
        except werkzeug.exceptions.HTTPException:
            raise
        except apis.models.errors.ApiError as e:
            current_app.logger.warning(f"Run into {e} while deleting a graph from the library "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, f"Invalid internal experiment payload", problem=str(e))
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while deleting a graph from the library "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(500, f"Internal error while returning the contents of the graph library "
                           f"- contact the administrator of this ST4SD deployment", problem=str(e))

    def delete(self, name: str):
        """Removes 1 Graph from the library"""
        if not apis.models.constants.LOCAL_DEPLOYMENT and not apis.models.constants.S3_GRAPH_LIBRARY_SECRET_NAME:
            api.abort(400, "Graph Library is disabled - contact the administrator of this ST4SD deployment")
            raise ValueError()  # VV: keep linter happy

        try:
            client = generate_client()

            try:
                client.delete(name)
            except apis.models.errors.GraphDoesNotExistError as e:
                api.abort(404, "Graph does not exist", graphName=e.graph_name)

            return {"message": "Success"}
        except werkzeug.exceptions.HTTPException:
            raise
        except apis.models.errors.ApiError as e:
            current_app.logger.warning(f"Run into {e} while deleting a graph from the library "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, f"Invalid internal experiment payload", problem=str(e))
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while deleting a graph from the library "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(500, f"Internal error while returning the contents of the graph library "
                           f"- contact the administrator of this ST4SD deployment", problem=str(e))
