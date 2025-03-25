# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

from __future__ import annotations

import traceback

import pydantic
import werkzeug.exceptions
from flask import request, current_app
from flask_restx import Resource

import apis.kernel.internal_experiments
import apis.models.errors
import apis.models.virtual_experiment

api = apis.models.api_utilities


@api.route("/dsl/")
class UtilityDSL(Resource):
    @api.expect(apis.models.m_utilities_dsl)
    def post(self):
        """Validates a DSL workflow and its associated Parameterised Virtual Experiment Package (PVEP) definition"""
        doc = request.get_json()

        try:
            apis.kernel.internal_experiments.validate_dsl(doc)

            return {
                "problems": []
            }
        except werkzeug.exceptions.HTTPException:
            raise
        except apis.models.errors.InvalidModelError as e:
            current_app.logger.warning(f"Run into {e} while validating DSL "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, e.message, problems=e.problems)
        except apis.models.errors.ApiError as e:
            current_app.logger.warning(f"Run into {e} while validating DSL "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, f"Invalid experiment payload", problems=[{"problem": str(e)}])
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while validating DSL "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(
                500,
                f"Internal error while validating DSL - contact the administrator of this ST4SD deployment",
                problems=[{
                    "problem": str(e),
                    "message": f"Internal error while validating DSL "
                           f"- contact the administrator of this ST4SD deployment"
                }])


@api.route("/pvep/")
class UtilityPVEP(Resource):
    @api.expect(apis.models.m_utilities_pvep)
    def post(self):
        """Generates the default Parameterised Virtual Experiment Package (PVEP) for a DSL workflow starting from an \
optional PVEP template
        """
        doc = request.get_json()

        try:
            try:
                dsl = doc["dsl"]
            except Exception:
                raise apis.models.errors.InvalidModelError("Invalid payload", problems=[
                    {
                        "problem": "Expected a dictionary with key \"dsl\" that contains the DSL 2.0 definition "
                                   "of a workflow"
                    }
                ])
            template = None

            if "pvep" in doc:
                try:
                    template = apis.models.virtual_experiment.ParameterisedPackage.validate(doc['pvep'])
                except pydantic.ValidationError as e:
                    raise apis.models.errors.InvalidModelError.from_pydantic("Invalid payload field pvep", e)

            pvep_and_changes = apis.kernel.internal_experiments.generate_pvep_for_dsl(
                dsl2_definition=dsl, template=template)

            return {
                "pvep": pvep_and_changes.pvep.model_dump(by_alias=True, exclude_none=True),
                "changes": pvep_and_changes.changes,
                "problems": []
            }
        except werkzeug.exceptions.HTTPException:
            raise
        except apis.models.errors.InvalidModelError as e:
            current_app.logger.warning(f"Run into {e} while generating the default PVEP for a DSL "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, e.message, problems=e.problems)
        except apis.models.errors.ApiError as e:
            current_app.logger.warning(f"Run into {e} while generating the default PVEP for a DSL "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, f"Invalid experiment payload", problems=[{"problem": str(e)}])
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while validating DSL "
                                       f"Traceback: {traceback.format_exc()}")
            msg = ("Internal error while while generating the default PVEP for a DSL - contact the "
                   "administrator of this ST4SD deployment")
            api.abort(500, msg, problems=[{"problem": str(e), "message": msg}])
