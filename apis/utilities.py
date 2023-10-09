# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

from __future__ import annotations

import traceback

import werkzeug.exceptions
from flask import request, current_app
from flask_restx import Resource

import apis.kernel.internal_experiments
import apis.models.errors


api = apis.models.api_utilities


@api.route("/dsl/")
class UtilityDSL(Resource):
    @api.expect(apis.models.m_utilities_dsl)
    def post(self):
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
