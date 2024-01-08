# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

from __future__ import annotations

import pathlib
import traceback

import pydantic.error_wrappers
import werkzeug.exceptions
from flask import request, current_app
from flask_restx import Resource

import apis.kernel.internal_experiments
import apis.models.constants
import apis.models.errors
import apis.models.virtual_experiment

import utils

api = apis.models.api_internal_experiments


@api.route("/")
class InternalExperiments(Resource):
    @api.expect(apis.models.m_internal_experiment)
    def post(self):
        if not apis.models.constants.S3_INTERNAL_EXPERIMENTS_SECRET_NAME:
            api.abort(400, "Internal Storage is disabled - contact the administrator of this ST4SD deployment")
            raise ValueError() # VV: keep linter happy

        doc = request.get_json()

        try:
            db_secrets = utils.database_secrets_open(local_deployment=apis.models.constants.LOCAL_DEPLOYMENT)
            db_experiments = utils.database_experiments_open(apis.models.constants.LOCAL_DEPLOYMENT)

            try:
                dsl = doc["workflow"]["dsl"]
                pvep = doc["pvep"]
                pvep = apis.models.virtual_experiment.ParameterisedPackage(**pvep)
            except KeyError as e:
                raise apis.models.errors.InvalidModelError(f"Invalid payload to endpoint", problems=[{
                    "message": f"Missing field {e}"
                }])
            except pydantic.ValidationError as e:
                raise apis.models.errors.InvalidModelError.from_pydantic(
                    msg=f"Invalid Parameterised Virtual Experiment Package",
                    exc=e
                )

            pvep.base.packages = []
            problems = []
            if not pvep.metadata.package.name:
                api.abort(400, 'Missing "pvep.metadata.package.name"', problems=[
                    {"message": 'Missing "pvep.metadata.package.name"'}
                ])
                raise ValueError() # VV: keeping linter happy

            pvep = apis.kernel.internal_experiments.upsert_internal_experiment(
                dsl2_definition=dsl,
                pvep=pvep,
                db_secrets=db_secrets,
                db_experiments=db_experiments,
                package_source=apis.models.constants.S3_INTERNAL_EXPERIMENTS_SECRET_NAME,
                dest_path=pathlib.Path(apis.models.constants.S3_ROOT_INTERNAL_EXPERIMENTS)
            )

            return {
                "result": pvep.dict(),
                "problems": problems,
            }
        except werkzeug.exceptions.HTTPException:
            raise
        except apis.models.errors.InvalidModelError as e:
            current_app.logger.warning(f"Run into {e} while registering internal-experiment "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, e.message, problems=e.problems)
        except apis.models.errors.DBError as e:
            current_app.logger.warning(f"Run into {e} while registering internal-experiment "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, f"Ran into issue while accessing the storage location of Internal Experiments - "
                           f"contact the administrator of this ST4SD deployment", problems=[
                    {"message": str(e)}
            ])
        except apis.models.errors.ApiError as e:
            current_app.logger.warning(f"Run into {e} while registering internal-experiment "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(400, f"Invalid internal experiment payload", problems=[
                    {"message": str(e)}
            ])
        except Exception as e:
            current_app.logger.warning(f"Run into {e} while registering internal-experiment "
                                       f"Traceback: {traceback.format_exc()}")
            api.abort(500, f"Internal error while registering internal-experiment "
                           f"- contact the administrator of this ST4SD deployment", problems=[
                    {"message": str(e)}
            ])
