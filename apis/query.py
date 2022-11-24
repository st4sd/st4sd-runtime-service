# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

from __future__ import annotations

from flask import request
from flask_restx import Resource

import apis.datasets
import apis.db
import apis.image_pull_secrets
import apis.instances
import apis.k8s
import apis.kernel.experiments
import apis.kernel.flask_utils
import apis.models
import apis.models.common
import apis.models.constants
import apis.models.constants
import apis.models.errors
import apis.models.query_experiment
import apis.models.virtual_experiment
import apis.runtime.package
import apis.runtime.package_derived
import apis.storage
import apis.url_map
import apis.url_map

api = apis.models.api_query


@api.route('/experiments', doc=False)
@api.route('/experiments/')
class QueryExperiments(Resource):
    _parser = apis.kernel.flask_utils.parser_formatting_parameterised_package()

    # @api.expect(_parser)
    @api.expect(apis.models.mQueryExperiment)
    def post(self):
        doc = request.get_json()

        try:
            return apis.kernel.experiments.api_list_queries(
                doc,
                format_options=apis.kernel.flask_utils.parser_to_format_options(self._parser))
        except apis.models.errors.ApiError as e:
            api.abort(400, str(e))
