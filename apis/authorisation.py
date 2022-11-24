# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


import traceback

from flask import current_app, request
from flask_restx import Resource

import apis.models

api = apis.models.api_authorisation


@api.route('/token/', doc=False)
@api.route('/token')
@api.response(500, 'Internal error while retrieving token')
@api.response(401, 'Unauthorised')
class AuthorisationToken(Resource):
    @api.response(200, "Success")
    def get(self):
        """Returns a token that can be used to authenticate to the service
        """
        try:
            # VV: Return the contents of the oauth-proxy cookie
            try:
                return request.cookies['oauth-proxy']
            except KeyError:
                api.abort(401, "Unauthorised")
        except Exception as e:
            current_app.logger.warning("Traceback: %s\nException: %s for %s" % (traceback.format_exc(), e, id))
            api.abort(500, "Exception while retrieving token")
