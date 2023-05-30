# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


import traceback
from typing import Dict

import werkzeug.exceptions
from flask import current_app
from flask_restx import Resource

import apis.models
import apis.models.constants
import utils

api = apis.models.api_url_map
# VV: FIXME these should change to st4sd-runtime-service, st4sd-datastore-mongodb-rest, st4sd-datastore-registry
knownServices = ["consumable-computing", "cdb-rest", "cdb-gateway-registry"]


def get_all_routes():
    # type: () -> Dict[str, str]
    """Returns a dictionary containing the URL of each of the keys:
    - consumable-computing
    - cdb-rest
    - cdb-gateway-registry

    FIXME: The above names are deprecated and should change to:
    - st4sd-runtime-service
    - st4sd-datastore-mongodb-proxy
    - st4sd-datastore-registry

    Prioritizes global Datastore routes (if available).
    """
    data = utils.get_config_map_data(utils.ConfigMapWithParameters)

    def to_url(route):
        # type: (str) -> str
        route = route.rstrip('/')
        return 'https://%s' % route

    route_runtime_service = data['hostRuntimeService']

    # VV: This may be a ST4SD deployment with no datastore-nexus of its own
    if 'hostDatastoreRestGlobal' in data:
        route_datastore_mongodb_rest = data['hostDatastoreRestGlobal']
    else:
        route_datastore_mongodb_rest = data['hostDatastoreRest']

    if 'hostDatastoreRegistryGlobal' in data:
        route_datastore_registry = data['hostDatastoreRegistryGlobal']
    else:
        route_datastore_registry = data['hostDatastoreRegistry']

    return {
        'consumable-computing': to_url(route_runtime_service),
        'cdb-rest': to_url(route_datastore_mongodb_rest),
        'cdb-gateway-registry': to_url(route_datastore_registry)
    }


@api.route('/')
@api.response(500, 'Internal error while generating url-map')
@api.response(401, 'Unauthorised')
class UrlMapAll(Resource):
    @api.response(200, "Success")
    def get(self):
        """Returns a dictionary containing the URL of each of the keys:
        - consumable-computing
        - cdb-rest
        - cdb-gateway-registry

        FIXME: The above names are deprecated and should change to:
        - st4sd-runtime-service
        - st4sd-datastore-mongodb-proxy
        - st4sd-datastore-registry

        Prioritizes global Datastore routes (if available).
        """
        if apis.models.constants.LOCAL_DEPLOYMENT:
            api.abort(400, "The API is running in LOCAL_DEPLOYMENT mode", localDeployment=True)

        try:
            return get_all_routes()
        except Exception as e:
            current_app.logger.warning("Traceback: %s\nException: %s for %s" % (traceback.format_exc(), e, id))
            api.abort(500, "Exception while building url-map")


@api.route('/<service>/', doc=False)
@api.route('/<service>')
@api.param('service', "Service identifier (i.e %s)" % (', '.join(knownServices)))
@api.response(500, 'Internal error while generating url-map')
@api.response(401, 'Unauthorised')
@api.response(404, 'Unknown service')
class UrlService(Resource):
    @api.response(200, "Success")
    def get(self, service):
        """Returns the URL of one of the services:
        - consumable-computing
        - cdb-rest
        - cdb-gateway-registry

        FIXME: The above names are deprecated and should change to:
        - st4sd-runtime-service
        - st4sd-datastore-mongodb-proxy
        - st4sd-datastore-registry

        Prioritizes global Datastore routes (if available).
        """
        if apis.models.constants.LOCAL_DEPLOYMENT:
            api.abort(400, "The API is running in LOCAL_DEPLOYMENT mode", localDeployment=True)

        try:
            all_urs = get_all_routes()
            try:
                return all_urs[service]
            except KeyError:
                api.abort(404, "Unknown service %s, valid options %s" % (service, ', '.join(knownServices)))
        except werkzeug.exceptions.HTTPException:
            raise
        except Exception as e:
            current_app.logger.warning("Traceback: %s\nException: %s for %s" % (
                traceback.format_exc(), e, id))
            api.abort(500, "Exception while building url-map")