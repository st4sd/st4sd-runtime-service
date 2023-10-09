# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis
#   Yiannis Gkoufas

import logging
import os
import signal
import sys

FLASK_URL_PREFIX = os.environ.get("FLASK_URL_PREFIX", "")

import threading
import utils
from werkzeug.middleware.proxy_fix import ProxyFix
from flask import Flask, request, Response, Blueprint
from flask_restx import Api
import flask_restx.apidoc

if FLASK_URL_PREFIX:
    old_static_url_path = flask_restx.apidoc.apidoc.static_url_path

    print(f"Prefixing SwaggerUI static url path ({old_static_url_path}) with {FLASK_URL_PREFIX}")
    flask_restx.apidoc.apidoc.static_url_path = f"{FLASK_URL_PREFIX}{old_static_url_path}"

from middlelayer import PrefixMiddleware
from flask_cors import CORS
from datetime import datetime as dt

import apis.models.constants
import kubernetes.config

# VV: Import modules to trigger the generation of API-Endpoints
import apis.experiments
import apis.instances
import apis.datasets
import apis.image_pull_secrets
import apis.authorisation
import apis.relationships
import apis.query
import apis.url_map
import apis.internal_experiments



app = Flask(__name__)
app.wsgi_app = PrefixMiddleware(app.wsgi_app)

app.config["LOG_TYPE"] = os.environ.get("LOG_TYPE", "watched")
app.config["LOG_LEVEL"] = os.environ.get("LOG_LEVEL", "INFO")

# VV: Disables the "did you mean .... endpoints" when a route returns 404
app.config['ERROR_404_HELP'] = False

blueprint = Blueprint("cc", __name__)

api = Api(
    app=blueprint,
    title='Runtime Service of the Simulation Toolkit for Scientific Discovery (ST4SD)',
    contact_url="https://github.ibm.com/st4sd",
    version='1.0',
    description='Launch/Monitor/Stop experiments',
    # doc=FLASK_URL_PREFIX,
    # All API metadata
)

api.add_namespace(apis.authorisation.api)
api.add_namespace(apis.experiments.api)
api.add_namespace(apis.internal_experiments.api)
api.add_namespace(apis.instances.api)
api.add_namespace(apis.image_pull_secrets.api)
api.add_namespace(apis.query.api)
api.add_namespace(apis.relationships.api)
api.add_namespace(apis.url_map.api)
api.add_namespace(apis.datasets.api)

# File Logging Setup
app.config['LOG_DIR'] = os.environ.get("LOG_DIR", "/tmp/logs")

if os.path.isdir(app.config['LOG_DIR']) is False:
    os.mkdir(app.config['LOG_DIR'])

app.config['APP_LOG_NAME'] = os.environ.get("APP_LOG_NAME", "app.log")
app.config['WWW_LOG_NAME'] = os.environ.get("WWW_LOG_NAME", "www.log")
CORS(app)

app.register_blueprint(blueprint, url_prefix=FLASK_URL_PREFIX)
# api.init_app(app)

app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

FORMAT = '%(levelname)-9s %(name)-30s: %(funcName)-20s %(asctime)-15s: %(message)s'
logging.basicConfig(format=FORMAT)

logging.getLogger().setLevel(logging.INFO)
app.logger.setLevel(logging.INFO)


def kill_web_server(exit_code):
    app.logger.critical("Terminating with sys.exit(%d)" % exit_code)
    with open(os.environ.get('GUNICORN_PID_PATH', "/gunicorn/webserver.pid"), 'r') as f:
        pid = int(f.read().rstrip())
    os.kill(pid, signal.Signals.SIGINT)
    sys.exit(exit_code)


def initialize():
    try:
        app.logger.info("Loading Kubernetes config ...")

        if apis.models.constants.LOCAL_DEPLOYMENT is False:
            kubernetes.config.load_incluster_config()

        app.logger.info("Validating config.json ...")

        utils.setup_config(local_deployment=apis.models.constants.LOCAL_DEPLOYMENT, validate=True)
    except BaseException as e:
        app.logger.critical("Unable to validate configuration %s - will terminate" % e)
        kill_web_server(1)
    else:
        app.logger.info("config.json is valid")


# VV: Spin up a thread to initialize the server 5 seconds rom now
# (gunicorn should have processed this python file by then)
init_thread = threading.Timer(5.0, function=initialize)
init_thread.setDaemon(True)
init_thread.start()


@app.after_request
def after_request(response):
    # type: (Response) -> Response
    """ Logging after every request. """
    logger = logging.getLogger("app.access")
    response.direct_passthrough = False

    # VV: Do not print the request data when there's sensitive information OR the data are contents of swagger files
    data = request.data
    path = request.path or ''
    resp_data = response.get_data()
    if 'image-pull-secrets' in path:
        data = '**hidden**'
    elif 'swaggerui' in path:
        data = '**omitted**'
        resp_data = '**omitted**'

    # VV: Special status codes:
    # 200: Success
    # 304: Not Modified -> i.e. no need for the server to reply with data (can be status code for css file that has
    #      been transmitted already in the past, etc)
    # Anything else: For the time being, let's treat everything else as an error.
    if response.status_code == 200:
        logger.info(
            "[%s] %s %s %s",
            dt.utcnow().strftime("%d/%b/%Y:%H:%M:%S.%f")[:-3],
            request.method,
            request.path,
            data
        )
    elif response.status_code not in [304]:
        logger.error(
            "[%s] %s %s %s %s",
            dt.utcnow().strftime("%d/%b/%Y:%H:%M:%S.%f")[:-3],
            request.method,
            request.path,
            request.data if 'image-pull-secrets' not in (request.path or '') else '**hidden**',
            resp_data
        )
    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4000, debug=True)
