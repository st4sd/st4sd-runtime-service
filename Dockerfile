
# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis
#   Yiannis Gkoufas

ARG base_image=quay.io/st4sd/official-base/st4sd-runtime-core:latest
FROM $base_image

ENV PYTHONUNBUFFERED 0

COPY requirements.txt /requirements.txt
RUN apt-get update && \
    export DEBIAN_FRONTEND=noninteractive && \
    apt-get install -y --no-install-recommends git && \
    pip install --upgrade wheel pip setuptools && \
    pip install -r /requirements.txt && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /gunicorn && \
    chgrp -R 0 /gunicorn && \
    chmod -R g+rwX /gunicorn && \
    mkdir /scripts && \
    chmod 666 /scripts

ARG BASE_PATH
ARG INPUT_DATA
ENV BASE_PATH=$BASE_PATH
ENV INPUT_DATA=$INPUT_DATA
ENV PYTHONUNBUFFERED=0
ENV GUNICORN_PID_PATH="/gunicorn/webserver.pid"


COPY *.py ./
COPY apis ./apis
RUN pip install .

EXPOSE 4000

CMD ["gunicorn", "--bind", "0.0.0.0:4000", "app:app", "-p", "/gunicorn/webserver.pid", \
     "--timeout", "120", "--threads", "2", "--keep-alive", "30"]
