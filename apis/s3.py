# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis
#   Alessandro Pomponio

import os

import boto3

import apis.models.common


def download_all(credentials: apis.models.common.OptionFromS3Values, s3_key_prefix: str, output_dir: str):
    s3 = boto3.resource('s3',
                        aws_access_key_id=credentials.accessKeyID,
                        aws_secret_access_key=credentials.secretAccessKey,
                        endpoint_url=credentials.endpoint,
                        region_name=credentials.region)
    bucket = s3.Bucket(credentials.bucket)

    for obj in bucket.objects.filter(Prefix=s3_key_prefix):
        target = os.path.join(output_dir, obj.key)
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        if obj.key[-1] == '/':
            continue
        bucket.download_file(obj.key, target)
