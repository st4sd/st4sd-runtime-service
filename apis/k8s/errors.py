# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


class KubernetesObjectNotFound(Exception):
    def __init__(self, k8s_kind, k8s_name):
        # type: (str, str) -> None
        self.kind = k8s_kind
        self.name = k8s_name
        self.message = 'Kubernetes object %s/%s does not exist' % (k8s_kind, k8s_name)

        super(KubernetesObjectNotFound, self).__init__()

    def __str__(self):
        return self.message


class DatashimNotInstalledError(Exception):
    def __init__(self):
        self.message = 'Datashim is not installed on this cluster. Please ask the cluster administrator to install' \
                       ' https://github.com/datashim-io/datashim'

        super(DatashimNotInstalledError, self).__init__()

    def __str__(self):
        return self.message
