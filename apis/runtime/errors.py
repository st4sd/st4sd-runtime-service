# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import apis.models.errors

class RuntimeError(apis.models.errors.ApiError):
    pass


class CannotDownloadBasePackageError(RuntimeError):
    pass


class CannotDownloadGitError(CannotDownloadBasePackageError):
    def __init__(self, message: str):
        super(CannotDownloadGitError, self).__init__(message)


class CannotCreateOAuthSecretError(apis.models.errors.ApiError):
    def __init__(self, base_name: str):
        self.base_name = base_name

        super(CannotCreateOAuthSecretError, self).__init__(
            f"Unable to create Kubernetes secret with oauth-token for base package {base_name}")
