# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis
#   Alessandro Pomponio

from __future__ import annotations

import copy
import logging
from typing import Dict
from typing import List
from typing import Optional

import experiment.model.frontends.flowir
import experiment.model.graph
import experiment.model.storage

import apis.db.secrets
import apis.k8s
import apis.models.common
import apis.models.errors
import apis.models.virtual_experiment
import apis.runtime.errors


class PackageMetadataCollection:
    def __init__(
            self,
            concrete_and_data: Dict[str, apis.models.virtual_experiment.StorageMetadata] | None = None,
            ve: apis.models.virtual_experiment.ParameterisedPackage | None = None,
            db_secrets: apis.db.secrets.DatabaseSecrets | None = None,
    ):
        self.db_secrets = db_secrets
        self._log = logging.getLogger('Downloader')
        self._metadata: Dict[str, apis.models.virtual_experiment.StorageMetadata] = concrete_and_data or {}
        self._ve = ve
        self._entered = 0
        self._times_entered_total = 0

    def get_common_platforms(self) -> List[str]:
        ret: Optional[List[str]] = None

        for name in sorted(self._metadata):
            concrete = self.get_concrete_of_package(name)
            platforms = concrete.platforms
            if ret is None:
                ret = list(platforms)
            else:
                ret = list(set(ret).intersection(platforms))

        return ret or []

    def update_parameterised_package(self, ve: apis.models.virtual_experiment.ParameterisedPackage | None):

        if self._times_entered_total == 0:
            self._ve = ve
        elif self._ve != ve:
            old_ve = self._ve.metadata.package.name if self._ve is not None else "*none*"
            new_ve = ve.metadata.package.name if ve is not None else "*none*"
            self._ve = ve
            self._log.warning(f"Changing ve from {old_ve} to {new_ve}")

    def get_parameterised_package(self) -> apis.models.virtual_experiment.ParameterisedPackage | None:
        return self._ve

    def get_all_package_metadata(self) -> Dict[str, apis.models.virtual_experiment.StorageMetadata]:
        return self._metadata

    def get_root_directory_containing_package(self, name: str) -> str:
        return self._metadata[name].rootDirectory

    def get_location_of_package(self, name: str) -> str:
        # VV: Ensure package exists
        return self._metadata[name].location

    def get_manifest_data_of_package(self, name: str) -> experiment.model.frontends.flowir.DictManifest:
        return copy.deepcopy(self._metadata[name].manifestData)

    def get_concrete_of_package(self, name: str) -> experiment.model.frontends.flowir.FlowIRConcrete:
        return self._metadata[name].concrete.copy()

    def get_datafiles_of_package(self, name: str) -> List[str]:
        return list(self._metadata[name].data)

    def get_metadata(self, name: str) -> apis.models.virtual_experiment.StorageMetadata:
        return self._metadata[name]

    def upsert_metadata(self, name: str, metadata: apis.models.virtual_experiment.StorageMetadata):
        self._metadata[name] = metadata

    def __enter__(self):
        self._entered += 1
        self._times_entered_total += 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._entered -= 1
        pass