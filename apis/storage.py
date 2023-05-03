# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis
#   Alessandro Pomponio

from __future__ import annotations

import copy
import datetime
import logging
import os
import subprocess
import tempfile
from typing import Dict
from typing import List
from typing import Optional
from typing import Any

import experiment.model.frontends.flowir
import experiment.model.graph
import experiment.model.storage
import stream_zip

import apis.k8s
import apis.models.common
import apis.models.errors
import apis.models.virtual_experiment
import apis.runtime.errors
import apis.s3


class PackageMetadataCollection:
    def __init__(
            self,
            concrete_and_data: Dict[str, apis.models.virtual_experiment.StorageMetadata] | None = None
    ):
        self._log = logging.getLogger('Downloader')
        self._concrete_and_data: Dict[str, apis.models.virtual_experiment.StorageMetadata] = concrete_and_data or {}

    def get_common_platforms(self) -> List[str]:
        ret: Optional[List[str]] = None

        for name in sorted(self._concrete_and_data):
            concrete = self.get_concrete_of_package(name)
            platforms = concrete.platforms
            if ret is None:
                ret = list(platforms)
            else:
                ret = list(set(ret).intersection(platforms))

        return ret or []

    def get_all_package_metadata(self) -> Dict[str, apis.models.virtual_experiment.StorageMetadata]:
        return self._concrete_and_data

    def get_root_directory_containing_package(self, name: str) -> str:
        return self._concrete_and_data[name].rootDirectory

    def get_location_of_package(self, name: str) -> str:
        # VV: Ensure package exists
        return self._concrete_and_data[name].location

    def get_manifest_data_of_package(self, name: str) -> experiment.model.frontends.flowir.DictManifest:
        return copy.deepcopy(self._concrete_and_data[name].manifestData)

    def get_concrete_of_package(self, name: str) -> experiment.model.frontends.flowir.FlowIRConcrete:
        return self._concrete_and_data[name].concrete.copy()

    def get_datafiles_of_package(self, name: str) -> List[str]:
        return list(self._concrete_and_data[name].data)

    def get_metadata(self, name: str) -> apis.models.virtual_experiment.StorageMetadata:
        return self._concrete_and_data[name]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class PackagesDownloader(PackageMetadataCollection):
    def __init__(
            self,
            ve: apis.models.virtual_experiment.ParameterisedPackage,
            prefix_dir: str | None = None,
            already_downloaded_to: str | None = None
    ):
        super(PackagesDownloader, self).__init__()
        self._prefix_dir = prefix_dir or "/tmp"
        self._ve = ve
        self._temp_dir: tempfile.TemporaryDirectory | None = None
        self._already_downloaded_to = already_downloaded_to
        self._do_cleanup = False

    def _download_package_git(self, package: apis.models.virtual_experiment.BasePackage):
        security = package.source.git.security
        location = package.source.git.location

        download_path = os.path.join(self._root_directory(), package.name)

        self._log.info(f"Downloading GIT {package.name} from {package.source.git.location.url}")

        def download_with_token_and_extract_commit_id(oauth_token: str | None):
            url = location.url or ''

            if url.startswith('https://') is False:
                raise apis.models.errors.ApiError("Currently only support cloning https://-style git urls")

            if oauth_token is not None:
                url = f"{url[:8]}{oauth_token}@{url[8:]}"

            def try_git_clone(git_clone: List[str]):
                x = subprocess.run(git_clone, check=False, capture_output=True)

                if x.returncode != 0:
                    stderr = x.stderr.decode('utf-8') if isinstance(x.stderr, bytes) else x.stderr
                    stdout = x.stdout.decode('utf-8') if isinstance(x.stdout, bytes) else x.stdout
                    message = f"git-stderr: {stderr}\ngit-stdout:  {stdout}"
                    self._log.warning(f"Unable to {git_clone} with exit error {x.returncode}, stderr and stdout follow:"
                                      f"{message}")
                    message = message.replace(str(oauth_token), "${OAUTH_TOKEN_HIDDEN}")

                    raise apis.runtime.errors.CannotDownloadGitError(f"Unable to clone - {message}")

            if location.branch or location.tag:
                branch = location.branch or location.tag
                clone_commands = ["git", "clone", "--depth", "1", url, "-b", branch, download_path]
                try_git_clone(clone_commands)
                process = subprocess.run(f"cd {download_path} && git rev-parse HEAD",
                                         check=True, shell=True, capture_output=True)
                # VV: We expect a byte encoded string containing the commit id followed by a `\n` character
                commit_id = process.stdout.decode('utf-8').rstrip()
            elif location.commit:
                # VV: This works if commit is full (i.e not an abbreviation))
                clone_commands = [
                    "sh",
                    "-c",
                    f"mkdir -p {download_path} && "
                    f"cd {download_path} && "
                    f"git init . && "
                    f"git remote add origin {url} &&"
                    f"git fetch --depth 1 origin {location.commit} &&"
                    f"git checkout FETCH_HEAD"]
                subprocess.run(clone_commands, check=True)
                commit_id = location.commit
            else:
                clone_commands = ["git", "clone", "--depth", "1", url, download_path]
                subprocess.run(clone_commands, check=True)
                process = subprocess.run(f"cd {download_path} && git rev-parse HEAD",
                                         check=True, shell=True, capture_output=True)
                commit_id = process.stdout.decode('utf-8').rstrip()

            package.source.git.version = commit_id

        if security is None or len(security.dict(exclude_none=True)) == 0:
            oauth_token = apis.k8s.extract_git_oauth_token_default()
            return download_with_token_and_extract_commit_id(oauth_token)
        elif security.oauth is not None and security.oauth.valueFrom.secretKeyRef:
            secret = security.oauth.valueFrom.secretKeyRef
            oauth_token = apis.k8s.extract_git_oauth_token(secret.name, secret.key)

            try:
                return download_with_token_and_extract_commit_id(oauth_token)
            except apis.runtime.errors.CannotDownloadGitError as e:
                raise apis.runtime.errors.CannotDownloadGitError(e.message + f"\nDouble check whether the oauth-token credentials in the {secret.name} Kubernetes Secret are correct.")
        
        else:
            raise apis.models.errors.ApiError("Currently only support extracting the "
                                              "interface of base packages with an oauth-token that is already stored "
                                              "as a Secret on the cluster")

    def _download_package_dataset(self, package: apis.models.virtual_experiment.BasePackage):
        credentials = apis.k8s.extract_s3_credentials_from_dataset(package.source.dataset.security.dataset)
        output_dir = os.path.join(self._root_directory(), package.name)

        self._log.info(f"Downloading DATASET {package.name} from {package.source.dataset.security.dataset}")

        apis.s3.download_all(credentials, package.config.path or '', output_dir)

        if package.config.manifestPath and (package.config.manifestPath.startswith(package.config.path or '') is False):
            apis.s3.download_all(credentials, package.config.manifestPath, output_dir)

    def _download_package(self, package: apis.models.virtual_experiment.BasePackage, platform: str | None):
        if package.source.git is not None:
            self._download_package_git(package)
        elif package.source.dataset is not None:
            self._download_package_dataset(package)
        else:
            # Should never happen: this function is called after
            # the experiment has already been validated
            raise apis.models.errors.ApiError("Package type was neither git nor dataset")

        download_path = os.path.join(self._root_directory(), package.name)

        # VV: If there's no instruction about which platform to load, just try to find ANY that is valid
        conc_data = apis.models.virtual_experiment.StorageMetadata.from_config(
            package.config, platform, download_path)

        self._concrete_and_data[package.name] = conc_data

    def _root_directory(self) -> str | None:
        if self._temp_dir:
            return self._temp_dir.name
        else:
            return self._already_downloaded_to

    def __enter__(self):
        if self._already_downloaded_to is None:
            self._temp_dir = tempfile.TemporaryDirectory(dir=self._prefix_dir)
            self._already_downloaded_to = self._temp_dir.name
            self._do_cleanup = True
            platforms = self._ve.get_known_platforms() or [None]
            platform = platforms[0]

            for package in self._ve.base.packages:
                self._download_package(package, platform)

        ret = PackagesDownloader(
            ve=self._ve, prefix_dir=self._prefix_dir, already_downloaded_to=self._already_downloaded_to)

        ret._concrete_and_data = self._concrete_and_data

        return ret

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._do_cleanup:
            try:
                self._temp_dir.cleanup()
            finally:
                self._temp_dir = None


class IterableStreamZipOfDirectory:
    def __init__(self, root):
        self.location = root

    @classmethod
    def iter_file(cls, full_path):
        def iter_read(full_path=full_path):
            with open(full_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b''):
                    yield chunk

        return iter_read(full_path)

    def __iter__(self):

        def yield_recursively(location: str):
            folders = [(location, '/')]
            while folders:
                abs_folder, in_zip_root = folders.pop(0)
                for f in os.listdir(abs_folder):
                    if f in ['.', '..']:
                        continue

                    full = os.path.join(abs_folder, f)
                    rel_path = os.path.join(in_zip_root, f)
                    if os.path.isdir(full):
                        folders.append((full, rel_path))
                    else:
                        stat = os.stat(full)
                        mod_time = datetime.datetime.fromtimestamp(stat.st_mtime)
                        file_mode = stat.st_mode
                        yield rel_path, mod_time, file_mode, stream_zip.ZIP_64, self.iter_file(full)

        for zipped_chunk in stream_zip.stream_zip(yield_recursively(self.location)):
            yield zipped_chunk
