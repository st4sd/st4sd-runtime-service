# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis
#   Alessandro Pomponio

from __future__ import annotations

import datetime
import os
import subprocess
import tempfile
from typing import List
from typing import Optional

import stream_zip

import apis.db.secrets
import apis.k8s
import apis.models.common
import apis.models.errors
import apis.models.virtual_experiment
import apis.runtime.errors
import apis.s3
import utils

import apis.storage.actuators.s3
import apis.storage.actuators.base

from .collection import PackageMetadataCollection


class PackagesDownloader(PackageMetadataCollection):
    def __init__(
            self,
            ve: apis.models.virtual_experiment.ParameterisedPackage | None,
            db_secrets: apis.db.secrets.DatabaseSecrets | None,
            prefix_dir: str | None = None,
            already_downloaded_to: str | None = None,
            local_deployment: Optional[bool] = None,
    ):
        """Downloads base packages of a parameterised package

        Args:
             ve: the parameterised package that owns the base packages
             db_secrets: The Secrets database (see @local_deployment doc)
             prefix_dir: The location that would host the packages if they were downloaded
             already_downloaded_to: The location that the packages where downloaded into
                the first time __enter__() ran
            local_deployment: Whether the API is running in LOCAL mode - when None
                this defaults to True if db_secrets is not an instance of
                apis.db.secrets.KubernetesSecrets, otherwise it defaults to False
        """
        super(PackagesDownloader, self).__init__(ve=ve, db_secrets=db_secrets)
        self._prefix_dir = prefix_dir or "/tmp"
        self._temp_dir: tempfile.TemporaryDirectory | None = None
        self._already_downloaded_to = already_downloaded_to

        self._local_deployment = local_deployment if local_deployment is not None else \
            not isinstance(db_secrets, apis.db.secrets.KubernetesSecrets)

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

        # VV: 1st step, identify whether git-clone should use a git oauth-token and which secret/key contains its value
        oauth_token = None
        secret_name = ...
        secret_key = ...
        is_default_secret = False

        if security is None or len(security.dict(exclude_none=True)) == 0:
            # VV: There's no Secret associated with this specific base package, check if there's a default one
            # for all base packages that this API instance can pull
            config = utils.parse_configuration(
                local_deployment=self._local_deployment,
                validate=False
            )

            if config.gitsecretOauth:
                secret_name = config.gitsecretOauth
                secret_key = "oauth-token"
                is_default_secret = True
        elif security.oauth is not None:
            if security.oauth.valueFrom:
                if security.oauth.valueFrom.secretKeyRef:
                    secret_ref = security.oauth.valueFrom.secretKeyRef
                    secret_name = secret_ref.name
                    secret_key = secret_ref.key or "oauth-token"
                else:
                    raise apis.models.errors.ApiError(
                        f"Base package {package.name} specifies package.source.git.security.oauth.valueFrom "
                        f"but package.source.git.security.oauth.valueFrom.secretKeyRef is empty")
            elif security.oauth.value:
                raise apis.models.errors.ApiError(
                    f"Base package {package.name} specifies package.source.git.security.oauth.value "
                    f"this indicates a programming error. The package should instead be using "
                    f"package.source.git.security.oauth.valueFrom.secretKeyRef")

        # VV: If there's a secret to use (which could be a "default" for all Base packages that don't explicitly
        # specify one) - try to look up the secret and extract the oauth-token from it
        if secret_name is not ...:
            what = "default " if is_default_secret else ""

            try:
                with self.db_secrets:
                    secret = self.db_secrets.secret_get(secret_name)
                if secret is None:
                    raise apis.runtime.errors.CannotDownloadGitError(
                        f"Cannot git clone because {what}secret {secret_name} with oauth credentials does not exist")
            except apis.runtime.errors.RuntimeError:
                raise
            except apis.models.errors.ApiError as e:
                raise apis.runtime.errors.CannotDownloadGitError(
                    f"Cannot git clone because the {what}secret {secret_name} of an underlying error while getting"
                    f"the secret. Underlying error: {e}")

            try:
                oauth_token = secret['data'][secret_key]
            except KeyError as e:
                raise apis.runtime.errors.CannotDownloadGitError(
                    f"Cannot git clone because the {what}secret {secret_name} does not contain "
                    f"the key data.{secret_key}. Underlying error: {e}")

        try:
            return download_with_token_and_extract_commit_id(oauth_token)
        except apis.runtime.errors.CannotDownloadGitError as e:
            if secret_name is not ...:
                raise apis.runtime.errors.CannotDownloadGitError(
                    f"Cannot git-clone with location {package.source.git.location.dict()} using the oauth credentials "
                    f"in Secret {secret_name}. Double check the location and then verify that the oauth credentials "
                    f"can clone the repository. Underlying error: {e.message}")
            raise apis.runtime.errors.CannotDownloadGitError(
                f"Cannot git-clone with location {package.source.git.location.dict()} without using any oauth "
                f"credentials. Double check that the location is correct and set to public. Underlying error: "
                f"{e.message}")

    def _download_package_dataset(self, package: apis.models.virtual_experiment.BasePackage):
        self._log.info(f"Downloading DATASET {package.name} from {package.source.dataset.security.dataset} via S3")
        credentials = apis.k8s.extract_s3_credentials_from_dataset(package.source.dataset.security.dataset)

        # VV: instead of handling a Dataset, convert the package to a S3 package and use that instead
        mock_package = package.copy(deep=True)
        mock_package.source.dataset = None
        mock_package.source.s3 = apis.models.virtual_experiment.BasePackageSourceS3(
            security=apis.models.virtual_experiment.BasePackageSourceS3Security(
                credentials=apis.models.virtual_experiment.SourceS3SecurityCredentials(
                    value=apis.models.virtual_experiment.SourceS3SecurityCredentialsValue(
                        accessKeyID=credentials.accessKeyID,
                        secretAccessKey=credentials.secretAccessKey,
                    )
                ),
            ),
            location = apis.models.virtual_experiment.BasePackageSourceS3Location(
                bucket=credentials.bucket,
                endpoint=credentials.endpoint,
                region=credentials.region,
            )
        )

        self._download_package_s3(mock_package)
        del mock_package

    def _download_package_s3(
        self,
        package: apis.models.virtual_experiment.BasePackage,
    ):
        output_dir = os.path.join(self._root_directory(), package.name)

        self._log.info(f"Downloading S3 {package.name} from {package.source.s3.location.endpoint} "
                       f"(bucket: {package.source.s3.location.bucket})")
        s3_access = apis.storage.actuators.storage_actuator_for_package(
            package=package,
            db_secrets=self.db_secrets,
        )

        dest = apis.storage.actuators.base.Storage()

        dest.copy(
            source=s3_access,
            source_path=package.config.path or "",
            dest_path=output_dir
        )

        if package.config.manifestPath and (package.config.manifestPath.startswith(package.config.path or '') is False):
            s3_access.store_to_file(
                src=package.config.manifestPath,
                dest=os.path.join(output_dir, os.path.basename(package.config.manifestPath))
            )

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

        self._metadata[package.name] = conc_data

    def _root_directory(self) -> str | None:
        if self._temp_dir:
            return self._temp_dir.name
        else:
            return self._already_downloaded_to

    def __enter__(self):
        super().__enter__()

        if self._already_downloaded_to is None:
            self._temp_dir = tempfile.TemporaryDirectory(dir=self._prefix_dir)
            self._already_downloaded_to = self._temp_dir.name
            platforms = self._ve.get_known_platforms() or [None]
            platform = platforms[0]

            for package in self._ve.base.packages:
                self._download_package(package, platform)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)

        if self._entered == 0:
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

