# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

from __future__ import annotations

from typing import Any
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import TYPE_CHECKING

import tinydb

import apis.db.base
import apis.models.common
import apis.models.errors

if TYPE_CHECKING:
    import tinydb.table
    import apis.models.virtual_experiment


class TagInfo(NamedTuple):
    tag: str
    head: str  # VV: The head digest
    times_executed: int | None
    created_on: str


class Untagged(NamedTuple):
    digest: str
    user_tag: str | None
    times_executed: int | None
    created_on: str


class History:
    def __init__(self):
        self._tags: Dict[str, TagInfo] = {}
        self._untagged: List[Untagged] = []

    def to_dict(self) -> Dict[str, Any]:
        return {
            'tags': [
                {'tag': x.tag, 'head': x.head, 'timesExecuted': x.times_executed, 'createdOn': x.created_on}
                for x in self._tags.values()
            ],
            'untagged': [
                {
                    'digest': x.digest,
                    'timesExecuted': x.times_executed,
                    'createdOn': x.created_on,
                    'originalTag': x.user_tag
                }
                for x in self._untagged
            ]
        }

    def add_tag(self, tags: List[str], digest: str, times_executed: int | None, created_on: str):
        for tag in tags:
            self._tags[tag] = TagInfo(tag, digest, times_executed, created_on)

    def add_untagged(self, digest: str, user_tags: List[str], times_executed: int | None, created_on: str):
        for user_tag in user_tags:
            self._untagged.append(Untagged(digest, user_tag, times_executed, created_on))


class DatabaseExperiments(apis.db.base.Database):
    """The pattern is:

    with DatabaseExperiments(Path to db) as db:
        db.query(...)
    """

    def __init__(self, db_path: str):
        super(DatabaseExperiments, self).__init__(db_path, db_label="exp")

    @classmethod
    def construct_query(
            cls,
            package_name: str,
            registry_tag: str | None = None,
            registry_digest: str | None = None) -> tinydb.table.QueryLike:
        entry = tinydb.Query()
        ql = entry.metadata.package.name == package_name

        if registry_tag:
            ql &= entry.metadata.registry.tags.any([registry_tag])

        if registry_digest:
            ql &= entry.metadata.registry.digest == registry_digest

        return ql

    @classmethod
    def _common_keys_have_equal_values(cls, d1: Dict[str, Any], d2: Dict[str, Any]) -> bool:
        pending = []

        for k in set(d1).intersection(d2):
            pending.append((d1[k], d2[k]))

        while pending:
            v1, v2 = pending.pop(0)

            if isinstance(v1, dict):
                if isinstance(v2, dict) is False:
                    return False
                for k in set(v1).intersection(v2):
                    pending.append((v1[k], v2[k]))
            else:
                if v1 != v2:
                    return False

        return True

    @classmethod
    def construct_query_for_package(
            cls,
            package: apis.models.virtual_experiment.BasePackage | Dict[str, Any],
            have_just_one_package: bool,
    ) -> tinydb.table.QueryLike:
        """Query for testing whether a PVEP uses a package

        Query discards unset, None, and default values of package. The query also only cares for fields that the
        package has a value for.

        For example, you can query for docs using just a subset of the fields that you would expect to find in a valid
        (i.e. complete) base package definition.

        Args:
            package: The package to query for
            have_just_one_package: If true will not match documents which contain more than 1 base package
        """

        def test_closure():
            if isinstance(package, apis.models.common.Digestable) is False:
                match_test = package
            else:
                # VV: Throw away most things we don't care for
                match_test = package.model_dump(exclude_unset=True, exclude_defaults=True, by_alias=True, exclude_none=True)

            def matches(packages: List[Dict[str, Any]], pkg_filter: Dict[str, Any] = match_test) -> bool:
                if have_just_one_package and len(packages) != 1:
                    return False

                for pkg in packages:
                    if cls._common_keys_have_equal_values(pkg_filter, pkg):
                        return True

                return False

            return matches

        ql = tinydb.Query().base.packages.test(test_closure())

        return ql

    @classmethod
    def construct_query_for_identifier(cls, identifier: str) -> tinydb.table.QueryLike:
        identifier = apis.models.common.PackageIdentifier(identifier)
        everything = identifier.parse()
        return cls.construct_query(
            package_name=everything.name,
            registry_tag=everything.tag,
            registry_digest=everything.digest)

    def query_identifier(self, identifier: str) -> List[tinydb.table.Document]:
        ql = self.construct_query_for_identifier(identifier)
        return self.query(ql)

    def delete_identifier(self, identifier: str):
        ql = self.construct_query_for_identifier(identifier)
        return self.delete(ql)

    def tag_update(self, identifier: str, new_tags: List[str]):
        """Updates the entry matching the @identifier to have the @new_tags and untags any existing entries with same
        $packageName so that they dot feature any of the @new_tags.

        Args:
            identifier: $packageName:$tag or $packageName (implied :latest suffix), or $packageName@$digest
            new_tags: A list of tags to ADD to the entry that @identifier points to

        Raises:
            apis.models.errors.ParameterisedPackageNotFoundError: No parameterised package with @identifier
            apis.models.errors.CannotRemoveLatestTagError: new_tags does not contain "latest" but parameterised package
                tags do (i.e. metadata.registry.tags)
        """
        self._log.info(f"Setting the tags of {identifier} to {new_tags}")

        pi = apis.models.common.PackageIdentifier(identifier)

        matches = self.query_identifier(identifier)

        if len(matches) == 0:
            raise apis.models.errors.ParameterisedPackageNotFoundError(identifier)

        if len(matches) != 1:
            raise ValueError(f"Expected to find exactly 1 match for {identifier} but found {len(matches)} instead")

        pp = apis.models.virtual_experiment.ParameterisedPackageDropUnknown.model_validate(matches[0])

        if 'latest' in pp.metadata.registry.tags and 'latest' not in new_tags:
            raise apis.models.errors.CannotRemoveLatestTagError(identifier)

        for t in new_tags:
            qi = apis.models.common.PackageIdentifier.from_parts(pi.name, tag=t, digest=None)
            matches = self.query_identifier(qi.identifier)
            if not matches:
                continue
            elif len(matches) > 1:
                self._log.warning(f"Found {len(matches)} entries for identifier {qi.identifier} - will update all")

            for m in matches:
                mpp = apis.models.virtual_experiment.ParameterisedPackageDropUnknown.model_validate(m)
                if mpp.metadata.registry.digest == pp.metadata.registry.digest:
                    continue
                mpp.metadata.registry.tags.remove(t)
                self.upsert(mpp.model_dump(exclude_none=True), self.construct_query_for_identifier(qi.identifier))

        pp.metadata.package.tags = list(new_tags)
        pp.metadata.registry.tags = list(new_tags)

        self.upsert(pp.model_dump(exclude_none=True), self.construct_query_for_identifier(pi.identifier))

    def push_new_entry(self, ve: apis.models.virtual_experiment.ParameterisedPackage, update_digest=True):
        # VV: Need to find if there's a document which the registry currently thinks it's the most recent with user tag
        # and update the registry_tag to None

        if update_digest:
            ve.update_digest()

        def update(tag: str):
            identifier = ":".join((ve.metadata.package.name, tag))
            if tag not in ve.metadata.registry.tags:
                ve.metadata.registry.tags.append(tag)

            ql = self.construct_query(package_name=ve.metadata.package.name, registry_tag=tag)
            existing = self.query(ql)

            self._log.info(f"Found {len(existing)} matching experiments")

            if ve.metadata.registry.digest is None:
                raise ValueError("Updated Parameterised package is missing a digest - this is a bug")

            to_upsert = []

            for doc in existing:
                self._log.info(f"There already exists a {identifier} - will forget about it")
                # VV: No need to test for existence of tag because the DB told us the tag exists in this document
                old_tags: List[str] = doc['metadata']['registry']['tags']
                old_tags.remove(tag)
                to_upsert.append(doc)

            if to_upsert:
                self.delete(ql)

            if ve.metadata.registry.digest is None:
                ve.update_digest()

            to_upsert.append(ve.model_dump(exclude_none=True))

            for doc in to_upsert:
                ql = self.construct_query(
                    package_name=doc['metadata']['package']['name'],
                    registry_digest=doc['metadata']['registry']['digest']
                )
                self.upsert(doc, ql)

        tags = list(ve.metadata.package.tags or [])
        if 'latest' not in tags:
            tags.append('latest')

        for tag in tags:
            update(tag)

    def trace_history(self, package_name: str) -> History:
        history = History()

        ql = self.construct_query(package_name=package_name)
        docs = self.query(ql)

        for doc in docs:
            tags = doc['metadata']['registry']['tags']
            digest = doc['metadata']['registry']['digest']
            times_executed = doc['metadata']['registry'].get('timesExecuted')
            created_on = doc['metadata']['registry'].get('createdOn')
            user_tags = list(doc['metadata']['package']['tags'])

            if 'latest' not in user_tags:
                user_tags.append('latest')

            if tags:
                history.add_tag(tags, digest, times_executed, created_on)
            else:
                history.add_untagged(digest, user_tags, times_executed, created_on)

        return history
