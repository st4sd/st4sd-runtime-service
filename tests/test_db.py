# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


import json
import logging
import os
import pprint
import tempfile
import time
from typing import Dict
from typing import List

import pytest

import apis.db.exp_packages
import apis.models.common
import apis.models.errors
import apis.models.virtual_experiment

log = logging.getLogger('tdb')


def test_insert(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            ve_sum_numbers.metadata.package.tags = ['latest']
            db.push_new_entry(ve_sum_numbers)

        old_digest = ve_sum_numbers.metadata.registry.digest
        # VV: Simulate changing the source location to something new
        base = ve_sum_numbers.base.packages[0]

        print("Base dict:")
        print(base.dict())
        source = base.source

        assert source.git is not None
        assert source.dataset is None

        source.git.location = apis.models.virtual_experiment.SourceGitLocation(
            url='new-url',
            branch='new-branch',
            tag=None,
            commit=None)

        ve_sum_numbers.update_digest()
        new_digest = ve_sum_numbers.metadata.registry.digest

        assert new_digest != old_digest

        # VV: Ask the database to update the entry
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            db.push_new_entry(ve_sum_numbers)

        # VV: Make sure that there are 2 entries in the db, and that the old one does not have a registry tag
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            many = [apis.models.virtual_experiment.ParameterisedPackage.parse_obj(x) for x in db.query()]

        many = sorted(many, key=lambda x: x.registry_created_on)
        assert len(many) == 2

        old = many[0]
        new = many[1]

        assert old.metadata.registry.digest == old_digest
        assert new.metadata.registry.digest == new_digest

        print(old.metadata.registry.dict())

        assert old.metadata.registry.tags == []
        assert old.metadata.package.tags == ['latest']

        assert new.metadata.registry.tags == ['latest']
        assert new.metadata.package.tags == ['latest']


def test_insert_many_same(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            for _ in range(10):
                db.push_new_entry(ve_sum_numbers)

        # VV: Make sure that there are 2 entries in the db, and that the old one does not have a registry tag
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            many = [apis.models.virtual_experiment.ParameterisedPackage.parse_obj(x) for x in db.query()]

        assert len(many) == 1


def test_record_timesExecuted(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            db.push_new_entry(ve_sum_numbers)

            for idx in range(10):
                ve_sum_numbers.metadata.registry.timesExecuted = idx
                db.upsert(ve_sum_numbers.dict(exclude_none=False), ql=db.construct_query(
                    package_name=ve_sum_numbers.metadata.package.name,
                    registry_digest=ve_sum_numbers.metadata.registry.digest))

        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            many = [apis.models.virtual_experiment.ParameterisedPackage.parse_obj(x) for x in db.query()]

        pprint.pprint(many)

        assert len(many) == 1
        assert many[0].metadata.registry.timesExecuted == 9


def test_generate_history(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    base = ve_sum_numbers.base.packages[0]
    source: apis.models.virtual_experiment.BasePackageSourceGit = base.source

    # VV: format is {tag: head_digest}
    heads: Dict[str, str] = {}

    def push(db: apis.db.exp_packages.DatabaseExperiments, ve: apis.models.virtual_experiment.ParameterisedPackage):
        # VV: Push then wait for some time so that next push has a different createdOn timestamp
        ve.metadata.registry.createdOn = ve.metadata.registry.get_time_now_as_str()
        ve.update_digest()
        db.push_new_entry(ve)
        time.sleep(0.1)

    def simulate_changes(db: apis.db.exp_packages.DatabaseExperiments, tag: str, times: int,
                         untagged_digests: List[str]) -> str:
        ve_sum_numbers.metadata.package.tags = [tag]
        ve_sum_numbers.metadata.registry.tags = []

        for i in range(times):
            # VV: Simulate changing the source location to something new
            source.git.location = apis.models.virtual_experiment.SourceGitLocation(url=f'new-url-{tag}', branch=f'{i}')
            push(db, ve_sum_numbers)

            # VV: This will get untagged
            untagged_digests.append(ve_sum_numbers.metadata.registry.digest)

        # VV: This will now be the head
        source.git.location = apis.models.virtual_experiment.SourceGitLocation(url=f'new-url-{tag}', branch="head")
        push(db, ve_sum_numbers)

        return ve_sum_numbers.metadata.registry.digest

    untagged_hello = []
    untagged_unique = []
    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            # VV: When hello2 pushed, it removed the `latest` and `hello` tag from `hello1`
            # VV: When hello1 pushed, it removed the `latest` and `hello` tag from `hello0`
            # VV: When hello0 pushed, it did not remove a tag from anything
            heads['hello'] = simulate_changes(db, 'hello', 2, untagged_hello)

            # VV: When `unique` pushed, it removed the `latest` tag from `hello2`
            heads['unique'] = simulate_changes(db, 'unique', 0, untagged_unique)
            heads['latest'] = heads['unique']

            history = db.trace_history(ve_sum_numbers.metadata.package.name)

            docs = db.query()
            for i, d in enumerate(docs):
                log.info(f"Doc[{i}] = {json.dumps(d, indent=2)}")

        trace = history.to_dict()

        log.info(f"All history trace: {json.dumps(trace, indent=2)}")

        untagged_digests = trace['untagged']

        untagged_grouped_by_tag = {}
        for x in untagged_digests:
            ot = x['originalTag']
            if ot not in untagged_grouped_by_tag:
                untagged_grouped_by_tag[ot] = []
            untagged_grouped_by_tag[ot].append(x)

        # VV: untagged digests should have 2 entries for hello0, hello1
        # (once with `hello` tag and another with `latest` tag)
        assert len(untagged_grouped_by_tag['hello']) == 2
        assert len(untagged_grouped_by_tag['latest']) == 2

        # VV: hello2 is still tagged with `hello (1 tagged entry)
        # VV: unique0 is tagged with `unique` and `latest` (2 tagged entries)
        assert len(trace['tags']) == 3

        for tag_info in trace['tags']:
            assert tag_info['head'] == heads[tag_info['tag']]

        assert heads['hello'] != heads['unique']
        assert heads['unique'] == heads['latest']


def test_update_tags_with_single_parameterised_package(
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    orig = list(ve_sum_numbers.metadata.package.tags)

    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            db.push_new_entry(ve_sum_numbers)

            for idx in range(10):
                db.tag_update(ve_sum_numbers.metadata.package.name, ["latest", f"lbl-{idx}"])

        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            many = [apis.models.virtual_experiment.ParameterisedPackage.parse_obj(x) for x in db.query()]

        pprint.pprint(many)

        assert len(many) == 1
        assert sorted(many[0].metadata.registry.tags) == sorted(["latest", f"lbl-9"])


def test_update_tags_with_many_parameterised_packages(
        ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    orig = list(ve_sum_numbers.metadata.package.tags)

    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            db.push_new_entry(ve_sum_numbers)

            # VV: Change something that would reuslt in a different digest
            ve_sum_numbers.parameterisation.presets.platform = "this-is-definitely-new"
            log.info(f"Orig digest {ve_sum_numbers.metadata.registry.digest}")
            ve_sum_numbers.update_digest()
            log.info(f"New digest {ve_sum_numbers.metadata.registry.digest}")

            db.push_new_entry(ve_sum_numbers)

            for idx in range(10):
                db.tag_update(ve_sum_numbers.metadata.package.name, ["latest", f"lbl-{idx}"])

        ql = db.construct_query(package_name=ve_sum_numbers.metadata.package.name)
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            many = [apis.models.virtual_experiment.ParameterisedPackage.parse_obj(x)
                    for x in db.query(ql)]

        log.info(pprint.pformat(many))

        assert len(many) == 2

        new_identifier = apis.models.common.PackageIdentifier.from_parts(
            package_name=ve_sum_numbers.metadata.package.name,
            tag="lbl-9",
            digest=None).identifier

        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            single = [apis.models.virtual_experiment.ParameterisedPackage.parse_obj(x)
                      for x in db.query_identifier(new_identifier)]

        log.info(pprint.pformat(single))

        assert len(single) == 1
        assert sorted(single[0].metadata.registry.tags) == sorted(["latest", f"lbl-9"])


def test_cannot_remove_latest_tag(ve_sum_numbers: apis.models.virtual_experiment.ParameterisedPackage):
    orig = list(ve_sum_numbers.metadata.package.tags)

    with tempfile.NamedTemporaryFile(suffix=".json", prefix="experiments", delete=True) as f:
        with apis.db.exp_packages.DatabaseExperiments(f.name) as db:
            db.push_new_entry(ve_sum_numbers)

            with pytest.raises(apis.models.errors.CannotRemoveLatestTagError) as e:
                db.tag_update(ve_sum_numbers.metadata.package.name, ["hello"])


def test_query_relationship_identifier(output_dir: str):
    import apis.kernel.relationships
    import apis.models.query_relationship
    import apis.models.relationships

    db = apis.db.relationships.DatabaseRelationships(os.path.join(output_dir, 'db'))

    with db:
        db.insert_many([
            apis.models.relationships.Relationship.parse_obj({'identifier': 'hello-world'}).dict(),
            apis.models.relationships.Relationship.parse_obj({'identifier': 'not-hello-world'}).dict(),
        ])

    q = apis.models.query_relationship.QueryRelationship.parse_obj({'identifier': 'hello.*'})

    x = apis.kernel.relationships.api_list_queries(q, db)

    assert len(x) == 1
    assert x[0]['identifier'] == 'hello-world'


def test_query_relationship_inputgraph_identifier(output_dir: str):
    import apis.models.query_relationship
    import apis.models.relationships

    db = apis.db.relationships.DatabaseRelationships(os.path.join(output_dir, 'db'))

    with db:
        db.insert_many([
            apis.models.relationships.Relationship.parse_obj({
                'identifier': 'hello-world',
                'transform': {
                    'outputGraph': {'identifier': 'dummy'},
                    'inputGraph': {'identifier': 'hello-world'}
                }
            }).dict(),
            apis.models.relationships.Relationship.parse_obj({
                'identifier': 'not-hello-world',
                'transform': {
                    'outputGraph': {'identifier': 'hello-world'},
                    'inputGraph': {'identifier': 'dummy'}
                }
            }).dict(),
        ])

    q = apis.models.query_relationship.QueryRelationship.parse_obj({
        'transform': {
            'inputGraph': {'identifier': 'hello-world:.*$'}
        }})

    x = apis.kernel.relationships.api_list_queries(q, db)

    assert len(x) == 1
    assert x[0]['transform']['inputGraph']['identifier'] == 'hello-world:latest'
