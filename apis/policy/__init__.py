# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

from __future__ import annotations

import logging
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import pydantic

import apis.models.errors
import apis.models.from_core
import apis.models.virtual_experiment


class SimpleExperimentRun(pydantic.BaseModel):
    uid: Optional[str] = None
    pvep_identifier: Optional[str] = None
    payload_start: Dict[str, Any] = {}

    # the injected_metadata are aso included as user metadata in payload_start
    # they are here to distinguish between user-metadata that the user added in
    # payload_start from those that the policy decided to auto-inject
    injected_metadata: Dict[str, Any] = {}

    def is_dry_run(self) -> bool:
        return self.uid is None


class PolicyBasedExperimentRun(pydantic.BaseModel):
    uid: Optional[str] = None
    policy_config: Dict[str, Any] = {}

    # Policy populates this with schema that it decides
    policy_metadata: Dict[str, Any] = {}

    # Policy generates 1 entry per PVEP instance it decided to launch
    simple_experiment_runs: List[SimpleExperimentRun] = []


class MatchingDerived(pydantic.BaseModel):
    pvep_identifier: str
    pvep_source: apis.models.virtual_experiment.ParameterisedPackage
    matching: List[Dict[str, Any]]


class PolicyPrior:
    def __init__(self, name: str, api: apis.models.from_core.BetaExperimentRestAPI):
        self._api = api
        self._name = name
        self._log = logging.getLogger(self._name)

    @property
    def name(self) -> str:
        return self._name

    @classmethod
    def _is_pvep_from_relationship(cls, pvep_def: Dict[str, Any]) -> bool:
        try:
            return 'from:relationship-transform' in pvep_def['metadata']['package']['keywords']
        except KeyError:
            return False

    @classmethod
    def _pvep_get_unique_identifier(cls, pvep_def: Dict[str, Any]) -> str:
        return '@'.join([pvep_def['metadata']['package']['name'], pvep_def['metadata']['registry']['digest']])

    def kernel_query_matching_derived(
            self,
            pvep_identifier: str,
            match_package_version: bool = False,
            must_have_one_package: bool = False,
            match_outputs_of_relationship_transform: bool = True,
    ) -> MatchingDerived:
        """Helper function to return PVEPs that match a PVEP with a specific identifier

        The algorithm ::

          1. Works only for pvep_identifiers which point to a base package with exactly 1 base package
          2. Finds PVEPs which have the same base package (MAY not take into account package version)
          3. MAY filter out those that do not contain multiple base packages
          4. Sorts packages in ascending order of created time

        Args:
          pvep_identifier: The identifier of PVEP to execute.
            The format is ${package-name}:${tag}, or ${package-name}@${digest}. If neither tag nor
            digest exists assume `:latest` tag.
          match_package_version: Whether to include package versions (i.e. source.git.version) in query
          must_have_one_package: Whether to reject packages with anything other than 1 package
          match_outputs_of_relationship_transform: Whether to reject packages which do not have
            "from:relationship-transform" keyword

        Returns:
          A MatchingDerived instance
        """
        pvep_source = self._api.api_experiment_get(pvep_identifier)
        pvep_source = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(pvep_source)

        if len(pvep_source.base.packages) != 1:
            raise apis.models.errors.ApiError(f"Source parameterised virtual experiment package does not contain "
                                              f"exactly 1 base package")
        pvep_source.base.packages[0].name = None
        pvep_matching = list(self._api.api_experiment_query(query={
            "package": {
                "definition": pvep_source.base.packages[0].dict(
                    exclude_none=True, exclude_defaults=True, exclude_unset=True)
            },
            "common": {
                "matchPackageVersion": match_package_version,
                "mustHaveOnePackage": must_have_one_package,
            }
        }).values())

        self._log.info(f"Found {len(pvep_matching)} matching PVEPs for {pvep_identifier}")

        if match_outputs_of_relationship_transform:
            from_relationship = [x for x in pvep_matching if self._is_pvep_from_relationship(x)]
            from_relationship = sorted(from_relationship, key=lambda x: x['metadata']['registry']['createdOn'])

            names = [self._pvep_get_unique_identifier(x) for x in from_relationship]
            self._log.info(f"Found {len(pvep_matching)} PVEPs from relationships: {names}")
            pvep_matching = from_relationship

        return MatchingDerived(pvep_identifier=pvep_identifier, matching=pvep_matching, pvep_source=pvep_source)

    def policy_based_run_plan(
            self,
            pvep_identifier: str,
            payload_start: Dict[str, Any],
            policy_config: Dict[str, Any],
    ) -> apis.policy.PolicyBasedExperimentRun:
        """Generates a session Plan

        Args:
          pvep_identifier: The identifier of PVEP to execute.
            The format is ${package-name}:${tag}, or ${package-name}@${digest}. If neither tag nor
            digest exists assume `:latest` tag.
          payload_start: The payload to api_experiment_start()
          policy_config: A dictionary containing instructions/metadata that the policy should take into
            account for launching the PVEP instances

        Returns:
          A PolicyBasedExperimentRun
        """
        raise NotImplementedError(f"policy_based_run_plan() for policy {self._name} is not implemented")

    def policy_based_run_commit(self, plan: PolicyBasedExperimentRun) -> PolicyBasedExperimentRun:
        """Commit to a plan about a PolicyBasedExperimentRun

        Method updates plan in-place but also returns it

        Args:
            plan: The plan to commit to

        Return:
            The plan
        """

        raise NotImplementedError(f"policy_based_run_commit() for policy {self._name} is not implemented")

    def policy_based_run_create(
            self,
            pvep_identifier: str,
            payload_start: Dict[str, Any],
            policy_config: Dict[str, Any],
            dry_run: bool = True,
    ) -> Dict[str, Any]:
        """Plans a PolicyBasedExperimentRun and may optionally launch it

        Method returns an identifier of the collection of PVEP instances it may have launched.
        If the policy decides to launch multiple PVEPs it needs to maintain a record of the mappings
        of identifiers to PVEP instance identifiers.

        Args:
          pvep_identifier: The identifier of PVEP to execute.
            The format is ${package-name}:${tag}, or ${package-name}@${digest}. If neither tag nor
            digest exists assume `:latest` tag.
          payload_start: The payload to api_experiment_start()
          policy_config: A dictionary containing instructions/metadata that the policy should take into
            account for launching the PVEP instances
          dry_run: If False will execute session (e.g. create 1 or more instances of PVEPs). Otherwise,
            will just return a potentially incomplete dictionary definition of a PolicyBasedExperimentRun

        Returns:
          A dictionary representation of PolicyBasedExperimentRun
        """

        raise NotImplementedError(f"policy_based_run_create() for policy {self._name} is not implemented")

    def policy_based_run_get_simple_run_uids(
            self,
            uid: str,
            policy_config: Dict[str, Any]
    ) -> List[str]:
        """Returns the list of ExperimentRun uids associated with a PolicyBasedExperimentRun

        Args:
          uid: The return value of a prior call to session_start()
          policy_config: A dictionary containing instructions/metadata that the policy should take into
              account when mapping the PolicyBasedExperimentRun to SimpleExperimentRuns
        """

        raise NotImplementedError(f"policy_based_run_get_simple_run_uids() for policy {self._name} is not implemented")
