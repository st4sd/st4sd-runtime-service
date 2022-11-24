# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import copy
from typing import Any
from typing import Dict
from typing import List

import experiment.service.db

import apis.db.base
import apis.models.common
import apis.models.errors
import apis.models.virtual_experiment
import apis.policy


class PolicyPreferSurrogate(apis.policy.PolicyPrior):
    def __init__(self, api: experiment.service.db.ExperimentRestAPI):
        super(PolicyPreferSurrogate, self).__init__("prefer-surrogate", api)

    def policy_based_run_plan(
            self,
            pvep_identifier: str,
            payload_start: Dict[str, Any],
            policy_config: Dict[str, Any],
    ) -> apis.policy.PolicyBasedExperimentRun:
        """Generates a PolicyBasedExperimentRun plan

        This policy uses the following algorithm to plan which PVEP to run: ::

          1. Works only for pvep_identifiers which point to a base package with exactly 1 base package
          2. Finds PVEPs which have the same base package (not matching package version)
          3. Filters out those that do not contain multiple base packages
          4. Filters out those that do not contain the "from:relationship-transform" keyword
          5. If there is at least 1 candidate left, use the most recently created candidate.
             Otherwise uses the pvep_identifier

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
        matching = self.kernel_query_matching_derived(pvep_identifier)

        from_relationship = matching.matching
        pvep_source = matching.pvep_source

        if from_relationship:
            most_recent = from_relationship[-1]
            name = self._pvep_get_unique_identifier(most_recent)
            if len(from_relationship) > 1:
                self._log.info(f"Identified multiple compatible PVEPs will use {name} for {pvep_identifier}")
            else:
                self._log.info(f"Will use {name} for {pvep_identifier}")
            target_identifier = name
            pvep_target = most_recent
            use_original = False
        else:
            use_original = True
            pvep_target = pvep_source.dict()
            target_identifier = pvep_identifier

        from_relationship.clear()

        identifier = apis.models.common.PackageIdentifier(target_identifier)

        injected_metadata = {
            "st4sd-runtime-policy-original-package-name": pvep_source.metadata.package.name,
            "st4sd-runtime-policy-original-package-digest": pvep_source.metadata.registry.digest,
            "st4sd-runtime-policy-selected-package-name": identifier.name,
            "st4sd-runtime-policy-selected-package-digest": pvep_target['metadata']['registry']['digest'],
            "st4sd-runtime-policy-name": self._name,
            "st4sd-runtime-policy-selected-original": use_original,
            # VV: TODO Add policy-rest-uid here
        }

        payload_start = copy.deepcopy(payload_start or {})

        if 'runtimePolicy' in payload_start:
            del payload_start['runtimePolicy']

        if 'metadata' not in payload_start:
            payload_start['metadata'] = {}

        payload_start['metadata'].update(injected_metadata)

        plan = apis.policy.PolicyBasedExperimentRun(policy_config=policy_config)

        plan.simple_experiment_runs.append(apis.policy.SimpleExperimentRun(
            pvep_identifier=target_identifier,
            payload_start=payload_start,
            injected_metadata=injected_metadata))

        return plan

    def policy_based_run_create(
            self,
            pvep_identifier: str,
            payload_start: Dict[str, Any],
            policy_config: Dict[str, Any],
            dry_run: bool = False,
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
        plan = self.policy_based_run_plan(pvep_identifier, payload_start, policy_config)

        if dry_run is False:
            plan = self.policy_based_run_commit(plan)

        return plan.dict()

    def policy_based_run_commit(
            self,
            plan: apis.policy.PolicyBasedExperimentRun
    ) -> apis.policy.PolicyBasedExperimentRun:
        """Commit to a plan about a PolicyBasedExperimentRun

        Method updates plan in-place but also returns it

        Args:
            plan: The plan to commit to

        Return:
            The plan
        """
        # VV: There's only ever going to be a single instance so we can just use that name
        # as the PolicyBasedExperimentRun identifier
        for simple in plan.simple_experiment_runs:
            simple.uid = self._api.api_experiment_start(simple.pvep_identifier, simple.payload_start)
            plan.uid = simple.uid

        return plan

    def policy_based_run_get_simple_run_uids(
            self,
            uid: str,
            policy_config: Dict[str, Any]
    ) -> List[str]:
        """Returns the list of rest-uids associated with a session identifier

        Args:
          uid: The return value of a prior call to session_start()
          policy_config: A dictionary containing instructions/metadata that the policy should take into
              account when mapping the session_identifier to rest_uids
        """
        return [uid]
