# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis


from __future__ import annotations

import copy
import random
from typing import Any
from typing import Dict

import experiment.service.db
import pydantic.error_wrappers

import apis.db.base
import apis.models.common
import apis.models.errors
import apis.models.virtual_experiment
import apis.policy


class PolicyDB(apis.db.base.Database):
    pass


class PolicyConfig(apis.models.common.Digestable):
    probabilitySurrogate: float = pydantic.Field(
        0.5, ge=0.0, le=1.0, description="Probability to attempt to launch a matching Derived package "
                                         "that has been created from a relationship.transfoorm")


class PolicyRandomCanarySurrogate(apis.policy.PolicyPrior):
    def __init__(self, api: experiment.service.db.ExperimentRestAPI):
        super(PolicyRandomCanarySurrogate, self).__init__("random-canary-surrogate", api)

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
        try:
            config: PolicyConfig = PolicyConfig.parse_obj(policy_config)
        except pydantic.error_wrappers.ValidationError as e:
            raise apis.models.errors.ApiError(f"Invalid PolicyConfig payload, problems: {e.json(indent=2)}")

        try:
            if config.probabilitySurrogate > random.random():
                matching = self.kernel_query_matching_derived(pvep_identifier)
            else:
                pvep_source = self._api.api_experiment_get(pvep_identifier)
                pvep_source = apis.models.virtual_experiment.ParameterisedPackage.parse_obj(pvep_source)
                matching = apis.policy.MatchingDerived(
                    pvep_identifier=pvep_identifier, pvep_source=pvep_source, matching=[])
        except pydantic.error_wrappers.ValidationError as e:
            raise apis.models.errors.ApiError(f"The PVEP {pvep_identifier} is invalid. Consider using a different "
                                              f"version, problems: {e.json(indent=2)}")

        from_relationship = matching.matching
        pvep_source = matching.pvep_source

        if from_relationship:
            most_recent = from_relationship[-1]
            name = '@'.join([most_recent['metadata']['package']['name'], most_recent['metadata']['registry']['digest']])
            if len(from_relationship) > 1:
                self._log.info(f"Identified multiple compatible PVEPs will use {name} for {pvep_identifier}")
            else:
                self._log.info(f"Will use {name} for {pvep_identifier}")
            # VV: place the pvep_source Last so that we can use the last SimpleExperimentRun uid
            # as the PolicyBasedExperimentRun uid too. This way the returned SimpleExperimentRun uid is always
            # that of the pvep_source (requested pvep)
            from_relationship = [most_recent, pvep_source.dict()]
        else:
            from_relationship = [pvep_source.dict()]

        full_source_identifier = matching.pvep_source.metadata.get_unique_identifier_str()

        plan = apis.policy.PolicyBasedExperimentRun(policy_config=policy_config)

        for pvep_target in from_relationship:
            target_identifier = self._pvep_get_unique_identifier(pvep_target)
            identifier = apis.models.common.PackageIdentifier(target_identifier)
            use_original = (full_source_identifier == identifier.identifier)

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

