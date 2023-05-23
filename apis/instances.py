# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis
#   Yiannis Gkoufas

from __future__ import annotations

import copy
import os
import re
import traceback
from typing import List, Dict, Any, cast, Optional

import experiment.model.frontends.flowir
import kubernetes.client.exceptions
import pandas
import werkzeug.exceptions
import yaml
from flask import current_app, Response
from flask_restx import Resource, reqparse, inputs
from kubernetes import client

import apis.models
import apis.models.common
import apis.models.virtual_experiment
import apis.storage
import utils
from utils import setup_config, WORKING_VOLUME_MOUNT

api = apis.models.api_instances
experiment_instance = apis.models.experiment_instance

DictWorkflow = Dict[str, Any]
DictExperimentDefinition = Dict[str, Any]


def postprocess_workflow_dictionary(k8s_workflow: DictWorkflow, ve_def: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """This decodes the status (which is a string representation of a YAML dictionary)

    Args:
         k8s_workflow: A dictionary representation of a Workflow object
         ve_def: The Dictionary definition of a virtual experiment
    """
    if 'status' in k8s_workflow:
        status_plain = copy.deepcopy(k8s_workflow['status'])
        err_desc = status_plain.get('errordescription', '')
    else:
        err_desc = ''
        status_plain = None

    outputfiles = {}

    # VV: Default values for status fields
    status_rest = {
        "experiment-state": None,
        "stage-state": None,
        "stages": [],
        "current-stage": None,
        "exit-status": None,
        "meta": None,
        "total-progress": 0.0,
        "stage-progress": 0.0,
        "error-description": err_desc,
    }

    # VV: If a status is present, fill it in
    if status_plain is not None:
        if ("outputfiles" in status_plain):
            outputfiles = status_plain.pop("outputfiles")

        if "stages" in status_plain:
            status_rest["stages"] = status_plain["stages"]

        if "currentstage" in status_plain:
            status_rest["current-stage"] = status_plain["currentstage"]

        if "experimentstate" in status_plain:
            status_rest["experiment-state"] = status_plain["experimentstate"]

        if status_plain.get("meta"):
            try:
                status_rest["meta"] = yaml.load(status_plain["meta"], Loader=yaml.SafeLoader)
            except Exception as e:
                current_app.logger.info("Unable to decode status.meta: %s - will ignore error" % e)

        if "stagestate" in status_plain:
            status_rest["stage-state"] = status_plain["stagestate"]

        if ("totalprogress" in status_plain
                and status_plain["totalprogress"] is not None
                and len(status_plain["totalprogress"]) > 0):
            try:
                status_rest["total-progress"] = float(status_plain["totalprogress"])
            except Exception as e:
                print(e)
        if ("exitstatus" in status_plain):
            try:
                status_rest["exit-status"] = status_plain["exitstatus"]
            except Exception as e:
                print(e)

        if ("stageprogress" in status_plain
                and status_plain["stageprogress"] is not None
                and len(status_plain["stageprogress"]) > 0):
            try:
                status_rest["stage-progress"] = float(status_plain["stageprogress"])
            except Exception as e:
                print(e)
    else:
        # VV: If status has not been generated yet, then just propagate this updwards
        status_rest = None

    k8s_labels = k8s_workflow.get("metadata", {}).get("labels", {})
    instance = {"id": k8s_labels.get('rest-uid', k8s_workflow["metadata"]["uid"]),
                "name": k8s_workflow["metadata"]["name"], "status": status_rest, "outputs": outputfiles,
                "k8s-labels": k8s_labels, "experiment": ve_def or {}}

    return instance


def extract_virtual_experiment_entry(item) -> Dict[str, Any] | None:
    instance_id = item.get('metadata', {}).get('name')
    package_name = item.get('metadata', {}).get('labels', {}).get('st4sd-package-name')
    digest = item.get('metadata', {}).get('labels', {}).get('st4sd-package-digest')

    if package_name and digest:
        with utils.database_experiments_open() as db:
            identifier = apis.models.common.PackageIdentifier.from_parts(
                package_name=package_name, tag=None, digest=digest)
            docs = db.query_identifier(identifier.identifier)
            if len(docs) == 1:
                return docs[0]
            else:
                current_app.logger.info(f"Found {len(docs)} matching virtual experiment entries for "
                                        f"{identifier.parse()} of workflow instance {instance_id} but I expected"
                                        f" exactly 1 - will not populate experiment field of instance")
    else:
        current_app.logger.info(
            f"Workflow instance {instance_id} does not contain package name and digest labels "
            f"- will not populate experiment field of instance")


def get_list_instances(api_instance, namespace):
    to_ret = []
    try:
        api_response = api_instance.list_namespaced_custom_object(
            utils.K8S_WORKFLOW_GROUP, utils.K8S_WORKFLOW_VERSION, namespace, utils.K8S_WORKFLOW_PLURAL)
        if ("items" in api_response):
            for item in api_response["items"]:
                ve_def = extract_virtual_experiment_entry(item)
                to_ret.append(postprocess_workflow_dictionary(item, ve_def))
    except Exception as e:
        print(traceback.format_exc())
        print(e)

    return to_ret


def get_instance(instance_id):
    namespace = utils.MONITORED_NAMESPACE

    _ = setup_config()
    api_instance = client.CustomObjectsApi(client.ApiClient())

    api_response = api_instance.list_namespaced_custom_object(
        utils.K8S_WORKFLOW_GROUP, utils.K8S_WORKFLOW_VERSION, namespace, utils.K8S_WORKFLOW_PLURAL,
        field_selector="metadata.name=%s" % instance_id)

    if 'items' not in api_response:
        raise ValueError("Expected a list of objects but got %s" % api_response)
    api_response = api_response['items']

    if len(api_response) > 1:
        raise ValueError("Found %d objects with the name %s instead of just 1" % (len(api_response), instance_id))
    elif len(api_response) == 0:
        return None

    instance = api_response[0]

    ve_def = extract_virtual_experiment_entry(instance)
    return postprocess_workflow_dictionary(instance, ve_def)


def increment_progress(stageProgress, totalProgress, numberStages=2, increment=0.25):
    '''Updates stage/total progress by increment assuming equal weights'''

    if totalProgress == 1.0:
        return 1.0, 1.0

    stageProgress = stageProgress + increment if stageProgress != 1.0 else increment
    totalIncrement = increment * 1.0 / numberStages
    totalProgress = totalProgress + totalIncrement

    return stageProgress, totalProgress


@api.route('/')
class InstanceExperimentList(Resource):
    @api.marshal_list_with(experiment_instance)
    def get(self):
        '''List all instances of experiments'''

        configuration = setup_config()
        api_instance = client.CustomObjectsApi(client.ApiClient())
        return get_list_instances(api_instance, utils.MONITORED_NAMESPACE)

        # experiment_instance_list = populate_from("instances.txt")
        # return experiment_instance_list


def hide_https_tokens(text: str) -> str:
    """Inspects a string which may contain https://<TOKEN>@<url> and removes the TOKEN"""
    pattern = re.compile(r"https://([:A-Za-z0-9]+)([@][\S]+)")

    matches = list(pattern.finditer(text))
    if not matches:
        return text

    last_end = 0
    new_text = ""

    for m in matches:
        desensitized = '<hidden token>'.join(('https://', m.group(2)))
        new_text += text[last_end:m.start()]
        new_text += desensitized
        last_end = m.end()

    new_text += text[last_end:]

    return new_text


def flesh_out_workflow_instance_status(instance):
    """Updates the `status` field for a Workflow Instance based on information extracted from the primary pod"""
    status = instance.get('status', {}) or {}

    if not status.get('error-description', ''):
        # VV: The workflow does not have a proper error-description

        # VV: the pod-name matches the workflow name:
        pod_name = instance.get('k8s-labels', {}).get('workflow')
        namespace = utils.MONITORED_NAMESPACE

        core = client.CoreV1Api(client.ApiClient())

        try:
            pod = core.read_namespaced_pod(pod_name, namespace)  # type: client.models.V1Pod
            pod_status = pod.status  # type: client.models.V1PodStatus
        except kubernetes.client.exceptions.ApiException as e:
            if e.status == 404 and not status:
                instance['status'] = {'warning': "Workflow has no status and there is no Pod for it, "
                                                 "ask your administrator to inspect the status of "
                                                 "the workflow-operator pod."}
                current_app.logger.warning(f"Workflow {pod_name} has neither an associated Pod object "
                                           f"nor a status - workflow-operator may be malfunctioning")
                return
            current_app.logger.warning("Unable to get status of pod %s/%s - %s" % (namespace, pod_name, e))
            return
        except Exception as e:
            current_app.logger.warning("Unable to get status of pod %s/%s - %s" % (namespace, pod_name, e))
            return

        if not pod_status:
            current_app.logger.warning("Unable to get Status of pod %s/%s" % (namespace, pod_name))
            return

        if pod_status.conditions is None:
            current_app.logger.warning("Unable to get Conditions of pod %s/%s" % (namespace, pod_name))
            return

        # VV: If the last known condition of the pod is unschedulable, let the user know. They may be able to fix it
        status_conditions = [x for x in pod_status.conditions
                             if x.last_transition_time]  # type: List[client.models.V1PodCondition]
        gen_fail_status = {
            "current-stage": "",  # Empty string
            "exit-status": "",  # Empty string (no stage ever ran, so there's no exit-status to report)
            "experiment-state": "",
            "error-description": "",
            "meta": {},
            "stage-progress": 0.0,
            "stage-state": "",
            "stages": [],
            "total-progress": 0.0
        }

        init_cont_statuses = cast("List[client.models.V1ContainerStatus]", pod_status.init_container_statuses)

        if init_cont_statuses:
            # VV: Another scenario to look for is for when one of the init-containers fails
            failed = []
            for ics in init_cont_statuses:
                ics_state = cast("client.models.V1ContainerState", ics.state)
                term = cast("client.models.V1ContainerStateTerminated", ics_state.terminated)
                waiting = ics_state.waiting  # type: client.models.V1ContainerStateWaiting
                if waiting and (waiting.reason or '').lower() == 'errimagepull':
                    failed.append("failed to pull image %s" % ics.image)
                elif term and term.exit_code != 0:
                    try:
                        ic_stdout = core.read_namespaced_pod_log(
                            name=pod_name, container=ics.name, namespace=namespace)
                    except Exception:
                        current_app.logger.info("Unable to fetch stdout of %s in pod %s" % (ics.name, pod_name))
                        ic_stdout = 'unable to fetch stdout of %s' % ics.name
                    else:
                        ic_stdout = hide_https_tokens(ic_stdout)

                    failed.append("%s failed with exit code %s and message %s" % (
                        ics.name, term.exit_code, ic_stdout))
            if failed:
                gen_fail_status.update({
                    "experiment-state": "failed",
                    "error-description": "initContainer failure: %s" % '. '.join(failed),
                })
                status = gen_fail_status

        for cond in status_conditions:
            if ((cond.reason or '').lower() == 'unschedulable'
                    and (cond.status or '').lower() == 'false'):
                gen_fail_status.update({
                    "experiment-state": "unscheduled",
                    "error-description": "Workflow scheduler is unscheduled, "
                                         "reason: %s" % cond.message,
                })
                status = gen_fail_status
            elif ((cond.reason or '').lower() == 'containersnotready'
                  and (cond.status or '').lower() == 'false'):
                # VV: There is a chance that the pod is unable to pull its container images
                failed = []
                cont_statuses = cast("List[client.models.V1ContainerStatus]", [x for x in pod_status.container_statuses
                                                                               if x.state])
                for cs in cont_statuses:
                    waiting = cs.state.waiting  # type: client.models.V1ContainerStateWaiting
                    if waiting and (waiting.reason or '').lower() == 'imagepullbackoff':
                        failed.append(cs.image)
                if failed:
                    gen_fail_status.update({
                        "experiment-state": "unschedulable",
                        "error-description": "Unable to pull %s %s" % (
                            'image' if len(failed) == 1 else 'images', ', '.join(failed))})
                    status = gen_fail_status

        # VV: Finally inspect the status of the `elaunch-primary` container, the container may have attempted
        #     to use more resources than it requested (i.e. more Ram) forcing K8s to evict it
        elaunch_exit_code = None
        if pod_status.container_statuses and status.get('error-description', '') == '':
            cont_statuses = cast("List[client.models.V1ContainerStatus]", [
                x for x in pod_status.container_statuses
                if x.state and x.state.terminated and x.name == 'elaunch-primary'])

            if cont_statuses:
                term = cast("client.models.V1ContainerStateTerminated", cont_statuses[0].state.terminated)
                elaunch_exit_code = term.exit_code

                if term.exit_code != 0:
                    gen_fail_status.update({
                        "experiment-state": "Failed",
                        "error-description": "elaunch-primary exitCode: %s, reason: %s" % (
                            term.exit_code, term.reason)})
                    status = gen_fail_status

            if elaunch_exit_code == 0:
                # VV: elaunch has completed successfully; is workflow-monitoring still in the waiting state?
                # If that is the case then update `status` to reflect that the workflow instance is successful.

                cont_statuses = cast(List[client.models.V1ContainerStatus], [
                    x for x in pod_status.container_statuses
                    if x.state and x.name == 'monitor-elaunch-container'])

                if not cont_statuses:
                    # VV: Cannot make a decision based on the information at hand.
                    current_app.logger.warning("elaunch-primary in pod %s/%s has successfully terminated but "
                                               "monitor-elaunch-container has not had any state yet" % (
                                                   namespace, pod_name))
                    return

                mon_state = cont_statuses[0].state  # type: client.models.V1ContainerState

                # VV: Monitoring container is neither running, nor terminated; it's still in the waiting state, that
                #     is likely to be problematic - investigate the underlying reason
                if (mon_state.running is None) and (mon_state.terminated is None) and mon_state.waiting:
                    waiting = cast(client.models.V1ContainerStateWaiting, mon_state.waiting)
                    # VV: If the container is in the waiting state for any of the reasons bellow it will never
                    #     transition to the running state; it's safe to auto-generate a Success status for the instance
                    if (waiting.reason or '').lower() in [x.lower() for x in [
                        "ImagePullBackOff", "ImageInspectError", "ErrImagePull", "ErrImageNeverPull",
                        "RegistryUnavailable", "InvalidImageName"]]:
                        current_app.logger.warning("elaunch-primary in pod %s/%s has successfully terminated but "
                                                   "monitor-elaunch-container is stuck in waiting state %s - will "
                                                   "report experiment as successfully terminated" % (
                                                       namespace, pod_name, waiting.reason))

                        status = gen_fail_status
                        status['error-description'] = ''
                        status['exit-status'] = 'Success'
                        status['experiment-state'] = 'finished'
                        status['stage-progress'] = 1.0
                        status['total-progress'] = 1.0

    instance['status'] = status


@api.route('/<id>/', doc=False)
@api.route('/<id>')
@api.param('id', 'The instance identifier')
@api.response(404, 'Instance not found')
class Instance(Resource):
    _parser_properties = reqparse.RequestParser()
    _parser_properties.add_argument(
        "includeProperties",
        type=str,
        default=None,
        help='Comma separated columns found in the properties dataframe, or empty string, or `*` which is translated '
             'to "all columns in properties dataframe". Column query is case insensitive and returned DataFrame has '
             'columns with lowercase names Method silently discards columns that do not exist in DataFrame. When '
             'this argument exists, this method inserts',
        # VV: It's important to define this, otherwise you get an error:
        # GET /rs/instances/homo-lumo-dft-interface-eabpbk/properties b''
        #       b'{"message": "The browser (or proxy) sent a request that this server could not understand."}\n'
        location='args')
    _parser_properties.add_argument(
        'stringifyNaN',
        type=inputs.boolean,
        default=False,
        help='A boolean flag that allows converting NaN and infinite values to strings.',
        location='args'
    )

    @api.expect(_parser_properties)
    def get(self, id):
        '''Fetch an instance given its name'''
        try:
            instance = get_instance(id)

            if instance is None:
                api.abort(404, message=f"Instance {id} is not found", unknownRestUID=id)

            flesh_out_workflow_instance_status(instance)

            args = self._parser_properties.parse_args()
            comma_separated_names = args.includeProperties
            stringify_nan = args.stringifyNaN
            if comma_separated_names is not None:
                comma_separated_names = (comma_separated_names or '*').lower()

                meta = instance['status']['meta']
                instance_name = meta['instanceName']
                instance_dir = os.path.join(utils.WORKING_VOLUME_MOUNT, instance_name)
                dict_interface: Dict[str, Any] = instance['experiment']['metadata']['registry']['interface']

                # VV: The experiment definition contains the FlowIR interface. Howeve,r the interface does not contain
                # any information that the virtual experiment instance discovers at execution time, e.g.
                # inputs, additionalInputData etc. Here, we read the flowir_instance.yaml FlowIR and extract
                # the interface from it.
                try:
                    concrete_interface = load_interface_of_virtual_experiment_instance(instance_dir)
                    dict_interface.clear()
                    dict_interface.update(concrete_interface)
                except Exception as e:
                    current_app.logger.warning(f"Could not load instance due to {e}\n"
                                               f"Traceback: {traceback.format_exc()}")

                try:
                    properties = load_measured_properties_of_instance(instance_dir, comma_separated_names,
                                                                      stringify_nan)
                except FileNotFoundError:
                    if instance['status'].get('experiment-state', '').lower() in ["finished", "failed"]:
                        api.abort(404, message=f"Instance {id} has no properties",
                                  unknownProperties=comma_separated_names.split(','))
                    else:
                        api.abort(400, message=f"Instance {id} has no properties but has "
                                               f"not completed yet, try again later",
                                  propertiesNotAvailableYet=True)
                else:
                    dict_interface['propertyTable'] = properties
            return instance
        except werkzeug.exceptions.HTTPException:
            raise
        except Exception as e:
            current_app.logger.warning("Traceback: %s\nException: %s for %s" % (traceback.format_exc(), e, id))
            api.abort(500, message=f"Run into an error while returning instances/{id}")

    def delete(self, id):
        instance = get_instance(id)
        if instance is None:
            api.abort(400)

        api_instance = client.CustomObjectsApi(client.ApiClient())

        api_instance.delete_namespaced_custom_object(
            body=instance,
            group=utils.K8S_WORKFLOW_GROUP,
            version=utils.K8S_WORKFLOW_VERSION,
            plural=utils.K8S_WORKFLOW_PLURAL,
            namespace=utils.MONITORED_NAMESPACE,
            name=instance["name"])

        return "OK"


@api.route('/<id>/outputs/', doc=False)
@api.route('/<id>/outputs')
@api.param('id', 'The instance identifier')
@api.response(404, 'Instance not found')
class InstanceOutputs(Resource):
    def get(self, id):
        '''Fetch outputs of an instance given its identifier'''
        instance = get_instance(id)
        if instance is None:
            api.abort(404, message=f"Workflow instance {id} not found")
        return instance.get("outputs", {})


@api.route('/<id>/status/', doc=False)
@api.route('/<id>/status')
@api.param('id', 'The instance identifier')
@api.response(404, 'Instance not found')
class InstanceStatus(Resource):
    def get(self, id):
        '''Fetch status of an instance given its identifier

        The status field contains:
        - `exit-status`: The status of the completed experiment. This receives its final value *after* `experiment-state` transitions to either `finished` or `failed`. Value is one of `["Success", "Failed", "Stopped", ""]`. Value may be empty while Kubernetes initializes opbjects.
        - `experiment-state`: Indicates the state of the orchestrator that is executing the experiment tasks. **Not** to be confused with status of experiment (`exit-status`). For example, an experiment status can have `experiment-state=finished` with `exit-status=failed`. This means that the experiment completed with a failure. The value of `experiment-state` is one of `["unscheduled", "running", "finished", "failed", "unschedulable", ""]`. Value may be empty while Kubernetes initializes opbjects.
        - `stage-state`: Indicates the state of the active stage in the experiment with the lowest stage index. Value is one of `["Initializing", "finished", "waiting_on_resource","running", "component_shutdown", "failed"]`
        - `error-description`: A string, which when printed is a human readable description that explains why `exit-status` is `Failed`.
        - `total-progress`: A number in [0.0, 1.0] indicating the progress of the experiment. Note that workflow developers may decide to control this value.
        - `stage-progress`: A number in [0.0, 1.0] indicating the progress of the active stage with the lowest stage index. Note that workflow developers may decide to control this value.
        - `stages`: A list of human-readable stage names
        - `current-stage`: UID of stage (e.g. `stage0`)
        - `meta`: This is a nested dictionary
          - `arguments`: The command-line of the orchestrator
          - `data`: The list of files that override data files
          - `input`: The list of input files
          - `pid`: The process ID of the st4sd orchestrator
          - `platform`: The name of the platform that the virtual experiment instance executes
          - `userVariables`: User provided variables, the schema is  `{'global':{name:value}, 'stages':{index:{name:value}}}`
          - `variables`: Global and stage variables active in the `platform`-scope that the virtual experiment executes. The schema is `{'global':{name:value}, 'stages':{index:{name:value}}}`
          - `hybridPlatform`: Name of hybrid-platform for communicating with LSF (can be None),
          - `userMetadata`: A dictionary with `key(str): Any` value pairs that users can provide
          - `instanceName`: The name of the directory containing the virtual experiment instance.
          - `version`: The version of the st4sd orchestrator
        '''
        instance = get_instance(id)
        if instance is None:
            api.abort(404, message=f"Workflow instance {id} not found")

        if instance.get('status', None) is None:
            api.abort(400, message=f"Workflow instance {id} does not have a status yet")

        flesh_out_workflow_instance_status(instance)
        return instance['status']


def load_interface_of_virtual_experiment_instance(
        instance_dir: str) -> experiment.model.frontends.flowir.DictFlowIRInterface:
    path = os.path.join(instance_dir, 'conf', 'flowir_instance.yaml')
    return yaml.load(open(path, 'r'), Loader=yaml.Loader).get('interface', {})


def load_measured_properties_of_instance(instance_dir: str, comma_separated_names: str, stringify_nan: bool) -> Dict[
    str, Any]:
    """Reads the properties of an instance and returns a Dictionary representation of the dataframe

    Args:
        comma_separated_names: Comma separated columns found in the properties dataframe, or empty string, or `*`
            which is translated to "all columns in properties dataframe". Column query is case insensitive and
            returned DataFrame has columns with lowercase names.
            Method silently discards columns that do not exist in DataFrame.
        stringify_nan: A boolean flag that allows converting NaN and infinite values to strings.
        instance_dir: Path to instance dir

    Returns:
        A dictionary representation of a pandas.DataFrame

    Raises:
        NotFoundError: If properties file does not exist
    """
    column_names = None  # VV: None -> we want everything

    if comma_separated_names != "*":
        names: List[str] = [x for x in comma_separated_names.split(",") if x]
        # VV: Throw away duplicates
        column_names = []
        for x in names:
            if x not in column_names:
                column_names.append(x)

        if 'input-id' not in column_names:
            column_names.insert(0, "input-id")

    path = os.path.join(instance_dir, "output", "properties.csv")
    # VV: This is how experiment.service.db.Mongo._kernel_getDocument() reads the properties file
    df: pandas.DataFrame = pandas.read_csv(path, sep=None, engine="python")

    if column_names:
        columns = [x for x in column_names if x in df.columns]
        df = df[columns]

    if stringify_nan:
        df.fillna('NaN', inplace=True)

    return df.to_dict(orient="list")


@api.route('/<id>/properties/', doc=False)
@api.route('/<id>/properties')
@api.param('id', 'The instance identifier')
@api.response(400, 'Instance has not finished measuring properties yet, or there was a problem loading the properties')
@api.response(404, 'Instance not found')
class InstanceProperties(Resource):
    _parser_properties = reqparse.RequestParser()
    _parser_properties.add_argument(
        "includeProperties",
        type=str,
        default="*",
        help='Comma separated columns found in the properties dataframe, or empty string, or `*` which is translated '
             'to "all columns in properties dataframe". Column query is case insensitive and returned DataFrame has '
             'columns with lowercase names Method silently discards columns that do not exist in DataFrame.',
        # VV: It's important to define this, otherwise you get an error:
        # GET /rs/instances/homo-lumo-dft-interface-eabpbk/properties b''
        #       b'{"message": "The browser (or proxy) sent a request that this server could not understand."}\n'
        location='args')
    _parser_properties.add_argument(
        'stringifyNaN',
        type=inputs.boolean,
        default=False,
        help='A boolean flag that allows converting NaN and infinite values to strings.',
        location='args'
    )

    @api.expect(_parser_properties)
    def get(self, id):
        '''Fetch columns from the measured properties of an instance given its identifier.
        '''
        try:
            instance = get_instance(id)

            if instance is None:
                api.abort(404, message=f"Instance {id} not found")

            if 'status' not in instance:
                api.abort(400, message=f"Instance {id} does not have a status field")

            meta = instance['status']['meta']
            instance_name = meta['instanceName']
            instance_dir = os.path.join(utils.WORKING_VOLUME_MOUNT, instance_name)

            args = self._parser_properties.parse_args()
            comma_separated_names = (args.includeProperties or "*").lower()
            stringify_nan = args.stringifyNaN

            try:
                return load_measured_properties_of_instance(instance_dir, comma_separated_names, stringify_nan)
            except FileNotFoundError:
                if instance['status'].get('experiment-state', '').lower() in ["finished", "failed"]:
                    api.abort(404, message=f"Instance {id} has no properties",
                              unknownProperties=comma_separated_names.split(','))
                else:
                    api.abort(400, message=f"Instance {id} has no properties but has "
                                           f"not completed yet, try again later",
                              propertiesNotAvailableYet=True)
        # VV: This is to catch any api.abort() above and re-raise it
        except werkzeug.exceptions.HTTPException:
            raise
        except Exception as e:
            current_app.logger.warning("Traceback: %s\nException: %s for %s" % (traceback.format_exc(), e, id))
            api.abort(500, message=f"Run into an error while extracting the properties of {id}")


@api.route('/<id>/outputs/<path:key>/', doc=False)
@api.route('/<id>/outputs/<path:key>')
@api.param('id', 'The instance identifier')
@api.param('key', 'The output key identifier')
@api.response(500, 'Internal exception while handling request')
@api.response(404, 'Instance OR key-output OR output-path not found (look for field unknownInstance OR '
                   'outputsReady OR '
                   'missingPath to differentiate)')
@api.response(400, 'Treated a file output as a folder OR asked for a file that is contained outside the tree '
                   'of a folder key-output (look for field isFolder OR '
                   'fileInFolder to differentiate)')
class InstanceOutputsKey(Resource):
    def get(self, id, key):
        '''Fetch outputs of an instance given its identifier'''
        try:
            instance = get_instance(id)
            if instance is None:
                api.abort(404, "unknown workflow instance %s" % id, unknownInstanceId=id)
            try:
                outputs = instance['outputs']
            except KeyError:
                api.abort(404, "Workflow instance %s has not produced any outputs yet" % id, outputsReady=False)

            if '/' in key:
                key, key_path = key.split('/', 1)
            else:
                key_path = None

            try:
                key_output = outputs[key]
            except KeyError:
                api.abort(404, "Workflow instance %s has not produced key-output %s yet" % (id, key), unknownOutput=key)

            filepath = key_output['filepath']
            if filepath.endswith('/'):
                filepath = filepath[:-1]
            meta = instance['status']['meta']
            instance_name = meta['instanceName']

            if filepath.startswith('/'):
                # VV: This workflow object is an old one ... this workflow instance can be hard to move around,
                # new workflow objects contain paths relative to the instance dir
                output_path = filepath
            else:
                output_path = os.path.join(WORKING_VOLUME_MOUNT, instance_name, filepath)

            output_path = os.path.realpath(output_path)

            if key_path:
                if os.path.isdir(output_path) is False:
                    api.abort(400, "Key-output %s of workflow instance %s is not a folder and cannot be used to "
                                   "retrieve child path %s" % (key_output, id, key_path), isFolder=False)
                path = os.path.realpath(os.path.join(output_path, key_path))

                relpath = os.path.relpath(path, output_path)
                if '/../' in relpath:
                    api.abort(400, "Key-output %s of workflow instance %s is a folder, but child path %s does not"
                                   "belong to file tree of key-output", fileInFolder=False)
            else:
                path = output_path

            filename = os.path.basename(path)

            if os.path.isfile(path):
                _, ext = os.path.splitext(path)
                if ext.lower() == '.zip':
                    app_type = '.zip'
                else:
                    app_type = 'octet-stream'
                response = Response(apis.storage.IterableStreamZipOfDirectory.iter_file(path),
                                    mimetype='application/%s' % app_type)
                response.headers['Content-Disposition'] = 'attachment; filename=\"%s\"' % filename
                return response
            elif os.path.isdir(path):
                response = Response(apis.storage.IterableStreamZipOfDirectory(path), mimetype='application/zip')
                response.headers['Content-Disposition'] = 'attachment; filename=\"%s.zip\"' % filename
                return response
            else:
                api.abort(404, "Output %s of workflow instance %s references non-existing file %s" % (
                    key, id, path), missingPath=path)
        except werkzeug.exceptions.HTTPException:
            raise
        except Exception as e:
            current_app.logger.warning("Traceback: %s\nException: %s for %s" % (traceback.format_exc(), e, id))
            api.abort(500, "Exception when getting output %s for workflow instance %s" % (key, id))
