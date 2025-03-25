# Copyright IBM Inc. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
# Authors:
#   Vassilis Vassiliadis

"""This file contains the  implementation of a Graph Library which is managed/accessed via one of the Storage Actuators

Each graph in the library:

1. Contains at least 1 workflow. One of which is **must** be the entry-instance
2. Contains at least 1 component. A component **cannot** be the entry-instance
3. If the entrypoint is missing, then there **must** be exactly 1 workflow templates in the graph. The entrypoint
   is auto-generated to point to the only workflow template
4. The entrypoint need not contain any arguments for the parameters of the entry workflow
5. All templates that are reachable from the entrypoint workflow **must** be valid
6. The name of the graph, is the name of the entry workflow
7. There **must not** be an existing graph with the same name in the library
8. The Graph DSL is stored under ${S3_ROOT_GRAPH_LIBRARY}/${graphName}/dsl.yaml

"""
import pathlib
import typing
import copy

import pydantic
import yaml

import apis.storage.actuators
import apis.models.constants
import apis.models.errors

import experiment.model.frontends.dsl
import experiment.model.errors


class Entry(typing.NamedTuple):
    """An entry of the Graph Library

    In the future we might add support for auxiliary files that the component templates in the graph need.
    """
    graph: typing.Dict[str, typing.Any]


class LibraryClient:
    def __init__(
        self,
        actuator: apis.storage.actuators.Storage,
        library_path: typing.Optional[typing.Union[str, pathlib.Path]] = None
    ):
        self.actuator = actuator

        if library_path is None:
            library_path = apis.models.constants.S3_ROOT_GRAPH_LIBRARY

        if not isinstance(library_path, pathlib.Path):
            library_path = pathlib.Path(library_path)

        self.library_path = library_path

    def _graph_dir_path(self, name: str) -> pathlib.Path:
        return self.library_path / name

    def _graph_path(self, name: str) -> pathlib.Path:
        return self._graph_dir_path(name) / "dsl.yaml"

    def add(self, entry: Entry) -> experiment.model.frontends.dsl.Namespace:
        """Validates then adds valid graphs to the Library

        Args:
            entry:
                The graph to add to the library. The contents of the dictionary may be modified

        Returns:
            The Namespace representation of the graph that was added to the Library

        Raises:
            apis.models.errors.InvalidModelError:
                If the graph is invalid
            apis.models.errors.GraphAlreadyExistsError:
                If there is an existing graph with the same name
            apis.models.errors.StorageError:
                If there is an issue accessing the Storage Actuator
        """
        namespace = self.validate(entry)
        graph_name = namespace.entrypoint.entryInstance
        path = self._graph_path(graph_name)

        try:
            try:
                if self.actuator.isfile(path):
                    raise apis.models.errors.GraphAlreadyExistsError(graph_name)
            except FileNotFoundError:
                pass
            contents = yaml.safe_dump(namespace.model_dump(by_alias=True, exclude_unset=True)).encode()
            self.actuator.write(path, contents)
        except (apis.models.errors.StorageError, apis.models.errors.LibraryError):
            raise
        except Exception as e:
            raise apis.models.errors.StorageError(f"Unable to store Graph under {path} due to {type(e)} {e}")

        return namespace

    def get(
        self, name: str,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> Entry:
        """Returns a Graph from the library

        Args:
            name:
                The name of the graph
            exclude_unset:
                Whether to exclude fields that are unset or None from the output.
            exclude_defaults:
                Whether to exclude fields that are set to their default value from the output.
            exclude_none:
                Whether to exclude fields that have a value of `None` from the output.

        Returns:
            The corresponding entry in the Graph Library

        Raises:
            apis.models.errors.StorageError:
                If there is an issue accessing the Storage Actuator
            apis.models.errors.GraphDoesNotExistError:
                If the graph does not exist
        """

        path = self._graph_path(name)
        original = None # VV: keep linter happy

        try:
            original = self.actuator.read(path)
            graph = yaml.safe_load(original)
            graph = experiment.model.frontends.dsl.Namespace(**graph).model_dump(
                exclude_unset=exclude_unset,
                exclude_none=exclude_none,
                exclude_defaults=exclude_defaults,
                by_alias=True,
            )
            return Entry(graph=graph)
        except pydantic.ValidationError as e:
            errors = apis.models.errors.make_pydantic_errors_jsonable(e)
            errors.append({
                "location": [],
                "message": yaml.safe_dump(original)
            })
            raise apis.models.errors.InvalidModelError(
                "Invalid graph, you must delete it", problems=errors
            )
        except apis.models.errors.StorageError:
            raise
        except FileNotFoundError:
            raise apis.models.errors.GraphDoesNotExistError(name)
        except Exception as e:
            raise apis.models.errors.StorageError(f"Unable to get Graph under {path} due to {e}")

    def delete(self, name: str):
        """Deletes a Graph from the library

        Args:
            name:
                The name of the graph
        Raises:
            apis.models.errors.StorageError:
                If there is an issue accessing the Storage Actuator
            apis.models.errors.GraphDoesNotExistError:
                If the graph does not exist
        """

        path = self._graph_dir_path(name)
        path = self.actuator.as_posix(path) + "/"

        try:
            self.actuator.remove(path)
        except apis.models.errors.StorageError:
            raise
        except FileNotFoundError:
            raise apis.models.errors.GraphDoesNotExistError(name)
        except Exception as e:
            raise apis.models.errors.StorageError(f"Unable to delete Graph in {path} due to {e}")


    def list(self) -> typing.List[str]:
        """Returns a list of available Graphs in the library

        Raises:
            apis.models.errors.StorageError:
                If there is an issue accessing the Storage Actuator
        """
        try:
            return [x.name for x in self.actuator.listdir(self.library_path) if x.isdir]
        except apis.models.errors.StorageError:
            raise
        except NotADirectoryError:
            raise apis.models.errors.StorageError(
                f"The library path {self.library_path} points to a file instead of a directory"
            )
        except FileNotFoundError:
            return []
        except Exception as e:
            raise apis.models.errors.StorageError(f"Unable to list graphs due to {type(e)}: {e}")

    @classmethod
    def _preprocess_workflows(cls, namespace: experiment.model.frontends.dsl.Namespace):
        """Utility method to auto-generate a wrapper workflow to the unique component of an entry which has no
        workflows

        Auto-generation:
        - if there is exactly 1 component called X and no workflows then
            - rename the component X-wrapped
            - auto-generate a workflow called X
            - copy the parameters of X-wrapped to X
            - invoke X-wrapped inside X
            - auto-generate the entrypoint

        Args:
            namespace:
                The graph to preprocess, may change in place
        """
        if len(namespace.workflows) != 0:
            return

        if len(namespace.components) != 1:
            return

        workflow_name = namespace.components[0].signature.name

        workflow = experiment.model.frontends.dsl.Workflow(
            signature=namespace.components[0].signature.model_dump(),
            steps= {
                f"{workflow_name}-wrapped": f"{workflow_name}-wrapped"
            },
            execute=[
                experiment.model.frontends.dsl.ExecuteStep(
                    target=f"<{workflow_name}-wrapped>",
                    args={
                        p.name: f"%({p.name})s" for p in namespace.components[0].signature.parameters
                    }
                )
            ]
        )

        namespace.components[0].signature.name = f"{workflow_name}-wrapped"
        namespace.workflows.append(workflow)

        namespace.model_fields_set.add("workflows")

        # VV: if there's no entrypoint auto generate. If there is one already then by definition it must be
        # pointing to the Component. Now, the Workflow has the original name of the component and therefore the
        # entrypoint is still valid
        if not namespace.entrypoint:
            namespace.entrypoint = experiment.model.frontends.dsl.Entrypoint(
                execute=[
                    experiment.model.frontends.dsl.ExecuteStepEntryInstance(
                        target=f"<entry-instance>",
                        args={}
                    )
                ],
                **{
                    "entry-instance": workflow_name
                }
            )
            namespace.model_fields_set.add("entrypoint")

    @classmethod
    def _preprocess_entrypoint(cls, entry: Entry):
        """Utility method to auto-generate the entrypoint of a Graph and preprocess it

        Auto-generation:
        - if there is exactly 1 workflow then that's the entrypoint template

        Preprocess:
        - If the entrypoint contains arguments, remove them

        Args:
            entry:
                The graph to test. The definition of the graph may be modified
        """
        graph = entry.graph

        if (
            isinstance(graph, dict)
            and "entrypoint" not in graph
            and isinstance(graph.get("workflows", []), list)
            and len(graph.get("workflows", [])) == 1
            and isinstance(graph["workflows"][0], dict)
            and isinstance(graph["workflows"][0].get("signature"), dict)
            and isinstance(graph["workflows"][0]["signature"].get("name"), str)
        ):
            graph["entrypoint"] = {
                "entry-instance": graph["workflows"][0]["signature"]["name"],
                "execute": [
                    {
                        "target": "<entry-instance>",
                        "args": {}
                    }
                ]
            }

        if (
            isinstance(graph, dict)
            and isinstance(graph.get("entrypoint"), dict)
            and isinstance(graph["entrypoint"].get("execute"), list)
            and len(graph["entrypoint"]["execute"]) == 1
            and isinstance(graph['entrypoint']['execute'][0], dict)
        ):
            graph['entrypoint']['execute'][0]['args'] = {}
        elif (
            isinstance(graph, dict)
            and isinstance(graph.get("entrypoint"), dict)
            and 'execute' not in graph['entrypoint']
        ):
            graph['entrypoint']['execute'] = [
                {
                    "target": "<entry-instance>",
                    "args": {}
                }
            ]

    @classmethod
    def validate(cls, entry: Entry) -> experiment.model.frontends.dsl.Namespace:
        """Tests whether a Graph Library entry is valid

        Args:
            entry:
                The graph to test. The graph definition may be modified

        Returns:
            The Namespace representation of the graph

        Raises:
            apis.models.errors.InvalidModelError:
                If the graph is invalid
        """
        if not isinstance(entry, Entry):
            raise apis.models.errors.InvalidModelError(
                "Invalid graph", problems=[
                    {"message": f"Unexpected type of parameter to validate() {type(entry)}"}]
            )

        cls._preprocess_entrypoint(entry)

        try:
            namespace = experiment.model.frontends.dsl.Namespace(**entry.graph)
        except pydantic.ValidationError as e:
            errors = apis.models.errors.make_pydantic_errors_jsonable(e)
            raise apis.models.errors.InvalidModelError(
                "Invalid graph", problems=errors
            )
        except Exception as e:
            raise apis.models.errors.InvalidModelError(
                "Invalid graph", problems=[{"message": f"Unexpected validation error: {e}"}]
            )

        errors = []
        if len(namespace.components) == 0:
            errors.append({"message": "There must be at least 1 component template"})

        cls._preprocess_workflows(namespace)

        if len(namespace.workflows) == 0:
            errors.append({"message": "There must be at least 1 workflow template"})

        if not namespace.entrypoint or len(namespace.entrypoint.execute) != 1:
            errors.append({"message": "Missing entrypoint workflow template"})

        if len(errors):
            raise apis.models.errors.InvalidModelError("Invalid graph", problems=errors)

        entry_target = namespace.entrypoint.entryInstance
        try:
            entry_template = namespace.get_template(entry_target)
        except KeyError:
            raise apis.models.errors.InvalidModelError(
                "Invalid graph",
                problems=[{"message": f"The entrypoint points to an unknown template {entry_target}"}],
            )

        if not isinstance(entry_template, experiment.model.frontends.dsl.Workflow):
            raise apis.models.errors.InvalidModelError(
                "Invalid graph",
                problems=[{"message": f"The entrypoint must to an Workflow template but {entry_target} is a "
                                      f"{type(entry_template).__name__}"}],
            )

        errors = []

        if len(entry_template.execute) == 0:
            errors.append({"message": f"The entry workflow template must execute at least 1 steps"})

        if len(entry_template.steps) == 0:
            errors.append({"message": f"The entry workflow template must have at least 1 steps"})

        if len(errors):
            raise apis.models.errors.InvalidModelError("Invalid graph", problems=errors)

        # VV: We cannot validate the FlowIR because we don't know what the parameters of the entrypoint workflow
        # are pointing at. For example, a component can have 2 parameters one which it references in its
        # command.arguments and a second one that it references in its workflowAttributes.replicate.
        # The 1st parameter could receive any value (including OutputReference and legacy DataReference) but the
        # 2nd one **must** be an integer or `None`. Without knowing exactly how parameters of the entrypoint
        # propagate to leaves we cannot auto-generate default values for the entrypoint of the Graph template.
        # The next best thing we can do is just produce fake values for all parameters of the workflow then just
        # visit all nodes that are reachable from the entrypoint using the `ScopeStack` class.

        # VV: First, auto-generate fake values for parameters of the template that the entrypoint points to
        auto_args = {
            p.name: "dummy-value" for p in entry_template.signature.parameters if p.default is None
        }

        try:
            # VV: Discover all reachable templates. If there are no errors then the Graph is good enough
            experiment.model.frontends.dsl.lightweight_validate(
                namespace=namespace,
                override_entrypoint_args=auto_args
            )
        except experiment.model.errors.DSLInvalidError as e:
            errors = apis.models.errors.make_pydantic_errors_jsonable(e)
            raise apis.models.errors.InvalidModelError("Invalid graph", problems=errors)

        # VV: Finally, make it so the 1st workflow is the one that the entrypoint points to

        if len(namespace.workflows) > 1:
            # VV: enumerate messes up type-hints here ...
            for i in range(len(namespace.workflows)):
                wf = namespace.workflows[i]
                if wf.signature.name == entry_template.signature.name:
                    if i == 0:
                        break
                    else:
                        namespace.workflows[0], namespace.workflows[i] = namespace.workflows[i], namespace.workflows[0]
                        break

        return namespace
