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
8. The Graph DSL is stored under ${S3_ROOT_LIBRARY}/${graphName}/dsl.yaml

"""
import pathlib
import typing

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
            library_path = apis.models.constants.S3_ROOT_LIBRARY

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
            contents = yaml.safe_dump(namespace.dict(exclude_none=True, by_alias=True)).encode()
            self.actuator.write(path, contents)
        except (apis.models.errors.StorageError, apis.models.errors.LibraryError):
            raise
        except Exception as e:
            raise apis.models.errors.StorageError(f"Unable to store Graph under {path} due to {type(e)} {e}")

        return namespace

    def get(self, name: str) -> Entry:
        """Returns a Graph from the library

        Args:
            name:
                The name of the graph

        Returns:
            The corresponding entry in the Graph Library

        Raises:
            apis.models.errors.StorageError:
                If there is an issue accessing the Storage Actuator
            apis.models.errors.GraphDoesNotExistError:
                If the graph does not exist
        """

        path = self._graph_path(name)

        try:
            return Entry(graph=yaml.safe_load(self.actuator.read(path)))
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
            errors = [dict(x) for x in e.errors()]
            for x in errors:
                try:
                    message = x.pop('msg')
                except KeyError:
                    continue
                x['message'] = message
            raise apis.models.errors.InvalidModelError(
                "Invalid graph", problems=errors
            )
        except Exception as e:
            raise apis.models.errors.InvalidModelError(
                "Invalid graph", problems=[{"message": f"Unexpected validation error: {e}"}]
            )

        errors = []

        if len(namespace.workflows) == 0:
            errors.append({"message": "There must be at least 1 workflow template"})
        if len(namespace.components) == 0:
            errors.append({"message": "There must be at least 1 component template"})
        if len(namespace.entrypoint.execute) != 1:
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
        scopes = experiment.model.frontends.dsl.ScopeStack()

        try:
            # VV: Discover all reachable templates. If there are no errors then the Graph is good enough
            scopes.discover_all_instances_of_templates(namespace, override_entrypoint_args=auto_args)
        except experiment.model.errors.DSLInvalidError as e:
            raise apis.models.errors.InvalidModelError("Invalid graph", problems=[{
                "message": str(exc.underlying_error), "location": exc.location
            } for exc in e.underlying_errors])

        return namespace
