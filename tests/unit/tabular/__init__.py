import importlib
import pathlib
from copy import deepcopy
from dataclasses import dataclass
from typing import Optional

from fhir.resources.fhirresourcemodel import FHIRResourceModel
from flatten_json import flatten, unflatten_list
from deepmerge import Merger

from pydantic import BaseModel, ValidationError

FHIR_CLASSES = importlib.import_module('fhir.resources')

DEFAULT_MERGER = Merger(
    # pass in a list of tuple, with the
    # strategies you are looking to apply
    # to each type.
    [
        (list, ["override"]),
        (dict, ["merge"]),
        (set, ["union"])
    ],
    # next, choose the fallback strategies,
    # applied to all other types:
    ["override"],
    # finally, choose the strategies in
    # the case where the types conflict:
    ["override"]
)


# we use pydantic BaseModel to inherit serialization and validation
class Config(BaseModel):
    """Mock config for testing."""

    default: dict[str, list[str]]
    """Default config for all resources. key is resourceType, value is list of fields to keep."""

    programs: Optional[dict[str, dict[str, list[str]]]] = None
    """Default config for program. key is `program`, value is config for program."""

    projects: Optional[dict[str, dict[str, list[str]]]] = None
    """Default config for project. key is `program-project`, value is config for projects."""

    def get_config(self, project_id: str) -> dict[str, list[str]]:
        """Get config for project."""
        if self.projects and project_id in self.projects:
            return self.projects[project_id]
        elif self.programs and project_id.split('-')[0] in self.programs:
            return self.programs[project_id.split('-')[0]]
        else:
            return self.default


@dataclass
class ParseResult():

    """Results of FHIR validation of dict."""
    object: dict
    """The "raw" dictionary loaded from json string."""
    resource: Optional[FHIRResourceModel] = None
    """If valid, the FHIR resource."""
    exception: Optional[Exception] = None
    """If invalid, the exception."""
    path: Optional[pathlib.Path] = None
    """Source file, if available."""
    offset: Optional[int] = None
    """Base 0 offset of line number(ndjson) or entry(bundle)."""
    resource_id: Optional[str] = None
    """Resource id of resource"""


def mock_to_tabular(resource: dict, config: Config, project_id: str) -> dict:
    """Convert a resource to a tabular representation. (mock for testing)

    Args:
        resource (dict): A resource in JSON format.
        config (Config): A configuration object.
        project_id (str): The project ID.

    Returns:
        dict: A tabular representation of the resource.
    """

    assert 'resourceType' in resource, 'resource must have resourceType'

    project_config = config.get_config(project_id)
    assert resource['resourceType'] in project_config, f"resourceType {resource['resourceType']} not in config for project {project_id}"

    keep = project_config[resource['resourceType']]

    flattened = flatten(resource)

    return {k: flattened[k] for k in keep}


def mock_from_tabular(tabular: dict, config: Config, project_id: str, resource: dict) -> dict:
    """Convert a tabular representation of a resource to a resource. (mock for testing)

    Args:
        tabular (dict): A tabular (edited) representation of a resource.
        config (Config): A configuration object.
        project_id (str): The project ID.
        resource (immutable dict): The original resource.

    Returns:
        dict: A merged resource in JSON format.
    """

    assert 'resourceType' in resource, 'resource must have resourceType'

    project_config = config.get_config(project_id)
    assert resource['resourceType'] in project_config, f"resourceType {resource['resourceType']} not in config for project {project_id}"

    keep = project_config[resource['resourceType']]
    assert set(tabular.keys()).issubset(set(keep)), f"tabular representation is missing fields {set(keep) - set(tabular.keys())}"

    unflattened = unflatten_list(tabular)

    return DEFAULT_MERGER.merge(deepcopy(resource), unflattened)


def mock_validate(resource_dict: dict) -> ParseResult:
    """Validate a resource. (mock for testing)

    Args:
        resource_dict (dict): A resource in JSON format.

    Returns:
        ParseResult: The validated resource.
    """

    try:
        assert 'resourceType' in resource_dict, "resource_dict missing `resourceType`"
        klass = FHIR_CLASSES.get_fhir_model_class(resource_dict['resourceType'])
        _ = klass.parse_obj(resource_dict)
        klass.validate(_)
        return ParseResult(object=resource_dict, resource=_, resource_id=_.id)
    except (ValidationError, AssertionError) as e:
        return ParseResult(object=resource_dict, exception=e, resource_id=resource_dict.get('id', None))
