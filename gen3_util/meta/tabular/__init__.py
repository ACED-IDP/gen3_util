import importlib
import json
import pathlib
from copy import deepcopy
from dataclasses import dataclass
from random import random
from typing import Optional, Iterator, Generator

import pandas as pd
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

    def get_config(self, project_id: str = None) -> dict[str, list[str]]:
        """Get config for project."""
        if not project_id:
            return self.default
        elif self.projects and project_id in self.projects:
            return self.projects[project_id]
        elif self.programs and project_id.split('-')[0] in self.programs:
            return self.programs[project_id.split('-')[0]]
        else:
            return self.default


@dataclass
class ParseResult:

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


def to_tabular(resource: dict, config: Config, project_id: str = None) -> dict:
    """Convert a resource to a tabular representation.

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

    return {k: flattened[k] for k in keep if k in flattened}


def from_tabular(tabular: dict, config: Config, project_id: str, resource: dict) -> dict:
    """Convert a tabular representation of a resource to a resource.

    Args:
        tabular (dict): A tabular (edited) representation of a resource.
        config (Config): A configuration object.
        project_id (str): The project ID.
        resource (immutable dict): The original resource.

    Returns:
        dict: A merged resource in JSON format.
    """

    assert 'resourceType' in resource, f'resource must have resourceType {resource}'

    project_config = config.get_config(project_id)
    assert resource['resourceType'] in project_config, f"resourceType {resource['resourceType']} not in config for project {project_id}"

    keep = project_config[resource['resourceType']]
    assert set(tabular.keys()).issubset(set(keep)), f"tabular representation is missing fields {set(keep) - set(tabular.keys())}"

    unflattened = unflatten_list(tabular)

    return DEFAULT_MERGER.merge(deepcopy(resource), unflattened)


def validate(resource_dict: dict) -> ParseResult:
    """Validate a resource.

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


def default_columns(resource: [Iterator[dict] or dict], sample: bool = True, sample_size: int = 100) -> list[str]:
    """Default columns for all resources.
    Takes either a iterator for a list of resources or a single resource.

    By default, uses sampling method to determine default columns.
    See https://gregable.com/2007/10/reservoir-sampling.html
    Returns:
        list[str]: Default columns for all resources.
    """
    if not isinstance(resource, list):
        resource = [resource]

    columns = set()
    sampled = None
    if sample:
        sampled = []
    for i, line in enumerate(resource):
        if 'resourceType' not in line:
            raise KeyError(f"resource missing `resourceType` {type(line)}")
        if not sample:
            columns.update(flatten(line).keys())
        else:
            if i < sample_size:
                sampled.append(line)
            elif i >= sample_size and random.random() < sample_size / float(i + 1):
                replace = random.randint(0, len(sampled) - 1)
                sampled[replace] = line

    if sampled:
        for line in sampled:
            columns.update(flatten(line).keys())

    # filter and order columns

    def filter_keys(keys: list[str]) -> list[str]:
        """Remove keys that start with _ or meta."""
        keys = [_ for _ in keys if not _.startswith('_')]
        keys = [_ for _ in keys if not _.startswith('meta')]
        return keys

    def order_keys(keys: list[str]) -> list[str]:
        """Order keys by resourceType, id, then scalar, then nested, then references."""
        ordered_columns = ['resourceType', 'id']
        keys.remove('resourceType')
        keys.remove('id')
        # scalar
        scalar = [_ for _ in keys if '_' not in _]
        ordered_columns.extend(sorted(scalar))
        keys = [_ for _ in keys if '_' in _]
        # references
        references = [_ for _ in keys if _.endswith('_reference')]
        nested = [_ for _ in keys if not _.endswith('_reference')]
        ordered_columns.extend(sorted(nested))
        ordered_columns.extend(sorted(references))
        return ordered_columns

    columns = order_keys(filter_keys(columns))
    return columns


def transform_dir_to_tabular(meta_data_path: str, tabular_data_path: str, file_type: str) -> Generator[str, None, None]:
    """Convert FHIR to tabular format, yields log messages"""

    meta_data_path = pathlib.Path(meta_data_path)
    assert meta_data_path.exists(), f"{meta_data_path} does not exist"
    assert meta_data_path.is_dir(), f"{meta_data_path} is not a directory"

    tabular_data_path = pathlib.Path(tabular_data_path)
    if not tabular_data_path.exists():
        tabular_data_path.mkdir(parents=True)
    assert tabular_data_path.is_dir(), f"{tabular_data_path} is not a directory"

    for file_name in meta_data_path.glob("*.ndjson"):

        with file_name.open() as fp:
            for line in fp:
                line = json.loads(line)
                resource_type = line['resourceType']
                break

        with file_name.open() as fp:
            config = Config(default={resource_type: default_columns([json.loads(line) for line in fp])})

        with file_name.open() as fp:
            create_file_from_dict_iterator(
                data_iterator=[to_tabular(json.loads(line), config) for line in fp],
                file_path=tabular_data_path / f"{resource_type}.{file_type}",
                file_type=file_type
            )
            yield f"{tabular_data_path / f'{resource_type}.{file_type}'}"


def transform_dir_from_tabular(meta_data_path, tabular_data_path) -> Generator[str, None, None]:
    """Convert tabular to FHIR format"""
    tabular_data_path = pathlib.Path(tabular_data_path)
    assert tabular_data_path.exists(), f"{tabular_data_path} does not exist"
    assert tabular_data_path.is_dir(), f"{tabular_data_path} is not a directory"

    meta_data_path = pathlib.Path(meta_data_path)
    if not meta_data_path.exists():
        meta_data_path.mkdir(parents=True)
    assert meta_data_path.is_dir(), f"{meta_data_path} is not a directory"

    # Glob for TSV files
    for tsv_file in tabular_data_path.glob('*.tsv'):
        # Perform operations on TSV files
        df = read_file_into_dataframe(tsv_file, file_type='tsv')
        yield write_df_to_ndjson(df, meta_data_path)

    # Glob for Excel files
    for excel_file in tabular_data_path.glob('*.xlsx'):
        # Perform operations on Excel files
        df = read_file_into_dataframe(excel_file, file_type='excel')
        yield write_df_to_ndjson(df, meta_data_path)


def write_df_to_ndjson(df, meta_data_path) -> str:
    columns = df.columns.tolist()
    resource_type = df.iloc[0].to_dict()['resourceType']
    config = Config(default={resource_type: columns})
    ndjson_file = meta_data_path / f"{resource_type}.ndjson"
    with ndjson_file.open('w') as fp:
        for _, row in df.iterrows():
            non_null_columns = row.dropna().to_dict()
            non_null_columns = from_tabular(non_null_columns, config, None, {'resourceType': resource_type})
            json.dump(non_null_columns, separators=(',', ':'), fp=fp)
            fp.write('\n')
    return f"{ndjson_file}"


def read_file_into_dataframe(file_path, file_type='tsv'):
    """
    Read a TSV or Excel file into a DataFrame.

    Parameters:
    - file_path (str): Path to the input file.
    - file_type (str, optional): Input file type - 'tsv' for TSV (default) or 'excel' for Excel.

    Returns:
    - pandas.DataFrame: DataFrame containing the file data.

    Raises:
    - ValueError: If an unsupported file type is provided.
    """
    if file_type == 'tsv':
        df = pd.read_csv(file_path, sep='\t')
    elif file_type == 'excel':
        df = pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file type. Choose 'tsv' or 'excel'.")

    return df


def create_file_from_dict_iterator(data_iterator, file_path, file_type='tsv'):
    """
    Create a TSV or Excel file from an iterator of dictionaries.

    Parameters:
    - data_iterator (iterator): Iterator of dictionaries containing data.
    - file_path (str): Path to save the output file.
    - file_type (str, optional): Output file type - 'tsv' for TSV (default) or 'excel' for Excel.

    Raises:
    - ValueError: If an unsupported file type is provided.
    """
    if file_type == 'tsv':
        df = pd.DataFrame(data_iterator).fillna('')
        df.to_csv(file_path, sep='\t', index=False)
    elif file_type == 'excel':
        df = pd.DataFrame(data_iterator).fillna('')
        df.to_excel(file_path, index=False)
    else:
        raise ValueError("Unsupported file type. Choose 'tsv' or 'excel'.")
