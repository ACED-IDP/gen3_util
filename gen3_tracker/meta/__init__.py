import importlib
import logging
import pathlib
from collections import defaultdict
from typing import Iterator, Any, Optional, Callable

# TODO fix me, make configurable
from fhir.resources import FHIRAbstractModel
from nested_lookup import nested_lookup
from pydantic.v1 import ValidationError, validator
from pydantic import BaseModel, ConfigDict

from gen3_tracker.common import is_json_extension, read_json, read_ndjson_file

FHIR_CLASSES = importlib.import_module('fhir.resources')

logger = logging.getLogger(__name__)


class ParseResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    """Results of FHIR validation of dict."""
    resource: Optional[Any] = None
    """If valid, the FHIR resource."""
    exception: Optional[Exception] = None
    """If invalid, the exception."""
    path: Optional[pathlib.Path] = None
    """Source file, if available."""
    offset: Optional[int] = 0
    """Base 0 offset of line number(ndjson) or entry(bundle)."""
    resource_id: Optional[str] = None
    """Resource id of resource"""
    json_obj: Optional[dict] = None
    """Original json object"""

    @validator("resource")
    def validate_resource(cls, val):
        if val is None:
            return val
        if issubclass(type(val), FHIRAbstractModel):
            return val
        raise TypeError(f"Wrong type for 'resource', was {type(val)} must be subclass of FHIRAbstractModel")


def parse_obj(resource: dict, validate=True) -> ParseResult:
    """Load a dictionary into a FHIR model """
    try:
        assert 'resourceType' in resource, "Dict missing `resourceType`, is it a FHIR dict?"
        klass = FHIR_CLASSES.get_fhir_model_class(resource['resourceType'])
        _ = klass.parse_obj(resource)
        if validate:
            # trigger object traversal, see monkey patch below, at bottom of file
            _.dict()
        return ParseResult(resource=_, exception=None, path=None, resource_id=_.id)
    except (ValidationError, AssertionError) as e:
        return ParseResult(resource=None, exception=e, path=None, resource_id=resource.get('id', None))


def _entry_iterator(parse_result: ParseResult) -> Iterator[ParseResult]:
    """See if there are entries"""
    if not _has_entries(parse_result):
        yield parse_result
    else:
        _path = parse_result.path
        offset = 0
        if parse_result.resource.entry and len(parse_result.resource.entry) > 0:
            for _ in parse_result.resource.entry:
                if _ is None:
                    break
                if hasattr(_, 'resource') and _.resource:  # BundleEntry
                    yield ParseResult(path=_path, resource=_.resource, offset=offset, exception=None, json_obj=_.resource.dict())
                elif hasattr(_, 'item'):  # ListEntry
                    yield ParseResult(path=_path, resource=_.item, offset=offset, exception=None, json_obj=_.item.dict())
                else:
                    yield ParseResult(path=_path, resource=_.item, offset=offset, exception=None, json_obj=_.item.dict())
                offset += 1
    pass


def _has_entries(_: ParseResult):
    """FHIR types Bundles List have entries"""
    if _.resource is None:
        return False
    return _.resource.resource_type in ["List"] and _.resource.entry is not None


def directory_reader(directory_path: str,
                     recurse: bool = True,
                     validate: bool = False) -> Iterator[ParseResult]:
    """Extract FHIR resources from directory

    Read any type of json file, return itemized resources by iterating through Bundles and Lists
    """

    if isinstance(directory_path, str):
        directory_path = pathlib.Path(directory_path)

    directory_path = directory_path.expanduser()

    try:
        input_files = [_ for _ in pathlib.Path.glob(directory_path.name) if is_json_extension(_.name)]
    except TypeError:
        input_files = []

    if len(input_files) == 0:
        if recurse:
            input_files = [_ for _ in directory_path.glob('**/*.*') if is_json_extension(_.name)]

    # assert len(input_files) > 0, f"No files found in {directory_path.name}"

    for input_file in input_files:
        offset = 0
        for json_obj in read_json(input_file):
            parse_result = parse_obj(json_obj, validate=validate)
            parse_result.path = input_file
            parse_result.offset = offset
            parse_result.json_obj = json_obj
            offset += 1
            for _ in _entry_iterator(parse_result):
                yield _


def aggregate(metadata_path: pathlib.Path | str) -> dict:
    """Aggregate metadata counts resourceType(count)-count->resourceType(count)."""

    nested_dict: Callable[[], defaultdict[str, defaultdict]] = lambda: defaultdict(defaultdict)

    if not isinstance(metadata_path, pathlib.Path):
        metadata_path = pathlib.Path(metadata_path)
    summary = nested_dict()
    for path in sorted(metadata_path.glob("*.ndjson")):
        for _ in read_ndjson_file(path):

            resource_type = _['resourceType']
            if 'count' not in summary[resource_type]:
                summary[resource_type]['count'] = 0
            summary[resource_type]['count'] += 1

            refs = nested_lookup('reference', _)
            for ref in refs:
                # A codeable reference is an object with a codeable concept and a reference
                if isinstance(ref, dict):
                    ref = ref['reference']
                ref_resource_type = ref.split('/')[0]
                if 'references' not in summary[resource_type]:
                    summary[resource_type]['references'] = nested_dict()
                dst = summary[resource_type]['references'][ref_resource_type]
                if 'count' not in dst:
                    dst['count'] = 0
                dst['count'] += 1

    return summary
