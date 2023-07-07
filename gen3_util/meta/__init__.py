import importlib
import logging
import pathlib
import uuid
from typing import Iterator, Any

# TODO fix me, make configurable
from fhir.resources import FHIRAbstractModel
from pydantic import ValidationError, BaseModel, validator

from gen3_util.common import is_json_extension, read_json

ACED_NAMESPACE = uuid.uuid3(uuid.NAMESPACE_DNS, 'aced-ipd.org')

FHIR_CLASSES = importlib.import_module('fhir.resources')

logger = logging.getLogger(__name__)


class ParseResult(BaseModel):
    class Config:
        arbitrary_types_allowed = True
    """Results of FHIR validation of dict."""
    resource: Any = None
    """If valid, the FHIR resource."""
    exception: Exception = None
    """If invalid, the exception."""
    path: pathlib.Path = None
    """Source file, if available."""
    offset: int = 0
    """Base 0 offset of line number(ndjson) or entry(bundle)."""
    resource_id: str = None
    """Resource id of resource"""

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
                if hasattr(_, 'resource'):  # BundleEntry
                    yield ParseResult(path=_path, resource=_.resource, offset=offset, exception=None)
                elif hasattr(_, 'item'):  # ListEntry
                    yield ParseResult(path=_path, resource=_.item, offset=offset, exception=None)
                else:
                    yield ParseResult(path=_path, resource=_.item, offset=offset, exception=None)
                offset += 1
    pass


def _has_entries(_: ParseResult):
    """FHIR types Bundles List have entries"""
    if _.resource is None:
        return False
    return _.resource.resource_type in ["Bundle", "List"] and _.resource.entry is not None


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

    assert len(input_files) > 0, f"No files found in {directory_path.name}"

    for input_file in input_files:
        for json_obj in read_json(input_file):
            parse_result = parse_obj(json_obj, validate=validate)
            for _ in _entry_iterator(parse_result):
                yield _
