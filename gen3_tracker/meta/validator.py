import pathlib
from collections import defaultdict
from typing import List
from urllib.parse import urlparse

import orjson
from fhir.resources.coding import Coding
from fhir.resources.identifier import Identifier
from fhir.resources.reference import Reference
from nested_lookup import nested_lookup
from pydantic import BaseModel, ConfigDict

from gen3_tracker.meta import ParseResult, directory_reader
from collections import Counter


class ValidateDirectoryResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    """Results of FHIR validation of directory."""
    resources: dict
    exceptions: List[ParseResult]

    def model_dump(self):
        """

         temporary until we switch to pydantic2
        """
        for _ in self.exceptions:
            _.exception = str(_.exception)
            _.path = str(_.path)
        return orjson.loads(self.model_dump_json())


def _check_coding(self: Coding, *args, **kwargs):
    """MonkeyPatch replacement for dict(), check Coding."""
    # note `self` is the Coding
    assert self.code, f"Missing `code` {self}"
    assert (not self.code.startswith("http")), f"`code` should _not_ be a url http {self.code}"
    assert ":" not in self.code, f"`code` should not contain ':' {self.code}"
    assert self.system, f"Missing `system` {self}"
    assert "%" not in self.system, f"`system` should be a simple url without uuencoding {self.system}"
    parsed = urlparse(self.system)
    assert parsed.scheme, f"`system` is not a URI {self}"
    assert self.display, f"Missing `display` {self}"
    # call the original dict() method
    return orig_coding_dict(self, *args, **kwargs)


def _check_identifier(self: Identifier, *args, **kwargs):
    """MonkeyPatch replacement for dict(), check Identifier."""
    # note `self` is the Identifier
    assert self.value, f"Missing `value` {self}"
    assert self.system, f"Missing `system` {self}"
    parsed = urlparse(self.system)
    assert parsed.scheme, f"`system` is not a URI {self}"
    assert "%" not in self.system, f"`system` should be a simple url without uuencoding {self.system}"
    # call the original dict() method
    return orig_identifier_dict(self, *args, **kwargs)


def _check_reference(self: Reference, *args, **kwargs):
    """MonkeyPatch replacement for dict(), check Reference."""
    # note `self` is the Identifier
    assert self.reference, f"Missing `reference` {self}"
    assert '/' in self.reference, f"Does not appear to be Relative reference {self}"
    assert 'http' not in self.reference, f"Absolute references not supported {self}"
    assert len(self.reference.split('/')) == 2, f"Does not appear to be Relative reference {self}"

    # call the original dict() method
    return orig_reference_dict(self, *args, **kwargs)


def validate(directory_path: pathlib.Path) -> ValidateDirectoryResult:
    """Check FHIR data, accumulate results."""
    exceptions = []
    resources = defaultdict(int)
    # add resources to bundle
    references = []
    ids = []

    for parse_result in directory_reader(directory_path, validate=True):
        if parse_result.exception:
            exceptions.append(parse_result)
            continue
        _ = parse_result.resource
        ids.append(f"{_.resource_type}/{_.id}")
        nested_references = nested_lookup('reference', parse_result.json_obj)
        # https://www.hl7.org/fhir/medicationrequest-definitions.html#MedicationRequest.medication
        # is a reference to a Medication resource https://www.hl7.org/fhir/references.html#CodeableReference
        # so it has a reference.reference form, strip it out
        nested_references = [_ for _ in nested_references if isinstance(_, str)]
        references.extend(nested_references)
        resources[parse_result.resource.resource_type] += 1

    # assert references exist
    references = set(references)
    ids_list = ids
    ids = set(ids)
    if not references.issubset(ids):
        _ = Exception(f"references not found {references - ids}")
        _ = ParseResult(resource=None, exception=_, path=directory_path, resource_id=None)
        exceptions.append(_)
    if len(ids) != len(ids_list):
        # Create a Counter object from ids_list
        counter = Counter(ids_list)
        # Get the duplicate ids
        duplicate_ids = [id_ for id_, count in counter.items() if count > 1]
        # log it
        _ = Exception(f"Duplicate ids found {duplicate_ids}")
        _ = ParseResult(resource=None, exception=_, path=directory_path, resource_id=None)
        exceptions.append(_)

    return ValidateDirectoryResult(resources={'summary': dict(resources)}, exceptions=exceptions)


#
# monkey patch dict() methods
#
orig_coding_dict = Coding.dict
Coding.dict = _check_coding

orig_identifier_dict = Identifier.dict
Identifier.dict = _check_identifier

orig_reference_dict = Reference.dict
Reference.dict = _check_reference
