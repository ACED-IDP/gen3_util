import pathlib

from gen3_util.meta.importer import PathParser
from fhir.resources.identifier import Identifier


class MyPathParser(PathParser):
    """A Class to extract Patient and Specimen from directory path.

    Must use PathParser as a base
    """
    def extract_patient_identifier(self, path: str) -> Identifier:
        path_ = pathlib.Path(path)
        if len(path_.parts) < 3:
            return None
        if not path_.parts[-3].startswith('p'):
            return None
        return Identifier.parse_obj({'system': 'http://example.org/patient', 'value': path_.parts[-3]})

    def extract_specimen_identifier(self, path: str) -> Identifier:
        path_ = pathlib.Path(path)
        if len(path_.parts) < 3:
            return None
        if not path_.parts[-2].startswith('s'):
            return None

        return Identifier.parse_obj({'system': 'http://example.org/specimen', 'value': path_.parts[-2]})
