import pathlib

from gen3_util.meta.importer import PathParser
from fhir.resources.identifier import Identifier


class MyPathParser(PathParser):
    """A Class to extract Patient and Specimen from directory path.

    Must use PathParser as a base.

    In this example, we assume the following directory structure:

    tests/fixtures/dir_to_study_with_meta/
    ├── file-2.csv
    ├── p1
    │   ├── s1
    │   │   └── file-3.pdf
    │   ├── s2
    │   │   └── file-4.tsv
    │   └── s3
    │       └── file-5
    └── p2
        └── s4
            └── file-1.txt

    Where the meta data associated with file file-3.pdf is:
    * Patient(p1), Specimen(s1)
    * etc.
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
