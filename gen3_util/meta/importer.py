import inspect
import logging
import sys
import unicodedata
import uuid
from datetime import timezone, datetime
import hashlib

import pathlib
import click

import orjson
import mimetypes

from gen3_util.cli import CLIOutput
from gen3_util.common import EmitterContextManager
from gen3_util.config import Config
from gen3_util.meta import ACED_NAMESPACE

from fhir.resources.identifier import Identifier
from fhir.resources.patient import Patient
from fhir.resources.specimen import Specimen
from fhir.resources.resource import Resource

try:
    import magic
except ImportError as e:
    print(f"Requires libmagic installed on your system to determine file mime-types\nError: '{e}'\nFor installation instructions see https://github.com/ahupp/python-magic#installation")
    sys.exit(1)

PLUGINS = []
PLUGINS_ADDED_TO_PATH = False

logger = logging.getLogger(__name__)


def create_research_study(name, description):
    """Creates bare-bones study."""
    study = {
        'title': name,
        'id': str(uuid.uuid5(ACED_NAMESPACE, name)),
        'description': description,
        'status': 'active',
        "resourceType": "ResearchStudy",
    }
    return study


def md5sum(file_name):
    """Calculate the hash and size."""
    md5_hash = hashlib.md5()
    file_name = unicodedata.normalize("NFKD", str(file_name))
    with open(file_name, "rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)

    return md5_hash.hexdigest()


def _extract_fhir_resources(file, input_path, plugin_path) -> list[Resource]:
    """Parse other resources from file."""
    plugins = _discover_plugins(plugin_path=plugin_path)

    if plugin_path:
        assert len(plugins) > 0, f"No plugins found in {plugin_path}"
    assert input_path, "No input path provided."

    resources = []

    for plugin in plugins:
        patient_identifier = plugin.extract_patient_identifier(path=str(file))
        specimen_identifier = plugin.extract_specimen_identifier(path=str(file))
        if patient_identifier:
            resources.append(plugin.patient(patient_identifier))
        if specimen_identifier:
            assert patient_identifier, "Specimen found without patient."
            resources.append(plugin.specimen(specimen_identifier, patient_identifier))

    return resources


@click.command('dir')
@click.argument('input_path')
@click.argument('output_path')
@click.option('--project_id', required=True,
              default=None,
              show_default=True,
              help='Gen3 program-project'
              )
@click.option('--remove_path_prefix',
              default='',
              show_default=True,
              help='Remove prefix from file paths.  '
                   'Creates well-known form for achieving reproducible directories independent '
                   'of the directory the files were collected from.'
              )
@click.option('--pattern',
              default='**/*',
              show_default=True,
              help='File names to match.')
@click.option('--plugin_path',
              default=None,
              show_default=True,
              help='Read plugins from this path.')
@click.pass_obj
def cli(config: Config, input_path, output_path, project_id, remove_path_prefix, pattern, plugin_path):
    """Create minimal study meta from matching files in INPUT_PATH, write to OUTPUT_PATH.
    """
    with CLIOutput(config=config) as output:
        output.update(dir_to_study(project_id, input_path, remove_path_prefix, output_path, pattern, plugin_path))


def dir_to_study(project_id, input_path, remove_path_prefix, output_path, pattern, plugin_path) -> dict:
    """Transform ResearchStudy, DocumentReference from matching files in input path."""
    # print(project_id, path, pattern)
    input_path = pathlib.Path(input_path)
    output_path = pathlib.Path(output_path)

    for _ in [input_path]:
        assert _.exists(), f"input_path {_} does not exist."
        assert _.is_dir(), f"input_path {_} is not a directory."

    for _ in [output_path]:
        if not _.exists():
            print(f"output_path {_} does not exist, creating...")
            _.mkdir(parents=True, exist_ok=True)

    _magic = magic.Magic(mime=True, uncompress=True)  # https://github.com/ahupp/python-magic#installation
    program, project = project_id.split('-')
    research_study = create_research_study(project, f"A study with files from {input_path}/{pattern}")

    # for user display/logs
    counts = {
        'ResearchStudy': {'count': 1},
        'ResearchSubject': {'count': 0},
        'Patient': {'count': 0},
        'Specimen': {'count': 0}
    }

    with EmitterContextManager(output_path, file_mode="wb") as emitter:
        emitter.emit('ResearchStudy').write(
            orjson.dumps(research_study, orjson.OPT_APPEND_NEWLINE))

        count = 0
        size = 0
        already_seen = set()
        for file in input_path.glob(pattern):
            if file.is_dir():
                continue
            if file.is_symlink():
                logger.info(f"Skipping symlink {file}")
                continue

            try:
                resources = _extract_fhir_resources(str(file).replace(remove_path_prefix, '', 1), input_path, plugin_path)
            except ValueError:
                # expected error if plugin does not want to process this file
                logger.info(f"Skipping {file}")
                continue

            stat = file.stat()
            modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
            mime, encoding = mimetypes.guess_type(file)
            if not mime:
                mime = _magic.from_file(file)

            subject_reference = f"ResearchStudy/{research_study['id']}"  # Who/what is the subject of the document

            for resource in resources:
                if resource.id in already_seen:
                    continue

                if resource.resource_type == 'Patient':
                    subject_reference = f"Patient/{resource.id}"
                    research_subject = {
                        "id": str(uuid.uuid5(ACED_NAMESPACE, "ResearchSubject" + subject_reference)),
                        "resourceType": "ResearchSubject",
                        "status": "active",
                        "study": {
                            "reference": f"ResearchStudy/{research_study['id']}"
                        },
                        "subject": {
                            "reference": subject_reference
                        },

                    }
                    emitter.emit("ResearchSubject").write(
                        orjson.dumps(research_subject, option=orjson.OPT_APPEND_NEWLINE)
                    )
                    counts["ResearchSubject"]['count'] += 1

                already_seen.add(resource.id)
                emitter.emit(resource.resource_type).write(
                    orjson.dumps(resource.dict(), option=orjson.OPT_APPEND_NEWLINE)
                )
                counts[resource.resource_type]['count'] += 1

            document_reference = {
              "resourceType": "DocumentReference",
              "status": "current",
              "docStatus": "final",
              "id": str(uuid.uuid5(ACED_NAMESPACE, research_study['id'] + f"::{file}")),
              "date": modified.isoformat(),  # When this document reference was created
              "content": [{
                "attachment": {
                    "extension": [{
                        "url": "http://aced-idp.org/fhir/StructureDefinition/md5",
                        "valueString": md5sum(file)
                    }, {
                        "url": "http://aced-idp.org/fhir/StructureDefinition/source_path",
                        "valueUrl": f"file:///{file}"
                    }],
                    "contentType": mime,  # Mime type of the content, with charset etc.
                    "url": f"file:///{str(file).replace(remove_path_prefix, '', 1)}",  # Uri where the data can be found
                    "size": stat.st_size,  # Number of bytes of content (if url provided)
                    "title": file.name,  # Label to display in place of the data
                    "creation": modified.isoformat()  # Date attachment was first created
                },
              }],
              "subject": {
                  "reference": subject_reference
              }
            }
            emitter.emit('DocumentReference').write(orjson.dumps(document_reference, option=orjson.OPT_APPEND_NEWLINE))
            count += 1
            size += stat.st_size

        counts['DocumentReference'] = {'count': count, 'size': size}
    return {'summary': counts}


class PathParser:
    """A Class to extract Patient and Specimen from directory path, extended by plugins."""

    def __init__(self):
        self.patients = {}
        self.specimens = {}

    def extract_patient_identifier(self, path: str) -> Identifier:
        """Parse path and return patient identifier.

        System and value must be set.
        See: https://build.fhir.org/datatypes.html#Identifier
        """
        raise NotImplementedError("Must be implemented by plugin.")

    def extract_specimen_identifier(self, path: str) -> Identifier:
        """Parse path and return specimen identifier.

        System and value must be set.
        See: https://hl7.org/fhir/datatypes.html#Identifier
        """
        raise NotImplementedError("Must be implemented by plugin.")

    def patient(self, patient_identifier: Identifier) -> Patient:
        """Return a complete Patient.

        See: https://hl7.org/fhir/patient.html
        """
        id_ = str(uuid.uuid5(ACED_NAMESPACE, f"{patient_identifier.system}#{patient_identifier.value}"))
        if id_ not in self.patients:
            self.patients[id_] = Patient.parse_obj({'id': id_, 'identifier': [patient_identifier]})
        return self.patients[id_]

    def specimen(self, specimen_identifier: Identifier, patient_identifier: Identifier) -> Specimen:
        """Return a complete Patient.

        See: https://hl7.org/fhir/specimen.html
        """
        patient_id = str(uuid.uuid5(ACED_NAMESPACE, f"{patient_identifier.system}#{patient_identifier.value}"))
        id_ = str(uuid.uuid5(ACED_NAMESPACE, f"{specimen_identifier.system}#{specimen_identifier.value}"))
        if id_ not in self.specimens:
            self.specimens[id_] = Specimen.parse_obj({'id': id_, 'identifier': [specimen_identifier], 'subject': {'reference': f"Patient/{patient_id}"}})
        return self.specimens[id_]


def _discover_plugins(plugin_path: str) -> list[PathParser]:
    """Discover plugins."""
    import importlib
    import pkgutil
    global PLUGINS_ADDED_TO_PATH

    if len(PLUGINS) > 0 or plugin_path is None:
        return PLUGINS

    if not PLUGINS_ADDED_TO_PATH and pathlib.Path(plugin_path).exists():
        sys.path.append(plugin_path)
        sys.path.append(str(pathlib.Path(plugin_path).parent))
        PLUGINS_ADDED_TO_PATH = True

    discovered_plugins = {
        name: importlib.import_module(name)
        for finder, name, is_pkg
        in pkgutil.iter_modules()
        if 'gen3_util_plugin_' in name and name in plugin_path
    }

    for name, pkg in discovered_plugins.items():
        for _, obj in inspect.getmembers(pkg):
            if inspect.isclass(obj) and issubclass(obj, PathParser) and obj.__module__ == name:
                logger.debug(f'plugin {_}')
                PLUGINS.append(obj())

    return PLUGINS
