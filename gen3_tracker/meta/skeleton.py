import pathlib
import uuid
from datetime import datetime
from pytz import UTC

import orjson
from fhir.resources.attachment import Attachment
from fhir.resources.bundle import Bundle, BundleEntry, BundleEntryRequest
from fhir.resources.documentreference import DocumentReference
from fhir.resources.fhirtypes import DocumentReferenceContentType
from fhir.resources.identifier import Identifier
from fhir.resources.observation import Observation
from fhir.resources.operationoutcome import OperationOutcome
from fhir.resources.patient import Patient
from fhir.resources.researchstudy import ResearchStudy
from fhir.resources.researchsubject import ResearchSubject
from fhir.resources.resource import Resource
from fhir.resources.specimen import Specimen
from fhir.resources.task import Task, TaskOutput, TaskInput

from gen3_tracker import ACED_NAMESPACE
from gen3_tracker.common import create_id, EmitterContextManager
from gen3_tracker.git import DVC, run_command, dvc_data


def _get_system(identifier: str, project_id: str):
    """Return system component of simplified identifier"""
    if '#' in identifier:
        return identifier.split('#')[0]
    if '|' in identifier:
        return identifier.split('|')[0]
    # default
    return f"https://aced-idp.org/{project_id}"


def meta_index():
    """Read all the ndjson files in the `META` directory and create a dictionary with the id as the key and the official identifier as the value"""
    meta_dir = pathlib.Path('META')
    id_dict = {}

    for file in meta_dir.glob('*.ndjson'):
        with open(file, 'r') as f:
            for line in f:
                record = orjson.loads(line)
                _id = record.get('id')
                resource_type = record.get('resourceType')
                if resource_type == 'Bundle':
                    break
                official_identifier = next((identifier.get('value') for identifier in record.get('identifier', []) if identifier.get('use') == 'official'), None)
                if not official_identifier and record.get('identifier'):
                    official_identifier = record['identifier'][0]['value']

                if _id and official_identifier:
                    id_dict[f"{resource_type}/{_id}"] = official_identifier

    return id_dict


def update_document_reference(document_reference: DocumentReference, dvc_data: DVC):
    """Update document reference with index record."""
    assert document_reference.resource_type == 'DocumentReference'
    assert dvc_data.out.object_id == document_reference.id, f"{dvc_data['did']} != {document_reference.id}"
    assert dvc_data.out.modified, f"dvc_data missing modified: {dvc_data}"
    document_reference.docStatus = 'final'
    document_reference.status = 'current'

    document_reference.date = dvc_data.out.modified

    attachment = Attachment()
    if dvc_data.out.realpath:
        source_path = f"file:///{dvc_data.out.realpath}"
    else:
        source_path = dvc_data.out.source_url

    attachment.extension = [
        {
            "url": f"http://aced-idp.org/fhir/StructureDefinition/{dvc_data.out.hash}",
            "valueString": dvc_data.out.hash_value
        },
        {
            "url": "http://aced-idp.org/fhir/StructureDefinition/source_path",
            "valueUrl": source_path
        }
    ]
    attachment.contentType = dvc_data.out.mime

    attachment.url = f"file:///{dvc_data.out.path}"
    # TODO "url": f"file:///{str(file).replace(remove_path_prefix, '', 1)}",  # Uri where the data can be found
    attachment.size = dvc_data.out.size
    attachment.title = pathlib.Path(dvc_data.out.path).name
    attachment.creation = dvc_data.out.modified

    content = DocumentReferenceContentType(attachment=attachment)

    document_reference.content = [content]


def create_id_from_strings(resource_type: str, project_id: str, identifier_string: str) -> str:
    """Create an id from strings."""
    if not identifier_string:
        return None
    return str(uuid.uuid5(ACED_NAMESPACE, f"{project_id}/{resource_type}/{_get_system(identifier_string, project_id)}|{identifier_string}"))


def create_skeleton(dvc: dict, project_id: str, meta_index: set[str] = []) -> list[Resource]:
    """
    Create a skeleton graph for document and ancestors from a set of identifiers.
    """
    dvc = DVC.model_validate(dvc)

    specimen_identifier = dvc.meta.specimen
    patient_identifier = dvc.meta.patient
    task_identifier = dvc.meta.task
    observation_identifier = dvc.meta.observation
    project_id = dvc.project_id or project_id

    assert dvc.out, f"out required {dvc}"
    document_reference_id = dvc.out.set_object_id(project_id=project_id)

    assert project_id, "project_id required"
    assert project_id.count('-') == 1, "project_id must be of the form program-project"
    program, project = project_id.split('-')

    research_study = research_subject = observation = specimen = patient = task = document_reference = None

    # check if we have already created the resources

    research_study_id = create_id_from_strings(resource_type='ResearchStudy', project_id=project_id, identifier_string=project_id)
    specimen_id = create_id_from_strings(resource_type='Specimen', project_id=project_id, identifier_string=specimen_identifier)
    patient_id = create_id_from_strings(resource_type='Patient', project_id=project_id, identifier_string=patient_identifier)
    task_id = create_id_from_strings(resource_type='Task', project_id=project_id, identifier_string=task_identifier)
    observation_id = create_id_from_strings(resource_type='Observation', project_id=project_id, identifier_string=observation_identifier)

    _ = f'ResearchStudy/{research_study_id}'
    if _ in meta_index:
        research_study = meta_index[_]
    _ = f'Specimen/{specimen_id}'
    if _ in meta_index:
        specimen = meta_index[_]
    _ = f'Patient/{patient_id}'
    if _ in meta_index:
        patient = meta_index[_]
    _ = f'Task/{task_id}'
    if _ in meta_index:
        task = meta_index[_]
    _ = f'Observation/{observation_id}'
    if _ in meta_index:
        observation = meta_index[_]

    # create entities

    document_reference = DocumentReference(status='current', content=[{'attachment': {'url': "file://"}}])
    document_reference.id = document_reference_id
    document_reference.identifier = [
        Identifier(value=document_reference_id, system=_get_system(document_reference_id, project_id=project_id),
                   use='official')]
    update_document_reference(document_reference, dvc)

    if not research_study:
        research_study = ResearchStudy(status='active')
        research_study.description = f"Skeleton ResearchStudy for {project_id}"
        research_study.identifier = [
            Identifier(value=project_id, system=_get_system(project_id, project_id=project_id),
                       use='official')]
        research_study.id = create_id(research_study, project_id)

    if not patient and patient_identifier:
        patient = Patient()
        patient.identifier = [Identifier(value=patient_identifier, system=_get_system(patient_identifier, project_id=project_id), use='official')]
        patient.id = create_id(patient, project_id)

        research_subject = ResearchSubject(
            status='active',
            study={'reference': f"ResearchStudy/{research_study_id}"},
            subject={'reference': f"Patient/{patient.id}"}
        )
        research_subject.identifier = [Identifier(value=patient_identifier, system=_get_system(patient_identifier, project_id=project_id), use='official')]
        research_subject.id = create_id(research_subject, project_id)

    if not observation and observation_identifier:
        observation = Observation(status='final', code={'text': 'unknown'})
        observation.identifier = [Identifier(value=observation_identifier, system=_get_system(observation_identifier, project_id=project_id), use='official')]
        observation.id = create_id(observation, project_id)

        assert patient, "patient required for observation"
        observation.subject = {'reference': f"Patient/{patient.id}"}
        patient_id = patient.id

    if not specimen and specimen_identifier:

        # TODO:?
        # print(f'{specimen_identifier} Specimen/{specimen_id} not in {[(k, _) for k, _ in meta_index.items() if k.startswith("Specimen")]}')
        # exit(1)

        specimen = Specimen()
        specimen.identifier = [Identifier(value=specimen_identifier, system=_get_system(specimen_identifier, project_id=project_id), use='official')]
        specimen.id = create_id(specimen, project_id)
        specimen_id = specimen.id

        assert patient, "patient required for specimen"
        specimen.subject = {'reference': f"Patient/{patient.id}"}

    if not task and task_identifier:
        task = Task(intent='unknown', status='completed')
        task.identifier = [Identifier(value=task_identifier, system=_get_system(task_identifier, project_id=project_id),
                                      use='official')]
        task.id = create_id(task, project_id)
        task_id = task.id

    # create relationships

    # assign subject, specimen of observation
    if observation and specimen and not observation.specimen:
        observation.specimen = {'reference': f"Specimen/{specimen_id}"}
    if observation and patient and not observation.subject:
        observation.subject = {'reference': f"Patient/{patient_id}"}

    if task:
        task.input = []
        if specimen:
            task.input.append(
                TaskInput(
                    valueReference={"reference": f"Specimen/{specimen.id}"},
                    type={'text': 'Specimen'}
                )
            )
        if patient:
            task.input.append(
                TaskInput(
                    valueReference={'reference': f"Patient/{patient.id}"},
                    type={'text': 'Patient'}
                )
            )
        task.output = [
            TaskOutput(
                valueReference={'reference': f"DocumentReference/{document_reference.id}"},
                type={'text': 'DocumentReference'})
        ]

    # assign document reference subject
    if observation:
        document_reference.subject = {'reference': f"Observation/{observation_id}"}
    if specimen and not document_reference.subject:
        document_reference.subject = {'reference': f"Specimen/{specimen_id}"}
    if patient and not document_reference.subject:
        document_reference.subject = {'reference': f"Patient/{patient_id}"}
    if not document_reference.subject:
        document_reference.subject = {'reference': f"ResearchStudy/{research_study_id}"}

    return [_ for _ in [research_study, research_subject, patient, observation, specimen, task, document_reference] if _ and not isinstance(_, str)]


def update_meta_files(dry_run=False, project_id=None) -> list[str]:
    """Maintain the META directory."""
    assert project_id, "project_id required"
    manifest_path = pathlib.Path('MANIFEST')
    dvc_files = [_ for _ in manifest_path.rglob('*.dvc')]

    if not dvc_files:
        return []

    before_meta_files = [_ for _ in pathlib.Path('META').glob('*.ndjson')]
    before_meta_index = set(list(meta_index().keys()))
    emitted_already = []

    with EmitterContextManager('META') as emitter:
        for _ in dvc_data(dvc_files):
            resources = create_skeleton(_, project_id, meta_index())
            for resource in resources:
                key = f"{resource.resource_type}/{resource.id}"
                if key not in emitted_already:
                    emitter.emit(resource.resource_type).write(
                        resource.json(option=orjson.OPT_APPEND_NEWLINE)
                    )
                    emitted_already.append(key)

    after_meta_index = set(list(meta_index().keys()))
    orphaned_meta_index = before_meta_index - after_meta_index

    if orphaned_meta_index:
        # create a bundle to tell server about deletes
        now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        bundle = Bundle(type='transaction', timestamp=now)

        bundle.identifier = Identifier(value=now, system=_get_system(project_id, project_id=project_id), use='official')
        bundle.id = create_id(bundle, project_id)

        bundle.entry = []
        outcome = OperationOutcome(issue=[{'severity': 'warning', 'code': 'processing', 'diagnostics': 'Meta data items no longer in study.'}])
        bundle.issues = outcome

        for _ in orphaned_meta_index:
            bundle_entry = BundleEntry()
            bundle_entry.request = BundleEntryRequest(url=_, method='DELETE')
            bundle.entry.append(bundle_entry)

        with EmitterContextManager('META') as emitter:
            emitter.emit(bundle.resource_type, file_mode='a').write(
                    bundle.json(option=orjson.OPT_APPEND_NEWLINE)
                )

    after_meta_files = [_ for _ in pathlib.Path('META').glob('*.ndjson')]
    new_meta_files = [str(_) for _ in after_meta_files if _ not in before_meta_files]

    if new_meta_files:
        run_command(f'git add {" ".join(new_meta_files)}', dry_run=dry_run, no_capture=True)

    return after_meta_files
