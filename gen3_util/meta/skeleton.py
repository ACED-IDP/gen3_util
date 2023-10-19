import pathlib
import sys
import uuid

from fhir.resources.attachment import Attachment
from fhir.resources.fhirtypes import DocumentReferenceContentType
from fhir.resources.researchsubject import ResearchSubject
from fhir.resources.resource import Resource
from fhir.resources.documentreference import DocumentReference
from fhir.resources.identifier import Identifier
from fhir.resources.observation import Observation
from fhir.resources.patient import Patient
from fhir.resources.researchstudy import ResearchStudy
from fhir.resources.specimen import Specimen
from fhir.resources.task import Task, TaskOutput, TaskInput
from gen3.submission import Gen3Submission
from orjson import orjson

from gen3_util.common import EmitterContextManager
from gen3_util.config import Config, ensure_auth
from gen3_util.files.lister import ls, meta_nodes, meta_resource
from gen3_util.meta import ACED_NAMESPACE


def update_document_reference(document_reference, index_record):
    """Update document reference with index record."""
    assert document_reference.resource_type == 'DocumentReference'
    assert index_record['did'] == document_reference.id

    document_reference.docStatus = 'final'
    document_reference.status = 'current'
    document_reference.date = index_record['created_date'] + "Z"
    attachment = Attachment()
    attachment.extension = [
        {
            "url": "http://aced-idp.org/fhir/StructureDefinition/md5",
            "valueString": index_record['hashes']['md5']
        },
        {
            "url": "http://aced-idp.org/fhir/StructureDefinition/source_path",
            "valueUrl": f"file:///{index_record['file_name']}"
        }
    ]
    attachment.contentType = index_record['metadata'].get('mime_type', 'application/octet-stream')

    attachment.url = f"file:///{index_record['file_name']}"
    # TODO "url": f"file:///{str(file).replace(remove_path_prefix, '', 1)}",  # Uri where the data can be found
    attachment.size = index_record['size']
    attachment.title = pathlib.Path(index_record['file_name']).name
    attachment.creation = index_record['created_date']

    content = DocumentReferenceContentType(attachment=attachment)

    document_reference.content = [content]


def indexd_to_study(config: Config, project_id: str, output_path: str, overwrite: bool) -> list[Resource]:
    """Read files uploaded to indexd and create a skeleton graph for document and ancestors from a set of identifiers.

    Args:
        config:
        project_id:
        output_path:
        overwrite: check for existing records and skip if found
    """

    assert project_id, "project_id required"
    assert project_id.count('-') == 1, "project_id must be of the form program-project"

    auth = ensure_auth(config.gen3.refresh_file)

    existing_resource_ids = set()
    if not overwrite:
        print("Checking for existing records...", file=sys.stderr)
        nodes = meta_nodes(config, project_id, auth=auth)  # fetches document_ids by default
        existing_resource_ids = set([_['id'] for _ in nodes])
        print("Done", file=sys.stderr)

    # get file client
    records = ls(config, metadata={'project_id': project_id})['records']

    submission_client = Gen3Submission(auth_provider=auth)

    with EmitterContextManager(output_path, file_mode="w") as emitter:
        for _ in records:
            resources = create_skeleton(metadata=_['metadata'], submission_client=submission_client)
            for resource in resources:
                # check document references
                if resource.id in existing_resource_ids:
                    continue
                existing_resource_ids.add(resource.id)

                if resource.resource_type == 'DocumentReference':
                    update_document_reference(resource, _)
                ids = ''
                if resource.identifier and len(resource.identifier) > 0:
                    ids = [_.value for _ in resource.identifier]
                subject = ''
                if hasattr(resource, 'subject') and resource.subject:
                    subject = resource.subject.reference

                print(f"Writing {resource.resource_type} {resource.id} {ids} {subject}", file=sys.stderr)
                emitter.emit(resource.resource_type).write(
                    resource.json(option=orjson.OPT_APPEND_NEWLINE)
                )


def _get_system(identifier: str, project_id: str):
    """Return system component of simplified identifier"""
    if '#' in identifier:
        return identifier.split('#')[0]
    # default
    return f"https://aced-idp.org/{project_id}"


def create_skeleton(metadata: dict, submission_client: Gen3Submission) -> list[Resource]:  # TODO fix caller auth
    """
    Create a skeleton graph for document and ancestors from a set of identifiers.
    """

    document_reference_id = metadata.get('document_reference_id', None)
    specimen_identifier = metadata.get('specimen_identifier', None)
    patient_identifier = metadata.get('patient_identifier', None)
    task_identifier = metadata.get('task_identifier', None)
    observation_identifier = metadata.get('observation_identifier', None)
    project_id = metadata.get('project_id', None)

    if not document_reference_id:
        document_reference_id = metadata.get('datanode_object_id', None)

    assert document_reference_id, f"document_reference_id required {metadata}"

    assert project_id, "project_id required"
    assert project_id.count('-') == 1, "project_id must be of the form program-project"
    program, project = project_id.split('-')

    # create entities
    research_study = research_subject = observation = specimen = patient = task = None

    _existing_research_study = meta_resource(submission_client=submission_client, identifier=None, project_id=project_id, gen3_type='research_study')
    if not _existing_research_study:
        research_study = ResearchStudy(status='active')
        research_study.id = str(uuid.uuid5(ACED_NAMESPACE, f"{project_id}/ResearchStudy/{project}"))
        research_study.description = f"Skeleton ResearchStudy for {project_id}"
        research_study_id = research_study.id
    else:
        research_study_id = _existing_research_study['id']

    document_reference = DocumentReference(status='current', content=[{'attachment': {'url': f"file://{document_reference_id}"}}])
    document_reference.id = document_reference_id

    if patient_identifier:
        _existing_patient = meta_resource(submission_client=submission_client, identifier=patient_identifier, project_id=project_id, gen3_type='patient')
        if not _existing_patient:
            patient = Patient()

            patient.id = str(uuid.uuid5(ACED_NAMESPACE, f"{project_id}/Patient/{patient_identifier}"))
            patient.identifier = [Identifier(value=patient_identifier, system=_get_system(patient_identifier, project_id=project_id), use='official')]

            research_subject = ResearchSubject(
                status='active',
                study={'reference': f"ResearchStudy/{research_study_id}"},
                subject={'reference': f"Patient/{patient.id}"}
            )
            research_subject.id = str(uuid.uuid5(ACED_NAMESPACE, f"{project_id}/ResearchSubject/{patient_identifier}"))
            research_subject.identifier = [Identifier(value=patient_identifier, system=_get_system(patient_identifier, project_id=project_id), use='official')]

    if observation_identifier:
        _existing_observation = meta_resource(submission_client=submission_client, identifier=observation_identifier, project_id=project_id, gen3_type='observation')
        if not _existing_observation:
            assert patient, "patient required for observation"
            observation = Observation(status='final', code={'text': 'unknown'})
            observation.id = str(uuid.uuid5(ACED_NAMESPACE, f"{project_id}/Observation/{observation_identifier}"))
            observation.identifier = [Identifier(value=observation_identifier, system=_get_system(observation_identifier, project_id=project_id), use='official')]
            observation.subject = {'reference': f"Patient/{patient.id}"}

    if specimen_identifier:
        _existing_specimen = meta_resource(submission_client=submission_client, identifier=specimen_identifier, project_id=project_id, gen3_type='specimen')
        if not _existing_specimen:
            assert patient, "patient required for specimen"
            specimen = Specimen()
            specimen.id = str(uuid.uuid5(ACED_NAMESPACE, f"{project_id}/Specimen/{specimen_identifier}"))
            specimen.identifier = [Identifier(value=specimen_identifier, system=_get_system(specimen_identifier, project_id=project_id), use='official')]
            specimen.subject = {'reference': f"Patient/{patient.id}"}

    if task_identifier:
        _existing_task = meta_resource(submission_client=submission_client, identifier=task_identifier, project_id=project_id, gen3_type='task')
        if not _existing_task:
            task = Task(intent='unknown', status='completed')
            task.identifier = [Identifier(value=task_identifier, system=_get_system(task_identifier, project_id=project_id), use='official')]
            task.id = str(uuid.uuid5(ACED_NAMESPACE, f"{project_id}/Task/{task_identifier}"))

    # create relationships

    # assign subject, specimen of observation
    if observation and specimen and not observation.specimen:
        observation.specimen = {'reference': f"Specimen/{specimen.id}"}
    if observation and patient and not observation.subject:
        observation.subject = {'reference': f"Patient/{patient.id}"}

    if task:
        task.input = []
        if specimen:
            task.input.append(
                TaskInput(
                    valueReference={'reference': f"Specimen/{specimen.id}"},
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
        document_reference.subject = {'reference': f"Observation/{observation.id}"}
    if specimen and not document_reference.subject:
        document_reference.subject = {'reference': f"Specimen/{specimen.id}"}
    if patient and not document_reference.subject:
        document_reference.subject = {'reference': f"Patient/{patient.id}"}
    if not document_reference.subject:
        document_reference.subject = {'reference': f"ResearchStudy/{research_study_id}"}

    return [_ for _ in [research_study, research_subject, patient, observation, specimen, task, document_reference] if _]
