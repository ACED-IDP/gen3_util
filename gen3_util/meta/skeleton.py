import pathlib
import sys

from fhir.resources.attachment import Attachment
from fhir.resources.documentreference import DocumentReference
from fhir.resources.fhirtypes import DocumentReferenceContentType
from fhir.resources.identifier import Identifier
from fhir.resources.observation import Observation
from fhir.resources.patient import Patient
from fhir.resources.researchstudy import ResearchStudy
from fhir.resources.researchsubject import ResearchSubject
from fhir.resources.resource import Resource
from fhir.resources.specimen import Specimen
from fhir.resources.task import Task, TaskOutput, TaskInput
from orjson import orjson

from gen3_util import Config
from gen3_util.common import EmitterContextManager, create_id, Push, read_meta_index
from gen3_util.config import ensure_auth
from gen3_util.files.lister import ls
from gen3_util.files.manifest import ls as manifest_ls
from gen3_util.meta import directory_reader


def update_document_reference(document_reference, index_record):
    """Update document reference with index record."""
    assert document_reference.resource_type == 'DocumentReference'
    assert 'did' in index_record, f"index_record missing did: {index_record}"
    assert index_record['did'] == document_reference.id, f"{index_record['did']} != {document_reference.id}"
    assert 'created_date' in index_record, f"index_record missing created_date: {index_record}"
    document_reference.docStatus = 'final'
    document_reference.status = 'current'
    if 'T' not in index_record['created_date']:
        document_reference.date = index_record['created_date'] + "Z"
    else:
        document_reference.date = index_record['created_date']
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


def study_metadata(config: Config, project_id: str, output_path: str, overwrite: bool, source: str, auth=None) -> list[str]:
    """Read files uploaded to indexd or the local manifest and create a skeleton graph for document and ancestors from a set of identifiers.

    Args:
        config:
        project_id:
        output_path:
        overwrite: check for existing records and skip if found
        source: indexd or manifest
        auth: auth provider
    """

    project_id = config.gen3.project_id
    assert project_id.count('-') == 1, "project_id must be of the form program-project"
    logs = []
    if not auth:
        if config.gen3.profile:
            auth = ensure_auth(config=config)
    if not auth:
        logs.append("Running in disconnected mode.")

    existing_resource_ids = set()
    if not overwrite:
        # print(f"Checking remote for existing records for project_id:{project_id}...", file=sys.stderr)
        # nodes = meta_nodes(config, project_id, auth=auth)  # fetches all nodes by default
        nodes = []
        print(f"Retrieved {len(nodes)} from remote.", file=sys.stderr)
        len_nodes = len(nodes)
        print(f"Checking {output_path} and pending commits...", file=sys.stderr)
        nodes.extend(Push(config=config).pending_meta_index())
        for parse_result in directory_reader(output_path):
            nodes.append({'id': parse_result.resource.id, 'type': parse_result.resource.resource_type})
        len_nodes = len(nodes) - len_nodes
        print(f"Retrieved {len_nodes} locally.", file=sys.stderr)
        existing_resource_ids = set([_['id'] for _ in nodes])

    # get file client
    if source == 'indexd':
        # get all records for project_id
        records = ls(config, metadata={'project_id': project_id})['records']
    elif source == 'manifest':
        # get all records already uploaded from current manifest
        # object_id = ','.join([_['object_id'] for _ in manifest_ls(config, project_id=project_id)])
        # records = ls(config, object_id=object_id)['records']
        records = manifest_to_indexd(config, project_id)
    else:
        raise ValueError(f"source must be 'indexd' or 'manifest' not {source}")

    with EmitterContextManager(output_path, file_mode="a+") as emitter:
        if len(records) == 0:
            print(f"No records found for project_id:{project_id}", file=sys.stderr)
        new_record_count = 0
        existing_record_count = 0
        for _ in records:
            resources = create_skeleton(config=config, metadata=_['metadata'], indexd_record=_)
            for resource in resources:

                # check document references
                if resource.id in existing_resource_ids:
                    existing_record_count += 1
                    continue

                existing_resource_ids.add(resource.id)

                emitter.emit(resource.resource_type).write(
                    resource.json(option=orjson.OPT_APPEND_NEWLINE)
                )
                new_record_count += 1

        # print(f"Of {len(records)} records in {source}, {existing_record_count} already existed, wrote {new_record_count} new records.", file=sys.stderr)
        logs.append(f"Created {new_record_count} new records.")

    return logs


def manifest_to_indexd(config, project_id):
    """Retrieve manifest, xform manifest records to indexd records."""
    records = [{'did': _['object_id'], 'metadata': _} for _ in manifest_ls(config, project_id=project_id)]
    for _ in records:
        transform_manifest_to_indexd(_, project_id)
    return records


def transform_manifest_to_indexd_keys(metadata: dict):
    """Transform manifest record to indexd record."""
    new_keys = {}
    for k, v in metadata.items():
        if k in ['object_id', 'project_id']:
            new_keys[k] = v
            continue
        if k.endswith('_id'):
            new_key = k.replace('_id', '_identifier')
            new_keys[new_key] = v
        else:
            new_keys[k] = v

    return new_keys


def transform_manifest_to_indexd(_: dict, project_id: str) -> dict:
    """Transform manifest record to indexd record."""
    old_keys = []
    new_keys = {'document_reference_id': _['did']}

    metadata = _['metadata']

    for k, v in metadata.items():
        if k in ['object_id', 'project_id']:
            continue
        if k.endswith('_id'):
            new_key = k.replace('_id', '_identifier')
            new_keys[new_key] = v
            old_keys.append(k)
    for k in old_keys:
        del _['metadata'][k]

    _['metadata'].update(new_keys)
    _['metadata']['project_id'] = project_id
    _['created_date'] = _['metadata']['modified']
    del _['metadata']['modified']
    _['hashes'] = {'md5': _['metadata']['md5']}
    del _['metadata']['md5']
    _['file_name'] = _['metadata']['file_name']
    del _['metadata']['file_name']
    _['size'] = _['metadata']['size']
    del _['metadata']['size']
    return _


def _get_system(identifier: str, project_id: str):
    """Return system component of simplified identifier"""
    if '#' in identifier:
        return identifier.split('#')[0]
    if '|' in identifier:
        return identifier.split('|')[0]
    # default
    return f"https://aced-idp.org/{project_id}"


def create_skeleton(metadata: dict, indexd_record: dict, config: Config = None) -> list[Resource]:  # TODO fix caller auth
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

    if not document_reference_id:
        return []
        # metadata can include submission files, etc. that are not attached to a document reference ie is_metadata = True

    assert project_id, "project_id required"
    assert project_id.count('-') == 1, "project_id must be of the form program-project"
    program, project = project_id.split('-')

    already_created = set()
    if config:
        existing_meta_index = {_['id']: _['md5'] for _ in read_meta_index(config.state_dir)}
        pending_meta_index = {_['id']: _['md5'] for _ in Push(config=config).pending_meta_index()}
        already_created = set(existing_meta_index.keys())
        already_created.update(pending_meta_index.keys())

    # create entities
    research_study = research_subject = observation = specimen = patient = task = None

    # _existing_research_study = meta_resource(submission_client=submission_client, identifier=None, project_id=project_id, gen3_type='research_study')
    _existing_research_study = None
    if not _existing_research_study:
        research_study = ResearchStudy(status='active')
        research_study.description = f"Skeleton ResearchStudy for {project_id}"
        research_study.identifier = [
            Identifier(value=project_id, system=_get_system(project_id, project_id=project_id),
                       use='official')]
        research_study.id = create_id(research_study, project_id)
        research_study_id = research_study.id

    else:
        research_study_id = _existing_research_study['id']

    document_reference = DocumentReference(status='current', content=[{'attachment': {'url': "file://"}}])
    document_reference.id = document_reference_id
    document_reference.identifier = [
        Identifier(value=document_reference_id, system=_get_system(document_reference_id, project_id=project_id),
                   use='official')]
    update_document_reference(document_reference, indexd_record)

    if patient_identifier:
        # _existing_patient = meta_resource(submission_client=submission_client, identifier=patient_identifier, project_id=project_id, gen3_type='patient')
        _existing_patient = None
        if not _existing_patient:
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

    if observation_identifier:
        # _existing_observation = meta_resource(submission_client=submission_client, identifier=observation_identifier, project_id=project_id, gen3_type='observation')
        _existing_observation = None
        if not _existing_observation:
            assert patient, "patient required for observation"
            observation = Observation(status='final', code={'text': 'unknown'})
            observation.identifier = [Identifier(value=observation_identifier, system=_get_system(observation_identifier, project_id=project_id), use='official')]
            observation.subject = {'reference': f"Patient/{patient.id}"}
            observation.id = create_id(observation, project_id)

    discard_specimen = False
    if specimen_identifier:
        # _existing_specimen = meta_resource(submission_client=submission_client, identifier=specimen_identifier, project_id=project_id, gen3_type='specimen')
        _existing_specimen = None
        if not _existing_specimen:
            specimen = Specimen()
            specimen.identifier = [Identifier(value=specimen_identifier, system=_get_system(specimen_identifier, project_id=project_id), use='official')]
            specimen.id = create_id(specimen, project_id)
            if specimen.id in already_created:
                # no need to create another one
                discard_specimen = True
            else:
                assert patient, "patient required for specimen"
                specimen.subject = {'reference': f"Patient/{patient.id}"}

    if task_identifier:
        # _existing_task = meta_resource(submission_client=submission_client, identifier=task_identifier, project_id=project_id, gen3_type='task')
        _existing_task = None
        if not _existing_task:
            task = Task(intent='unknown', status='completed')
            task.identifier = [Identifier(value=task_identifier, system=_get_system(task_identifier, project_id=project_id), use='official')]
            task.id = create_id(task, project_id)

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
        document_reference.subject = {'reference': f"Observation/{observation.id}"}
    if specimen and not document_reference.subject:
        document_reference.subject = {'reference': f"Specimen/{specimen.id}"}
    if patient and not document_reference.subject:
        document_reference.subject = {'reference': f"Patient/{patient.id}"}
    if not document_reference.subject:
        document_reference.subject = {'reference': f"ResearchStudy/{research_study.id}"}

    if discard_specimen:
        specimen = None
    return [_ for _ in [research_study, research_subject, patient, observation, specimen, task, document_reference] if _]
