import logging
from socket import socket
from urllib.parse import urlparse

import requests
from gen3.auth import Gen3Auth
from gen3.index import Gen3Index

from gen3_tracker.git import DVC, DVCMeta


def write_indexd(auth: Gen3Auth,
                 project_id: str,
                 dvc: DVC,
                 bucket_name: str,
                 overwrite: bool,
                 restricted_project_id: str,
                 existing_records: list[str] = [],
                 message: str = None) -> bool:
    """Write manifest entry to indexd."""
    assert auth, "Expected auth"
    assert project_id, "Expected project_id"
    index_client = Gen3Index(auth)
    program, project = project_id.split('-')
    logger = logging.getLogger(__name__)
    dvc.project_id = project_id

    # SYNC
    existing_record = None
    hashes, metadata = create_hashes_metadata(dvc, program, project)

    if message:
        metadata['message'] = message

    if overwrite:
        existing_record = dvc.object_id in existing_records
        if existing_record:
            # SYNC
            # print(f"Deleting existing record {dvc.object_id}")
            index_client.delete_record(guid=dvc.object_id)
            existing_record = False

    authz = [f'/programs/{program}/projects/{project}']
    if restricted_project_id:
        _ = restricted_project_id.split('-')
        authz.append(f'/programs/{_[0]}/projects/{_[1]}')

    # strip any file:/// prefix
    dvc.out.path = urlparse(dvc.out.path).path

    # We need this for symlinked files
    if dvc.out.realpath:
        metadata['realpath'] = urlparse(dvc.out.realpath).path

    if not existing_record:
        try:
            file_name = dvc.out.path
            urls = [f"s3://{bucket_name}/{dvc.object_id}/{file_name}"]
            if dvc.meta.no_bucket:
                hostname = socket.gethostname()
                _ = f"{hostname}/{metadata['realpath']}".replace('//', '/')
                urls = [f"scp://{_}"]
            if dvc.out.source_url:
                urls = [dvc.out.source_url]

            # print(f"Writing indexd record for {dvc.object_id} {urls}")
            response = index_client.create_record(
                did=dvc.object_id,
                hashes=hashes,
                size=dvc.out.size,
                authz=authz,
                file_name=file_name,
                metadata=metadata,
                urls=urls
            )
            assert response, "Expected response from indexd create_record"

        except (requests.exceptions.HTTPError, AssertionError) as e:
            if 'already exists' in str(e):
                logger.error(
                    f"indexd record already exists, consider using --overwrite. {dvc.object_id} {str(e)}")
            raise e
    return True


def create_hashes_metadata(dvc: DVC, program, project):
    meta: DVCMeta = dvc.meta

    if not meta:
        meta = DVCMeta()

    hashes = {dvc.out.hash: getattr(dvc.out, dvc.out.hash)}
    metadata = {
        **{
            'document_reference_id': dvc.object_id,
            'specimen_identifier': meta.specimen,
            'patient_identifier': meta.patient,
            'task_identifier': meta.task,
            'observation_identifier': meta.observation,
            'project_id': f'{program}-{project}',
            'no_bucket': meta.no_bucket,
        },
        **hashes}
    return hashes, metadata
