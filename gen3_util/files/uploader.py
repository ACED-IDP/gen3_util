
import base64
import datetime
import logging
import pathlib
import re
import urllib
from dataclasses import dataclass
from typing import List
from urllib.parse import urlparse

import requests
from orjson import orjson
from pydantic import BaseModel

from gen3_util.config import Config, gen3_services
from gen3_util.meta.importer import md5sum

logger = logging.getLogger(__name__)


def _upload_file_to_signed_url(file_name, md5sum, metadata, signed_url):
    """Upload file to bucket.  Provided here for environments where gen3-client is not installed.

     For example, in the job container, only an access token is available"""

    # When you use this header, Amazon S3 checks the object against the provided MD5 value and,
    # if they do not match, returns an error.
    content_md5 = base64.b64encode(bytes.fromhex(md5sum))
    headers = {'Content-MD5': content_md5}
    # attach our metadata to s3 object
    for key, value in metadata.items():
        headers[f"x-amz-meta-{key}"] = value

    with open(file_name, 'rb') as fp:
        # SYNC
        response = requests.put(signed_url, data=fp)
        response_text = response.text
        assert response.status_code == 200, (signed_url, response_text)


def update_indexd(attachment, bucket_name, document_reference, duplicate_check, index_client, md5, object_name,
                  program, project, metadata=None, specimen_id=None, patient_id=None, task_id=None, observation_id=None):
    hashes = {'md5': md5}
    assert 'id' in document_reference, document_reference
    guid = document_reference['id']
    if metadata is None:
        metadata = {
            **{
                'document_reference_id': guid,
                'specimen_identifier': specimen_id,
                'patient_identifier': patient_id,
                'task_identifier': task_id,
                'observation_id': observation_id,
                'project_id': f'{program}-{project}',
            },
            **hashes}
    # trim None values
    metadata = {k: v for k, v in metadata.items() if v is not None}

    # SYNC
    existing_record = None
    s3_url = f"s3://{bucket_name}/{guid}/{object_name}"
    if duplicate_check:
        try:
            existing_record = index_client.get_record(guid=document_reference["id"])
        except Exception: # noqa
            pass
        if existing_record:
            skip_delete = all([
                existing_record['hashes']['md5'] == md5sum,
                s3_url in existing_record['urls']
            ])
            if not skip_delete:
                # SYNC
                logger.info(f"Deleting existing record {document_reference['id']}")
                index_client.delete_record(guid=document_reference["id"])
                existing_record = None
    if not existing_record:
        try:
            _ = index_client.create_record(
                did=document_reference["id"],
                hashes=hashes,
                size=attachment["size"],
                authz=[f'/programs/{program}/projects/{project}'],
                file_name=object_name,
                metadata=metadata,
                urls=[s3_url]  # TODO make a DRS URL
            )
        except (requests.exceptions.HTTPError, AssertionError) as e:
            if not ('already exists' in str(e)):
                raise e
            logger.info(f"indexd record already exists, continuing upload. {document_reference['id']}")
    return metadata


def _extract_source_path(attachment, source_path, source_path_extension):
    if source_path:
        source_path = pathlib.Path(source_path)
        assert source_path.is_dir(), f"Path is not a directory {source_path}"
        source_path = source_path / attachment['url'].lstrip('./').lstrip('file:///')
    else:
        assert len(source_path_extension) == 1, "Missing source_path extension."
        source_path = source_path_extension[0]['valueUrl']
    return source_path


def _extract_extensions(document_reference):
    """Extract useful data from document_reference."""
    attachment = document_reference['content'][0]['attachment']
    md5_extension = [_ for _ in attachment['extension'] if
                     _['url'] == "http://aced-idp.org/fhir/StructureDefinition/md5"]
    assert len(md5_extension) == 1, "Missing MD5 extension."
    md5sum = md5_extension[0]['valueString']
    source_path_extension = [_ for _ in attachment['extension'] if
                             _['url'] == "http://aced-idp.org/fhir/StructureDefinition/source_path"]
    return attachment, md5sum, source_path_extension


def _validate_parameters(from_: str) -> pathlib.Path:

    assert len(urlparse(from_).scheme) == 0, f"{from_} appears to be an url. url to url cp not supported"

    from_ = pathlib.Path(from_)
    assert from_.parent.exists(), f"{from_.parent} is not a directory"
    assert from_.exists(), f"{from_} does not exist"

    return from_


class UploaderResults(BaseModel):
    info: List[str] = []
    """Logging"""
    incomplete: List[str] = []
    """Logging"""
    errors: List[str] = []
    """Logging"""
    object_id: str = None
    """The object_id of the uploaded file."""


@dataclass
class UploadResult:
    """Results of Upload DocumentReference."""
    document_reference: dict
    """The source document reference."""
    elapsed: [datetime.timedelta, None]
    """Amount of time it took to upload."""
    exception: Exception = None
    """On error."""


def _normalize_file_url(path: str) -> str:
    """Strip leading ./ and file:/// from file urls."""
    path = re.sub(r'^file:\/\/\/', '', path)
    path = re.sub(r'^\.\/', '', path)
    return path


def _upload_document_reference(config: Config, document_reference: dict, bucket_name: str,
                               program: str, project: str, duplicate_check: bool,
                               source_path: str) -> UploadResult:
    """Write a single document reference to indexd and upload file."""

    try:
        start = datetime.datetime.now()

        file_client, index_client, user, auth = gen3_services(config=config)

        attachment, md5sum, source_path_extension = _extract_extensions(document_reference)

        source_path = _extract_source_path(attachment, source_path, source_path_extension)

        file_name = _normalize_file_url(source_path)
        object_name = _normalize_file_url(attachment['url'])

        metadata = update_indexd(attachment, bucket_name, document_reference, duplicate_check, index_client, md5sum,
                                 object_name, program, project)

        # create a record in gen3 using document_reference's id as guid, get a signed url
        # SYNC

        # if document_reference['id'] == '0d1a3766-e179-5e94-8bcb-a2563542d13a':
        #     # print('sleeping for test')
        #     raise Exception("Simulated Failure")

        document = file_client.upload_file_to_guid(guid=document_reference['id'], file_name=object_name, bucket=bucket_name)
        assert 'url' in document, document
        signed_url = urllib.parse.unquote(document['url'])

        file_name = pathlib.Path(file_name)

        _upload_file_to_signed_url(file_name, md5sum, metadata, signed_url)

        end = datetime.datetime.now()
        # print(('complete', document_reference['id'], end.isoformat(), end-start, attachment["size"]))

        return UploadResult(document_reference, end - start)
    except Exception as e:  # noqa
        return UploadResult(document_reference, None, e)


def _ensure_already_uploaded(ignore_state, state_file):
    already_uploaded = set()
    if not ignore_state:
        if state_file.exists():
            with open(state_file, "rb") as fp:
                for _ in fp.readlines():
                    state = orjson.loads(_)
                    already_uploaded.update([_ for _ in state['completed']])
                # print("loaded state")
    return already_uploaded
