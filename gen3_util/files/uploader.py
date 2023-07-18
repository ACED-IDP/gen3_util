
import base64
import datetime
import logging
import pathlib
import re
import urllib
from dataclasses import dataclass
from multiprocessing import Pool
from time import sleep
from typing import List
from urllib.parse import urlparse, ParseResult

import requests
from orjson import orjson
from pydantic import BaseModel
from tqdm import tqdm

from gen3_util.common import read_ndjson_file
from gen3_util.config import Config, gen3_services
from gen3_util.files import assert_valid_project_id, assert_valid_bucket

logger = logging.getLogger(__name__)


def _upload_file_to_signed_url(file_name, md5sum, metadata, signed_url):
    """Upload file """

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


def _update_indexd(attachment, bucket_name, document_reference, duplicate_check, index_client, md5sum, object_name,
                   program, project, metadata=None):
    hashes = {'md5': md5sum}
    assert 'id' in document_reference, document_reference
    guid = document_reference['id']
    if metadata is None:
        metadata = {
            **{
                'datanode_type': 'DocumentReference',
                'datanode_object_id': guid
            },
            **hashes}
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


def _validate_parameters(from_: str, to_: str) -> (pathlib.Path, ParseResult):
    url = urlparse(to_)
    assert url.scheme, f"{to_} does not appear to be a url"

    assert len(urlparse(from_).scheme) == 0, f"{from_} appears to be an url. url to url cp not supported"

    from_ = pathlib.Path(from_)
    assert from_.parent.exists(), f"{from_.parent} is not a directory"
    assert from_.exists(), f"{from_} does not exist"

    return from_, url


class UploaderResults(BaseModel):
    info: List[str] = []
    """Logging"""
    incomplete: List[str] = []
    """Logging"""
    errors: List[str] = []
    """Logging"""


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
    path = re.sub(r'^\.\/', '', path)
    path = re.sub(r'^file:\/\/\/', '', path)
    return path


def _upload_document_reference(config: Config, document_reference: dict, bucket_name: str,
                               program: str, project: str, duplicate_check: bool,
                               source_path: str) -> UploadResult:
    """Write a single document reference to indexd and upload file."""

    try:
        start = datetime.datetime.now()

        file_client, index_client, user = gen3_services(config=config)

        attachment, md5sum, source_path_extension = _extract_extensions(document_reference)

        source_path = _extract_source_path(attachment, source_path, source_path_extension)

        file_name = _normalize_file_url(source_path)
        object_name = _normalize_file_url(attachment['url'])

        metadata = _update_indexd(attachment, bucket_name, document_reference, duplicate_check, index_client, md5sum,
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


def cp(config: Config, from_: str, to_: str, ignore_state: bool, worker_count: int, project_id: str,
       source_path: str, disable_progress_bar: bool, duplicate_check: bool) -> UploaderResults:
    """Copy files from local file system to bucket"""

    document_reference_path, to_ = _validate_parameters(from_, to_)
    bucket_name = to_.hostname

    assert_valid_project_id(config, project_id)

    assert_valid_bucket(config, bucket_name)

    program, project = project_id.split('-')

    state_file = config.state_dir / "state.ndjson"

    state_file.parent.mkdir(parents=True, exist_ok=True)
    assert state_file.parent.exists(), f"{state_file.parent} does not exist"

    # all attempts ids are incomplete until they succeed
    incomplete = set()
    # key:document_reference.id of completed file transfers
    completed = set()
    # key:document_reference.id  of failed file transfers
    exceptions = {}

    info = []

    document_reference_lookup = {}

    # progress bar control
    document_references_size = 0
    # loop control
    document_references_length = 0

    already_uploaded = _ensure_already_uploaded(ignore_state, state_file)

    for _ in read_ndjson_file(document_reference_path):
        if _['id'] not in already_uploaded:
            incomplete.add(_['id'])
            document_references_size += _['content'][0]['attachment']['size']
            document_references_length += 1
        else:
            info.append(f"{_['id']} already uploaded, skipping")
    # print("loaded manifest from document references")

    # process a chunk at a time
    with Pool(processes=worker_count) as pool:
        results = []
        for document_reference in read_ndjson_file(document_reference_path):
            if document_reference['id'] in already_uploaded:
                continue
            result = pool.apply_async(
                func=_upload_document_reference,
                args=(
                    config,
                    document_reference,
                    bucket_name,
                    program,
                    project,
                    duplicate_check,
                    source_path
                )
            )
            results.append(result)
            document_reference_lookup[id(result)] = document_reference['id']

        # close the process pool
        pool.close()

        # poll the results every sec.
        with tqdm(total=document_references_size, unit='B', disable=disable_progress_bar,
                  unit_scale=True, unit_divisor=1024) as pbar:
            while True:
                results_to_remove = []
                for record in results:
                    if record.ready() and record.successful():
                        r = record.get()
                        if r.exception:
                            exceptions[r.document_reference['id']] = {
                                'exception': str(r.exception),
                                'document_reference': {
                                    'id': r.document_reference
                                }
                            }
                        elif r.document_reference['id'] not in completed:
                            completed.add(r.document_reference['id'])
                            incomplete.remove(r.document_reference['id'])

                        results_to_remove.append(record)
                        document_references_length = document_references_length - 1
                        pbar.set_postfix(file=f"{r.document_reference['id'][-6:]}", elapsed=f"{r.elapsed}")
                        pbar.update(r.document_reference['content'][0]['attachment']['size'])
                        sleep(.1)  # give screen a chance to refresh

                    if record.ready() and not record.successful():
                        print('record.ready() and not record.successful()')
                        document_reference_id = document_reference_lookup[id(record)]
                        try:
                            record.get()
                        except Exception as e:  # noqa
                            if document_reference_id not in exceptions:
                                exceptions[document_reference_id] = {
                                    'exception': str(e),
                                    'document_reference': {
                                        'id': document_reference_id
                                    }
                                }
                                document_references_length = document_references_length - 1

                if document_references_length == 0:
                    break

                sleep(1)

                # using list comprehension to cull processed results
                results = [_ for _ in results if _ not in results_to_remove]

        with open(state_file, "a+b") as fp:
            fp.write(orjson.dumps(
                {
                    'timestamp': datetime.datetime.now().isoformat(),
                    'completed': [_ for _ in completed],
                    'incomplete': [_ for _ in incomplete],
                    'exceptions': exceptions
                },
                option=orjson.OPT_APPEND_NEWLINE
            ))

        info.append(f"Wrote state to {state_file}")

    results = {
        'info': info,
        'incomplete': [_ for _ in incomplete],
        'errors': [f"{k}: {str(v['exception'])}" for k, v in exceptions.items()],
    }
    return UploaderResults(**results)


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
