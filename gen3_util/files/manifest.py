import json
import logging
import mimetypes
import pathlib
import sqlite3
import subprocess
import uuid
from datetime import datetime

import orjson
import requests
from pydantic.json import pydantic_encoder
import sys

from gen3_util.buckets import get_program_bucket
from gen3_util.config import Config, gen3_services
from gen3_util.files.uploader import _normalize_file_url
from gen3_util.meta import ACED_NAMESPACE
from gen3_util.meta.importer import md5sum

try:
    import magic
except ImportError as e:
    print(f"Requires libmagic installed on your system to determine file mime-types\nError: '{e}'\nFor installation instructions see https://github.com/ahupp/python-magic#installation")
    sys.exit(1)

logger = logging.getLogger(__name__)


def put(config: Config, file_name: str, project_id: str, md5: str):
    """Create manifest entry for a file."""
    object_name = _normalize_file_url(file_name)
    file = pathlib.Path(file_name)
    assert file.is_file(), f"{file} is not a file"

    assert project_id, "project_id is missing"
    assert project_id.count('-') == 1, f"{project_id} should have a single '-' delimiter."

    stat = file.stat()
    _magic = magic.Magic(mime=True, uncompress=True)  # https://github.com/ahupp/python-magic#installation

    mime, encoding = mimetypes.guess_type(file)
    if not mime:
        mime = _magic.from_file(file)

    object_id = str(uuid.uuid5(ACED_NAMESPACE, project_id + f"::{file}"))

    if not md5:
        md5 = md5sum(file)

    return {
        "object_id": object_id,
        "file_name": object_name,
        "size": stat.st_size,
        "modified": datetime.fromtimestamp(stat.st_mtime),
        "md5": md5,
        "mime_type": mime,
    }


def save(config: Config, project_id: str, generator):
    """Write to local sqlite."""
    connection = sqlite3.connect(config.state_dir / 'manifest.sqlite')
    with connection:
        connection.execute('CREATE TABLE if not exists manifest (object_id PRIMARY KEY, project_id Text, entity Text)')
        with connection:
            connection.executemany('INSERT OR REPLACE into manifest values (?, ?, ?)',
                                   [(
                                       _['object_id'],
                                       project_id,
                                       orjson.dumps(
                                           _, default=pydantic_encoder
                                       ).decode()
                                     ) for _ in generator])


def ls(config: Config, project_id: str, object_id: str):
    """Read from local sqlite."""
    connection = sqlite3.connect(config.state_dir / 'manifest.sqlite')
    with connection:
        cursor = connection.cursor()
        if object_id:
            cursor.execute('SELECT entity from manifest where object_id = ?', (object_id,))
        else:
            cursor.execute('SELECT entity from manifest where project_id = ?', (project_id,))
        return [orjson.loads(_[0]) for _ in cursor.fetchall()]


def upload_indexd(config: Config, project_id: str, object_id: str = None, duplicate_check: bool = False) -> list[dict]:
    """Save manifest to indexd, returns list of manifest entries"""

    manifest_entries = []

    assert project_id, "project_id is missing"
    assert project_id.count('-') == 1, f"{project_id} should have a single '-' delimiter."
    program, project = project_id.split('-')

    file_client, index_client, user, auth = gen3_services(config=config)

    bucket_name = get_program_bucket(config, program, auth=auth)
    assert bucket_name, f"could not find bucket for {program}"

    for _ in ls(config, project_id=project_id, object_id=object_id):
        # _['config'] = config
        _['project_id'] = project_id
        program, project = project_id.split('-')

        # SYNC
        existing_record = None
        hashes = {'md5': _['md5']}
        metadata = {
            **{
                'document_reference_id': _['object_id'],
                'specimen_identifier': _['specimen_id'],
                'patient_identifier': _['patient_id'],
                'task_identifier': _['task_id'],
                'observation_identifier': _['observation_id'],
                'project_id': f'{program}-{project}',
            },
            **hashes}

        if duplicate_check:
            try:
                existing_record = index_client.get_record(guid=_["object_id"])
            except Exception: # noqa
                pass
            if existing_record:
                md5_match = existing_record['hashes']['md5'] == _['md5']
                if md5_match:
                    # SYNC
                    logger.info(f"Deleting existing record {_['object_id']}")
                    index_client.delete_record(guid=_['object_id'])
                    existing_record = None
                else:
                    logger.info(f"NOT DELETING, MD5 didn't match existing record {_['object_id']} existing_record_md5:{existing_record['hashes']['md5']} manifest_md5:{_['md5']}")
        if not existing_record:
            try:
                response = index_client.create_record(
                    did=_["object_id"],
                    hashes=hashes,
                    size=_["size"],
                    authz=[f'/programs/{program}/projects/{project}'],
                    file_name=_['file_name'],
                    metadata=metadata,
                    urls=[f"s3://{bucket_name}/{_['object_id']}"]  # TODO make a DRS URL
                )
                assert response, "Expected response from indexd create_record"
            except (requests.exceptions.HTTPError, AssertionError) as e:
                if not ('already exists' in str(e)):
                    raise e
                logger.info(f"indexd record already exists, continuing upload. {_['object_id']}")

            manifest_entries.append(_)
    return manifest_entries


def upload_files(config: Config, manifest_entries: list[dict], project_id: str, profile: str, upload_path: str) -> subprocess.CompletedProcess:
    """Upload files to gen3 via gen3-client."""

    assert project_id, "project_id is missing"
    assert project_id.count('-') == 1, f"{project_id} should have a single '-' delimiter."
    program, project = project_id.split('-')

    assert profile, "profile is missing"

    file_client, index_client, user, auth = gen3_services(config=config)
    bucket_name = get_program_bucket(config, program, auth=auth)
    assert bucket_name, f"could not find bucket for {program}"

    manifest_path = config.state_dir / f"{project_id}.manifest.json"

    with open(manifest_path, 'w') as manifest_file:
        json.dump(manifest_entries, manifest_file)

    cmd = f"gen3-client upload-multiple --manifest {manifest_path} --profile {profile} --upload-path {upload_path} --bucket {bucket_name}".split()
    upload_results = subprocess.run(cmd)
    assert upload_results.returncode == 0, upload_results
    return upload_results


def rm(config: Config, project_id: str, object_id: str):
    """Remove manifest entry from local sqlite."""
    connection = sqlite3.connect(config.state_dir / 'manifest.sqlite')
    with connection:
        if object_id:
            connection.execute('DELETE from manifest where object_id = ?', (object_id,))
        else:
            connection.execute('DELETE from manifest where project_id = ?', (project_id,))
