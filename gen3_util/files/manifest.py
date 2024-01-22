import json
import logging
import pathlib
import sqlite3
import subprocess
import sys
import uuid
from datetime import datetime, time
import multiprocessing
# from multiprocessing.pool import Pool
from urllib.parse import urlparse

import orjson
import requests
from gen3.index import Gen3Index
from pydantic.json import pydantic_encoder

from gen3_util.buckets import get_program_bucket
from gen3_util.common import Commit
from gen3_util.config import Config, gen3_services, ensure_auth
from gen3_util.files import get_mime_type
from gen3_util.files.uploader import _normalize_file_url
from gen3_util import ACED_NAMESPACE
from gen3_util.meta.importer import md5sum
from tqdm import tqdm

logger = logging.getLogger(__name__)


def _get_connection(config: Config, commit_id: str = None):
    """Return sqlite connection, ensure table exists."""
    config.state_dir.mkdir(parents=True, exist_ok=True)
    sqllite_path = config.state_dir / 'manifest.sqlite'
    if commit_id:
        sqllite_path = config.state_dir / config.gen3.project_id / 'commits' / commit_id / 'manifest.sqlite'
    _connection = sqlite3.connect(sqllite_path)
    with _connection:
        _connection.execute('CREATE TABLE if not exists manifest (object_id PRIMARY KEY, project_id Text, entity Text)')
    return _connection


def put(config: Config, file_name: str, project_id: str, md5: str):
    """Create manifest entry for a file."""
    object_name = _normalize_file_url(file_name)
    file = pathlib.Path(object_name)
    assert file.is_file(), f"{file} is not a file"

    assert project_id, "project_id is missing"
    assert project_id.count('-') == 1, f"{project_id} should have a single '-' delimiter."

    stat = file.stat()
    mime = get_mime_type(file)

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


def save(config: Config, project_id: str, generator, max_retries: int = 3, base_delay=2):
    """Write to local sqlite."""
    for retry_count in range(max_retries):
        try:
            connection = _get_connection(config)
            with connection:
                connection.executemany('INSERT OR REPLACE into manifest values (?, ?, ?)',
                                       [(
                                           _['object_id'],
                                           project_id,
                                           orjson.dumps(
                                               _, default=pydantic_encoder
                                           ).decode()
                                         ) for _ in generator])
            return
        except sqlite3.OperationalError as e:
            print(f"Error locking database: {e}", file=sys.stderr)
            # Calculate the delay for the next retry using exponential backoff
            delay = base_delay * 2**retry_count
            print(f"Retrying in {delay} seconds...", file=sys.stderr)
            # Wait before the next retry
            time.sleep(delay)
        raise Exception(f"Failed to lock database after {max_retries} retries.")


def ls(config: Config, project_id: str, object_id: str = None, commit_id: str = None):
    """Read from local sqlite."""
    connection = _get_connection(config, commit_id=commit_id)
    with connection:
        cursor = connection.cursor()
        if object_id:
            cursor.execute('SELECT entity from manifest where object_id = ?', (object_id,))
        else:
            cursor.execute('SELECT entity from manifest where project_id = ?', (project_id,))
        return [orjson.loads(_[0]) for _ in cursor.fetchall()]


def _write_indexd(index_client,
                  project_id: str,
                  manifest_item: dict,
                  bucket_name: str,
                  duplicate_check: bool,
                  restricted_project_id: str,
                  existing_records: list[str] = [],
                  message: str = None) -> bool:
    """Write manifest entry to indexd."""
    manifest_item['project_id'] = project_id
    program, project = project_id.split('-')

    # SYNC
    existing_record = None
    hashes, metadata = create_hashes_metadata(manifest_item, program, project)

    if message:
        metadata['message'] = message

    if duplicate_check:
        try:
            existing_record = manifest_item["object_id"] in existing_records
        except Exception:  # noqa
            pass
        if existing_record:
            # SYNC
            logger.debug(f"Deleting existing record {manifest_item['object_id']}")
            index_client.delete_record(guid=manifest_item['object_id'])
            existing_record = False

    authz = [f'/programs/{program}/projects/{project}']
    if restricted_project_id:
        _ = restricted_project_id.split('-')
        authz.append(f'/programs/{_[0]}/projects/{_[1]}')

    # strip any file:/// prefix
    manifest_item['file_name'] = urlparse(manifest_item['file_name']).path

    if not existing_record:
        try:
            file_name = manifest_item['remote_path'] or manifest_item['file_name']
            response = index_client.create_record(
                did=manifest_item["object_id"],
                hashes=hashes,
                size=manifest_item["size"],
                authz=authz,
                file_name=file_name,
                metadata=metadata,
                urls=[f"s3://{bucket_name}/{manifest_item['object_id']}/{file_name}"]
            )
            assert response, "Expected response from indexd create_record"
        except (requests.exceptions.HTTPError, AssertionError) as e:
            if 'already exists' in str(e):
                logger.error(f"indexd record already exists, consider using --duplicate_check. {manifest_item['object_id']} {str(e)}")
            raise e
    return True


def create_hashes_metadata(manifest_item, program, project):
    hashes = {'md5': manifest_item['md5']}
    metadata = {
        **{
            'document_reference_id': manifest_item['object_id'],
            'specimen_identifier': manifest_item.get('specimen_id', None),
            'patient_identifier': manifest_item.get('patient_id', None),
            'task_identifier': manifest_item.get('task_id', None),
            'observation_identifier': manifest_item.get('observation_id', None),
            'project_id': f'{program}-{project}',
        },
        **hashes}
    return hashes, metadata


def worker_count():
    """Return number of workers for multiprocessing."""
    return multiprocessing.cpu_count() - 1


def upload_commit_to_indexd(config: Config, commit: Commit, overwrite_index: bool = False,
                            restricted_project_id: str = None, auth=None) -> list[dict]:
    """Save manifest to indexd, returns list of manifest entries"""
    if restricted_project_id:
        assert restricted_project_id.count('-') == 1, f"{restricted_project_id} should have a single '-' delimiter."
    if not auth:
        auth = ensure_auth(config=config)
    assert commit.message, f"commit.message is missing {commit}"

    program, project = config.gen3.project_id.split('-')
    bucket_name = get_program_bucket(config, program, auth=auth)
    assert bucket_name, f"could not find bucket for {program}"
    index_client = Gen3Index(auth_provider=auth)
    _generator = ls(config, project_id=config.gen3.project_id, commit_id=commit.commit_id)
    # see if there are existing records
    dids = [_['object_id'] for _ in _generator]
    if len(dids) == 0:
        # print(f"INFO No files to upload for {commit.commit_id}", file=sys.stderr)
        return []
    existing_records = index_client.get_records(dids=dids)
    if existing_records:
        existing_records = [_['did'] for _ in existing_records]
    print(f"Found {len(existing_records)} existing records")
    manifest_entries = []
    for manifest_item in tqdm(_generator):
        _ = _write_indexd(
            index_client=index_client,
            project_id=config.gen3.project_id,
            manifest_item=manifest_item,
            bucket_name=bucket_name,
            duplicate_check=overwrite_index,
            restricted_project_id=restricted_project_id,
            existing_records=existing_records
        )
        assert _, "Expected a result from _write_indexd"
        manifest_entries.append(manifest_item)
    return manifest_entries


def upload_indexd(config: Config, project_id: str, object_id: str = None, duplicate_check: bool = False,
                  manifest_path: str = None, restricted_project_id: str = None) -> list[dict]:
    """Save manifest to indexd, returns list of manifest entries"""

    manifest_entries = []

    assert project_id, "project_id is missing"
    assert project_id.count('-') == 1, f"{project_id} should have a single '-' delimiter."
    program, project = project_id.split('-')

    if restricted_project_id:
        assert restricted_project_id.count('-') == 1, f"{restricted_project_id} should have a single '-' delimiter."

    file_client, index_client, user, auth = gen3_services(config=config)

    bucket_name = get_program_bucket(config, program, auth=auth)
    assert bucket_name, f"could not find bucket for {program}"

    if manifest_path:
        assert pathlib.Path(manifest_path).is_file(), f"{manifest_path} is not a file"
        with open(manifest_path) as manifest_file:
            _generator = json.load(manifest_file)
    else:
        _generator = ls(config, project_id=project_id, object_id=object_id)

    # with Pool(processes=worker_count()) as pool:
    # start = datetime.now()
    # logger.info(f"{start} Writing {len(_generator)} records to indexd...")
    for manifest_item in tqdm(_generator):
        # multiprocessing takes too much overhead to start up, so we're not using it
        # _ = pool.apply(
        #     func=_write_indexd,
        #     args=(
        #         index_client,
        #         project_id,
        #         manifest_item,
        #         bucket_name,
        #         duplicate_check,
        #         restricted_project_id
        #     )
        # )
        _ = _write_indexd(
            index_client,
            project_id,
            manifest_item,
            bucket_name,
            duplicate_check,
            restricted_project_id
        )
        assert _, "Expected a result from _write_indexd"
        manifest_entries.append(manifest_item)
    # end = datetime.now()
    # logger.info(f"{end} Wrote {len(manifest_entries)} records to indexd in {(end - start).total_seconds()} seconds")
    return manifest_entries


def upload_files(config: Config, manifest_entries: list[dict], project_id: str, profile: str, upload_path: str,
                 overwrite_files: bool, auth=None) -> subprocess.CompletedProcess:
    """Upload files to gen3 via gen3-client."""

    assert project_id, "project_id is missing"
    assert project_id.count('-') == 1, f"{project_id} should have a single '-' delimiter."
    program, project = project_id.split('-')

    assert profile, "profile is missing"

    if not auth:
        auth = ensure_auth(profile=profile)

    bucket_name = get_program_bucket(config, program, auth=auth)
    assert bucket_name, f"could not find bucket for {program}"

    manifest_path = config.state_dir / f"{project_id}.manifest.json"

    with open(manifest_path, 'w') as manifest_file:
        json.dump(manifest_entries, manifest_file)

    assert upload_path, "upload_path is missing"
    cmd = f"gen3-client upload-multiple --manifest {manifest_path} --profile {profile} --upload-path {upload_path} --bucket {bucket_name} --numparallel {worker_count()}"
    print(f"Running: {cmd}", file=sys.stderr)
    cmd = cmd.split()
    upload_results = subprocess.run(cmd)
    assert upload_results.returncode == 0, upload_results
    return upload_results


def rm(config: Config, project_id: str, object_id: str, file_name: str = None):
    """Remove manifest entry(s) from local sqlite."""
    connection = _get_connection(config)
    with connection:
        if object_id:
            connection.execute('DELETE from manifest where object_id = ?', (object_id,))
        elif file_name:
            connection.execute('DELETE from manifest where file_name = ?', (file_name,))
        else:
            connection.execute('DELETE from manifest where project_id = ?', (project_id,))
