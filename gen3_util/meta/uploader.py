import logging
import pathlib
import tempfile
import urllib
import uuid
from datetime import datetime
from urllib.parse import urlparse

import requests
from gen3.file import Gen3File
from gen3.index import Gen3Index

from gen3_util.buckets import get_program_bucket
from gen3_util.config import Config, ensure_auth
from zipfile import ZipFile

from gen3_util import ACED_NAMESPACE
from gen3_util.meta.importer import md5sum

from gen3_util.files.uploader import _upload_file_to_signed_url  # noqa

logger = logging.getLogger(__name__)


def _update_indexd(id_, bucket_name, duplicate_check, index_client, md5_sum, object_name,
                   program, project, metadata, size):
    hashes = {'md5': md5_sum}
    guid = id_
    metadata = {
        **metadata,
        **hashes}
    # SYNC
    existing_record = None
    s3_url = f"s3://{bucket_name}/{guid}/{object_name}"
    if duplicate_check:
        try:
            existing_record = index_client.get_record(guid=guid)
        except Exception: # noqa
            pass
        if existing_record:
            skip_delete = all([
                existing_record['hashes']['md5'] == md5sum,
                s3_url in existing_record['urls']
            ])
            if not skip_delete:
                # SYNC
                logger.debug(f"Deleting existing record {guid}")
                index_client.delete_record(guid=guid)
                existing_record = None
    if not existing_record:
        try:
            existing_record = index_client.create_record(
                did=guid,
                hashes=hashes,
                size=size,
                authz=[f'/programs/{program}/projects/{project}'],
                file_name=object_name,
                metadata=metadata,
                urls=[s3_url]  # TODO make a DRS URL
            )
        except (requests.exceptions.HTTPError, AssertionError) as e:
            if not ('already exists' in str(e)):
                raise e
            logger.info(f"indexd record already exists, continuing upload. {guid}")
    return existing_record


def _validate_parameters(from_: str) -> pathlib.Path:

    assert len(urlparse(from_).scheme) == 0, f"{from_} appears to be an url. url to url cp not supported"

    return from_


def cp(config: Config,
       from_: str,
       project_id: str,
       ignore_state: bool,
       auth=None,
       user=None,
       object_name=None,
       metadata: dict = {}
       ):
    """Copy meta to bucket, used by etl_pod job"""
    from_ = _validate_parameters(str(from_))
    if not isinstance(from_, pathlib.Path):
        from_ = pathlib.Path(from_)

    if not auth:
        auth = ensure_auth(config=config)

    index_client = Gen3Index(auth_provider=auth)
    file_client = Gen3File(auth_provider=auth)

    metadata = dict({'submitter': None, 'metadata_version': '0.0.1', 'is_metadata': True} | metadata)
    if not metadata['submitter']:
        if not user:
            user = auth.curl('/user/user').json()
        metadata['submitter'] = user['name']

    program, project = project_id.split('-')

    bucket_name = get_program_bucket(config, program, auth=auth)
    assert bucket_name, f"could not find bucket for {program}"

    with tempfile.TemporaryDirectory() as temp_dir:
        if from_.is_dir():
            temp_dir = pathlib.Path(temp_dir)

            if not object_name:
                now = datetime.now().strftime("%Y%m%d-%H%M%S")
                object_name = f'_{project_id}-{now}_meta.zip'

            zipfile_path = temp_dir / object_name
            with ZipFile(zipfile_path, 'w') as zip_object:
                for _ in from_.glob("*.ndjson"):
                    zip_object.write(_)
        else:
            zipfile_path = pathlib.Path(from_)
            if not object_name:
                object_name = str(zipfile_path)

        stat = zipfile_path.stat()
        md5_sum = md5sum(zipfile_path)
        id_ = str(uuid.uuid5(ACED_NAMESPACE, object_name))

        metadata = _update_indexd(
            id_,
            bucket_name,
            ignore_state,
            index_client,
            md5_sum,
            object_name,
            program,
            project,
            metadata,
            stat.st_size
        )

        document = file_client.upload_file_to_guid(guid=id_, file_name=object_name, bucket=bucket_name)
        assert 'url' in document, document
        signed_url = urllib.parse.unquote(document['url'])

        file_name = pathlib.Path(zipfile_path)

        _upload_file_to_signed_url(file_name, md5_sum, metadata, signed_url)
        return {'msg': f"Uploaded {file_name} to {bucket_name} {id_}", "object_id": id_}
