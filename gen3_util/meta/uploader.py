import logging
import pathlib
import tempfile
import urllib
import uuid
from urllib.parse import urlparse, ParseResult

import requests

from gen3_util.config import Config, gen3_services
from zipfile import ZipFile

from gen3_util.meta import ACED_NAMESPACE
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
                logger.info(f"Deleting existing record {guid}")
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


def _validate_parameters(from_: str, to_: str) -> (pathlib.Path, ParseResult):
    url = urlparse(to_)
    assert url.scheme, f"{to_} does not appear to be a url"

    assert len(urlparse(from_).scheme) == 0, f"{from_} appears to be an url. url to url cp not supported"

    from_ = pathlib.Path(from_)
    assert from_.is_dir(), f"{from_} is not a directory"

    return from_, url


def cp(config: Config, from_: str, to_: str, project_id: str, ignore_state: bool):
    """Copy meta to bucket"""
    from_, to_ = _validate_parameters(from_, to_)
    bucket_name = to_.hostname

    file_client, index_client, user = gen3_services(config=config)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_dir = pathlib.Path(temp_dir)
        object_name = f'_{project_id}_meta.zip'

        zipfile_path = temp_dir / object_name
        with ZipFile(zipfile_path, 'w') as zip_object:
            for _ in from_.glob("*.ndjson"):
                zip_object.write(_)

        stat = zipfile_path.stat()
        md5_sum = md5sum(zipfile_path)
        id_ = str(uuid.uuid5(ACED_NAMESPACE, object_name))
        program, project = project_id.split('-')

        metadata = _update_indexd(
            id_,
            bucket_name,
            ignore_state,
            index_client,
            md5_sum,
            object_name,
            program,
            project,
            {'metadata_submitter': user['username'], 'metadata_version': '0.0.1', 'is_metadata': True},
            stat.st_size
        )

        document = file_client.upload_file_to_guid(guid=id_, file_name=object_name, bucket=bucket_name)
        assert 'url' in document, document
        signed_url = urllib.parse.unquote(document['url'])

        file_name = pathlib.Path(zipfile_path)

        _upload_file_to_signed_url(file_name, md5_sum, metadata, signed_url)
        return {'msg': f"Uploaded {file_name} to {signed_url}"}
