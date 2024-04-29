import os
import pathlib
import sys
import click
from io import StringIO

from gen3.file import Gen3File

from gen3_util import Config
from gen3_util.repo.puller import pull_files
from gen3_util.common import unzip_collapse, write_meta_index
from gen3_util.config import ensure_auth, init
from gen3_util.files.lister import ls


def clone(config: Config, project_id: str, data_type: str = 'all') -> list[str]:
    """Clone a project from a Gen3 commons."""
    # setup directories
    path = pathlib.Path.cwd()
    original_path = path
    path = pathlib.Path(path) / project_id
    logs = []

    if path.exists():
        click.echo(f"Directory {path} already exists, proceeding.", file=sys.stdout)

    path.mkdir(exist_ok=True)
    os.chdir(path)
    for _ in init(config, project_id):
        logs.append(_)
    meta_data_path = pathlib.Path(path) / 'META'
    assert meta_data_path.exists(), f"Directory {meta_data_path} does not exist."

    auth = ensure_auth(config=config)

    snapshot_manifest = find_latest_snapshot(auth, config)

    # get metadata
    if data_type in ['all', 'meta']:
        extract_to = pathlib.Path(path)
        # download single needs the directory to exist
        (extract_to / snapshot_manifest['file_name']).parent.mkdir(exist_ok=True, parents=True)

        download_unzip_snapshot_meta(auth, config, snapshot_manifest, extract_to, logs, original_path)

    if data_type in ['all', 'files']:
        # download data files to local dir, create a manifest file
        manifest_name = f"manifest-{snapshot_manifest['did']}.json"
        logs.extend(pull_files(config, auth, manifest_name, original_path, path))

    return logs


def find_latest_snapshot(auth, config):
    results = ls(config=config, metadata={'project_id': config.gen3.project_id, 'is_snapshot': True}, auth=auth)
    records = 'records' in results and results['records'] or []
    records = sorted(records, key=lambda d: d['file_name'])
    assert len(records) > 0, f"No snapshot found for {config.gen3.project_id}"
    # print(f"Found {len(records)} metadata records {[_['file_name'] for _ in records]}", file=sys.stderr)
    # most recent metadata, file_name has a timestamp
    download_meta = records[-1]
    return download_meta


def download_unzip_snapshot_meta(auth, config, snapshot_manifest, extract_to, logs, original_path):
    """Download metadata and unzip it."""
    logs.append(f"Cloning from metadata records {snapshot_manifest['file_name']}")
    # -------------- Download metadata ----------------
    # gen3 always logs to stdout, seems to be impossible to configure loglevel or stderr, so we capture it
    # ------------------------------------------------
    file_client = Gen3File(auth_provider=auth)
    # Create the in-memory "file"
    temp_out = StringIO()
    # Replace default stdout (terminal) with our stream
    sys.stdout = temp_out
    # call the function that will print to stdout
    is_ok = file_client.download_single(snapshot_manifest['did'], path=extract_to)
    # The original `sys.stdout` is kept in a special
    # dunder named `sys.__stdout__`. So you can restore
    # the original output stream to the terminal.
    sys.stdout = sys.__stdout__
    temp_out.close()
    assert is_ok, f"Failed to download metadata {snapshot_manifest['did']}"
    zip_file = extract_to / snapshot_manifest['file_name']
    unzip_collapse(zip_file=zip_file, extract_to=(extract_to / 'META'))
    zip_file.unlink()
    assert not (extract_to / snapshot_manifest['file_name']).exists()
    logs.append(f"metadata downloaded to {extract_to.relative_to(original_path)}")
    # index the cloned metadata
    write_meta_index(
        index_path=config.state_dir,
        source_path=(extract_to / 'META')
    )
