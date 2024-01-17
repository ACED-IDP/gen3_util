import json
import os
import pathlib
import subprocess
import sys
from io import StringIO

from gen3.file import Gen3File

from gen3_util import Config
from gen3_util.common import unzip_collapse
from gen3_util.config import ensure_auth, init
from gen3_util.files.lister import ls
from gen3_util.files.manifest import worker_count


def clone(config: Config, project_id: str, data_type: str = 'all') -> list[str]:
    """Clone a project from a Gen3 commons."""
    # setup directories
    path = pathlib.Path.cwd()
    original_path = path
    path = pathlib.Path(path) / project_id
    assert not path.exists(), f"Directory {path} already exists."
    path.mkdir(exist_ok=True)
    os.chdir(path)
    logs = []
    for _ in init(config, project_id):
        logs.append(_)
    meta_data_path = pathlib.Path(path) / 'META'
    assert meta_data_path.exists(), f"Directory {meta_data_path} does not exist."

    auth = ensure_auth(profile=config.gen3.profile)
    results = ls(config=config, metadata={'project_id': config.gen3.project_id, 'is_metadata': True}, auth=auth)
    records = 'records' in results and results['records'] or []
    records = sorted(records, key=lambda d: d['file_name'])
    assert len(records) > 0, f"No metadata found for {config.gen3.project_id}"
    # most recent metadata, file_name has a timestamp
    download_meta = records[-1]

    # get metadata
    if data_type in ['all', 'meta']:
        file_client = Gen3File(auth_provider=auth)
        extract_to = pathlib.Path(path) / 'META'

        # -------------- Download metadata ----------------
        # gen3 always logs to stdout, seems to be impossible to configure, so we capture it
        # ------------------------------------------------
        # Create the in-memory "file"
        temp_out = StringIO()
        # Replace default stdout (terminal) with our stream
        sys.stdout = temp_out
        # call the function that will print to stdout
        is_ok = file_client.download_single(download_meta['did'], path=extract_to)
        # The original `sys.stdout` is kept in a special
        # dunder named `sys.__stdout__`. So you can restore
        # the original output stream to the terminal.
        sys.stdout = sys.__stdout__
        temp_out.close()
        assert is_ok, f"Failed to download metadata {download_meta['did']}"

        zip_file = extract_to / download_meta['file_name']
        unzip_collapse(zip_file=zip_file, extract_to=extract_to)
        zip_file.unlink()
        assert not (extract_to / download_meta['file_name']).exists()
        logs.append(f"metadata downloaded to {extract_to.relative_to(original_path)}")

    if data_type in ['all', 'files']:
        # download data files to local dir, create a manifest file
        results = ls(config=config, metadata={'project_id': config.gen3.project_id}, auth=auth)
        records = 'records' in results and results['records'] or []
        records = sorted(records, key=lambda d: d['size'])
        manifest = [{'object_id': _['did']} for _ in records if 'is_metadata' not in _['metadata']]
        manifest_file = config.state_dir / f"manifest-{download_meta['did']}.json"
        with manifest_file.open('w') as fp:
            json.dump(manifest, fp, indent=2, default=str)
        # download files using the manifest created above
        data_path = pathlib.Path(path)
        cmd = f"gen3-client download-multiple --manifest {manifest_file.absolute()} --profile {config.gen3.profile} --download-path {data_path} --no-prompt  --skip-completed --numparallel {worker_count()}"
        logs.append(cmd)
        download_results = subprocess.run(cmd.split(), capture_output=False, stdout=sys.stderr)
        assert download_results.returncode == 0, f"gen3-client download-multiple  failed {download_results}"
        logs.append(f"Downloaded {len(manifest)} files to {data_path.relative_to(original_path)}")

    return logs
