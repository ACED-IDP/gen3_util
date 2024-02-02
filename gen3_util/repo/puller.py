import json
import pathlib
import subprocess
import sys

from gen3_util.files.lister import ls
from gen3_util.files.manifest import worker_count

from wcmatch import glob


def pull_files(config, auth, manifest_name, original_path, path, extra_metadata={}, path_filter=None):
    """Pull files from a Gen3 commons."""
    logs = []
    # get all files from indexd
    metadata = dict(extra_metadata | {'project_id': config.gen3.project_id})
    results = ls(config=config, metadata=metadata, auth=auth)
    records = 'records' in results and results['records'] or []
    records = sorted(records, key=lambda d: d['size'])

    # create a manifest
    if path_filter:
        records = [_ for _ in records if glob.globmatch(_['file_name'], path_filter, flags=glob.G)]

    manifest = [{'object_id': _['did']} for _ in records if 'is_metadata' not in _['metadata']]
    assert len(manifest) > 0, f"No files found for {metadata}"
    manifest_file = config.state_dir / manifest_name
    with manifest_file.open('w') as fp:
        json.dump(manifest, fp, indent=2, default=str)

    # download files using the manifest created above
    data_path = pathlib.Path(path)
    cmd = f"gen3-client download-multiple --manifest {manifest_file.absolute()} --profile {config.gen3.profile} --download-path {data_path} --no-prompt  --skip-completed --numparallel {worker_count()}"
    logs.append(cmd)
    download_results = subprocess.run(cmd.split(), capture_output=False, stdout=sys.stderr)
    assert download_results.returncode == 0, f"gen3-client download-multiple  failed {download_results}"

    # logs.append(f"Downloaded {len(manifest)} files to {data_path.relative_to(original_path)}")
    return logs
