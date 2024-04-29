import json
import pathlib
import subprocess

from gen3_util.common import read_ndjson_file
from gen3_util.files.manifest import worker_count


def pull_files(config, auth, manifest_name, original_path, path, extra_metadata={}, path_filter=None):
    """Pull files from a Gen3 based on current METADATA"""
    logs = []
    # create a manifest
    manifest = []
    for _ in read_ndjson_file(pathlib.Path('META/DocumentReference.ndjson')):
        manifest.append({'object_id': _['id']})

    data_path = pathlib.Path(path)
    if len(manifest) > 0:
        manifest_file = config.state_dir / manifest_name
        with manifest_file.open('w') as fp:
            json.dump(manifest, fp, indent=2, default=str)

        # download files using the manifest created above
        cmd = f"gen3-client download-multiple --manifest {manifest_file.absolute()} --profile {config.gen3.profile} --download-path {data_path} --no-prompt  --skip-completed --numparallel {worker_count()}"
        logs.append(cmd)
        download_results = subprocess.run(cmd.split(), capture_output=False)
        assert download_results.returncode == 0, f"gen3-client download-multiple  failed {download_results}"
    else:
        logs.append(f"No files to download for {config.gen3.project_id}")

    # manifest = [{'object_id': _['did'], 'file_name': _['file_name'], 'urls': _['urls']} for _ in records if _['metadata'].get('no_bucket', False)]
    # if len(manifest) > 0:
    #     hostname = socket.gethostname()
    #     files_for_scp = []
    #     files_for_symlink = []
    #     for _ in manifest:
    #         for url in _['urls']:
    #             parse_result = urlparse(url)
    #             if parse_result.scheme == 'scp':
    #                 if parse_result.netloc == hostname:
    #                     files_for_symlink.append(_)
    #                 else:
    #                     files_for_scp.append(_)
    #
    #     # print(f"SCP files {files_for_scp}", file=sys.stderr)
    #     for _ in files_for_scp:
    #         for url in _['urls']:
    #             if 'scp' not in url:
    #                 continue
    #             cmd = f"scp {url} {data_path}"
    #             logs.append(cmd)
    #             # download_results = subprocess.run(cmd.split(), capture_output=False, stdout=sys.stderr)
    #             # assert download_results.returncode == 0, f"SCP failed {download_results}"
    #         logs.append(f"There are {len(files_for_scp)} files available for scp")
    #     # print(f"Symlink files {files_for_symlink}", file=sys.stderr)
    #     for _ in files_for_symlink:
    #         for url in _['urls']:
    #             if 'scp' not in url:
    #                 continue
    #             pathlib.Path(_['file_name']).parent.mkdir(parents=True, exist_ok=True)
    #             os.symlink(urlparse(url).path, _['file_name'])
    #         logs.append(f"Symlinked {len(files_for_symlink)} files.")
    # else:
    #     logs.append(f"No files to symlink or scp for {config.gen3.project_id}")

    # logs.append(f"Downloaded {len(manifest)} files to {data_path.relative_to(original_path)}")
    return logs
