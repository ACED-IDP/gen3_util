import urllib
from os import stat

import requests
from gen3.auth import Gen3Auth
from gen3.file import Gen3File

import gen3_tracker
from gen3_tracker import Config
from gen3_tracker.gen3.buckets import get_program_bucket
from gen3_tracker.gen3.indexd import write_indexd
from gen3_tracker.git import calculate_hash, DVC, DVCMeta, DVCItem, git_archive, modified_date, run_command


def push_snapshot(config: Config, auth: Gen3Auth, project_id: str = None, object_name: str = None):
    """Zip the git repo and push it to the server."""
    # create a zip of the git repo and associate it with the project
    # TODO should we query git to get the list of files to zip?
    files_to_zip = ['.git', 'MANIFEST', 'META', '.gitignore', '.g3t']

    proj_id = project_id or config.gen3.project_id
    program, _ = proj_id.split('-')

    # provide support for server provided path name
    if object_name:
        zipfile_path =  object_name
    else:
        zipfile_path = str(config.work_dir / f'{config.gen3.project_id}.git.zip')
        git_archive(zipfile_path)

    # this version simply adds the file to indexd and uploads it
    md5_sum = calculate_hash('md5', zipfile_path)
    my_dvc = DVC(
        meta=DVCMeta(),
        project_id=proj_id,
        outs=[
            DVCItem(
                path=zipfile_path,
                md5=md5_sum,
                hash='md5',
                modified=modified_date(zipfile_path),
                size=stat(zipfile_path).st_size,
            )
        ]
    )

    if not auth:
        auth = gen3_tracker.config.ensure_auth(config=config)

    bucket_name = get_program_bucket(config=config, program=program, auth=auth)
    metadata = write_indexd(
        auth=auth,
        project_id=proj_id,
        bucket_name=bucket_name,
        overwrite=True,
        restricted_project_id=None,
        existing_records=[my_dvc.object_id],
        dvc=my_dvc,
    )

    gen3_file = Gen3File(auth_provider=auth)
    response = gen3_file.upload_file_to_guid(
        bucket=bucket_name,
        guid=my_dvc.object_id,
        file_name=zipfile_path,
        expires_in=3600,
    )
    url = response.get('url', None)
    assert url, f"Expected url in {response}"
    # this url needs to be unquoted
    signed_url = urllib.parse.unquote(url)
    with open(zipfile_path, 'rb') as f:
        files = {'file': (zipfile_path, f)}
        # this needs to be a PUT
        response = requests.put(signed_url, files=files)
        response.raise_for_status()

    return {"msg": str(response), "object_id": my_dvc.object_id}

    # cmd = f"gen3-client upload-single --bucket {bucket_name} --guid {my_dvc.object_id} --file {zipfile_path} --profile {config.gen3.profile}",
    # print(cmd)
    # run_command(
    #     cmd,
    #     no_capture=False)
