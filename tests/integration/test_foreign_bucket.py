import json
import os
import pathlib
import time
import uuid

import requests
from click.testing import CliRunner
from gen3.file import Gen3File

from gen3_util import Config
from gen3_util.config import ensure_auth
from gen3_util.repo.cli import cli
from gen3_util.common import PROJECT_DIRECTORIES, read_yaml

CURRENT_DIR = pathlib.Path.cwd()


def setup_module(module):
    """Setup for module."""
    global CURRENT_DIR
    CURRENT_DIR = pathlib.Path.cwd()


def teardown_module(module):
    """Teardown for module."""
    os.chdir(CURRENT_DIR)


def test_foreign_bucket(tmp_path, program, profile):
    """Test adding a foreign bucket."""

    # cwd to tmp dir
    os.chdir(tmp_path)
    runner = CliRunner()

    # create an uniq test project
    guid = str(uuid.uuid4())
    project = f'TEST_INCREMENTAL_{guid.replace("-", "_")}'
    project_id = f'{program}-{project}'

    # initialize the project
    result = runner.invoke(cli, ['--format', 'json', '--profile', profile, 'init', project_id])
    assert result.exit_code == 0, f"cmd failed with {result.output}"
    result_output = result.output
    print(result_output)
    print(tmp_path)
    for _ in PROJECT_DIRECTORIES:
        assert pathlib.Path(tmp_path, _).exists(), f"{_} not found in {tmp_path}"

    # sign the requests
    result = runner.invoke(cli, f'--format json --profile {profile} utilities access sign'.split())
    _, project = project_id.split('-')
    print(result.output)
    assert result.exit_code == 0
    assert project in result.output, result.output

    # create the project
    result = runner.invoke(cli, f'--format json --profile {profile} utilities projects create /programs/{program}/projects/{project}'.split())
    _, project = project_id.split('-')
    print(result.output)
    assert result.exit_code == 0
    assert project in result.output, result.output

    # add the file to the index
    # result = runner.invoke(cli, f'--format json --profile {profile} add s3://aced-public/aced.json --md5 eda5d98677ede8ea4ab1748e722d19ec --size 386270 --modified 2024-03-02T21:58:20 '.split())
    result = runner.invoke(cli,
                           f'--format json --profile {profile} add s3://data-import-test/file-1MB.1.txt --md5 b6d81b360a5672d80c27430f39153e2c --size 1000000 --modified 2024-03-02T21:58:20 '.split())
    # data-import-test
    print(result.output)
    assert result.exit_code == 0

    # create the metadata
    result = runner.invoke(cli, f'--format json --profile {profile} utilities meta create'.split())
    print(result.output)
    assert result.exit_code == 0

    # commit the change
    result = runner.invoke(cli, f'--format json --profile {profile} commit -m "initial"'.split())
    print(result.output)
    assert result.exit_code == 0

    # push the change
    result = runner.invoke(cli, f'--format json --profile {profile} push'.split())
    print(result.output)
    assert result.exit_code == 0

    # poll the job until complete
    c = limit = 20
    completed = False
    while True:
        result = runner.invoke(cli, f'--format json --profile {profile} status'.split())
        assert 'status' in result.output, f"status not found in {result.output}"

        if 'Completed' in result.output:
            completed = True
            break
        if 'Error' in result.output:
            break

        time.sleep(30)
        c -= 1
        if c == 0:
            break
    print(result.output)
    assert completed, f"Job not completed after {limit} tries"

    # create directory for clone
    cloned_dir = pathlib.Path(tmp_path) / 'cloned'
    cloned_dir.mkdir(parents=True, exist_ok=True)

    # clone the project
    os.chdir(str(cloned_dir))
    result = runner.invoke(cli, f'--format json --profile {profile} clone --project_id {project_id}'.split())
    print(result.output)
    assert result.exit_code == 0
    assert pathlib.Path(cloned_dir / project_id).exists()

    # cd into the clone
    os.chdir(str(cloned_dir / project_id))

    # weirdness with CLIRunner gen3-client and fileno
    # see https://stackoverflow.com/questions/73311668/how-do-i-test-that-command-sends-subprocess-output-to-stderr
    # so, we manually call the same methods

    # read the META data
    document_reference = json.loads(open(pathlib.Path('META') / 'DocumentReference.ndjson').readline())
    print(json.dumps(document_reference, indent=2))
    guid = document_reference['id']

    _ = pathlib.Path().cwd() / '.g3t' / 'config.yaml'
    config = Config(**read_yaml(_))
    authentication_object = ensure_auth(config=config)
    file_client = Gen3File(auth_provider=authentication_object)

    presigned_url = file_client.get_presigned_url(guid=guid)

    print(presigned_url)

    # download the file
    response = requests.get(presigned_url['url'])
    with open('file-1MB.1.txt', 'wb') as f:
        f.write(response.content)
