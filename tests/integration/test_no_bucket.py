import os
import pathlib
import time
import uuid

import pytest
from click.testing import CliRunner
from gen3_util.repo.cli import cli
from gen3_util.common import PROJECT_DIRECTORIES
import unittest.mock as mock
import socket

CURRENT_DIR = pathlib.Path.cwd()


def setup_module(module):
    """Setup for module."""
    global CURRENT_DIR
    CURRENT_DIR = pathlib.Path.cwd()


def teardown_module(module):
    """Teardown for module."""
    os.chdir(CURRENT_DIR)


@pytest.mark.skip("Code for this was commented out see https://github.com/ACED-IDP/gen3_util/blob/ea10a1d9a23296f90616d2fb4f52ff6918c623f5/gen3_util/repo/puller.py#L31-L65")
def test_no_bucket(tmp_path, program, profile):
    """Test adding file with no bucket."""

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

    # create a data file
    os.mkdir('data')
    with open('data/test.txt', 'w') as file:
        file.write('test')

    # add the file to the index
    result = runner.invoke(cli, f'--format json --profile {profile} add data/test.txt  --no-bucket'.split())
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
    print("REAL HOSTNAME", socket.gethostname())
    os.chdir(str(cloned_dir))
    result = runner.invoke(cli, f'--format json --profile {profile} clone --project_id {project_id}'.split())
    print(result.output)
    assert result.exit_code == 0
    assert pathlib.Path(cloned_dir / project_id).exists()

    # cd into the clone
    os.chdir(str(cloned_dir / project_id))

    # pull down the data
    result = runner.invoke(cli, f'--format json --profile {profile} pull'.split())
    print(">>>>> pull output", result.output)
    assert result.exit_code == 0

    data_file = pathlib.Path('data/test.txt')
    assert data_file.exists(), f"File not found {data_file}"
    assert data_file.is_symlink(), f"File not a symlink {data_file}"
    assert data_file.readlink(), f"Symlink pointer not found {data_file.readlink()}"
    assert data_file.read_text() == 'test', f"File content not as expected {data_file.read_text()}"

    #
    # now test clone from a foreign host (i.e. not the host that created the project)
    #

    with mock.patch("socket.gethostname", return_value="fakehostname"):
        print("MOCKED HOSTNAME", socket.gethostname())

        # create directory for second clone
        cloned_dir = pathlib.Path(tmp_path) / 'cloned-foreign-host'
        cloned_dir.mkdir(parents=True, exist_ok=True)
        os.chdir(str(cloned_dir))

        # clone the project
        os.chdir(str(cloned_dir))
        result = runner.invoke(cli, f'--format json --profile {profile} clone --project_id {project_id}'.split())
        print('>>>> clone output START', result.output, 'END clone output')

        assert result.exit_code == 0
        assert pathlib.Path(cloned_dir / project_id).exists()

        # cd into the clone
        os.chdir(str(cloned_dir / project_id))

        # pull down the data
        result = runner.invoke(cli, f'--format json --profile {profile} pull'.split())
        result_output = result.output

        print('>>>> pull output START', result_output, 'END pull output')
        print('>>>> result', result)

        assert result.exit_code == 0

        data_file = pathlib.Path('data/test.txt')
        assert not data_file.exists(), f"Should not have created file or symlink {data_file}"

        # assert "scp" in result_output, f"Should have created scp command {result_output}"
        # assert 'data/test.txt' in result_output, f"Should find file data/test.txt in {result_output}"
