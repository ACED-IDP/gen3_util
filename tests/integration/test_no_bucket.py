import os
import pathlib
import time
import uuid

from click.testing import CliRunner
from gen3_util.repo.cli import cli
from gen3_util.common import PROJECT_DIRECTORIES

CURRENT_DIR = pathlib.Path.cwd()


def setup_module(module):
    """Setup for module."""
    print("setup_module      module:%s" % module.__name__)
    global CURRENT_DIR
    CURRENT_DIR = pathlib.Path.cwd()


def teardown_module(module):
    """Teardown for module."""
    print(f"teardown_module   module:{module.__name__} cwd:{CURRENT_DIR}")
    os.chdir(CURRENT_DIR)


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
    limit = 10
    completed = False
    while True:
        result = runner.invoke(cli, f'--format json --profile {profile} status'.split())
        print(result.output)
        if 'Completed' in result.output:
            completed = True
            break
        time.sleep(5)
        limit -= 1
        if limit == 0:
            break
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

    # pull down the data
    result = runner.invoke(cli, f'--format json --profile {profile} pull'.split())
    print(result.output)
    assert result.exit_code == 0

    data_file = pathlib.Path('data/test.txt')
    assert data_file.exists(), f"File not found {data_file}"
    assert data_file.is_symlink(), f"File not a symlink {data_file}"
    assert data_file.readlink(), f"Symlink pointer not found {data_file.readlink()}"
    assert data_file.read_text() == 'test', f"File content not as expected {data_file.read_text()}"
