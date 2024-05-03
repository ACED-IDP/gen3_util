import os
import pathlib
import uuid

import pytest
import yaml
from click.testing import CliRunner
from g3t.git import DVC
from tests.integration import run


@pytest.fixture
def runner() -> CliRunner:
    """Fixture for creating a CliRunner instance."""
    return CliRunner()


def test_simple_workflow(runner: CliRunner, tmpdir) -> None:
    """Test the init command."""
    # change to the temporary directory
    assert tmpdir.chdir()
    print(pathlib.Path.cwd())

    assert os.environ.get("G3T_PROFILE"), "G3T_PROFILE environment variable must be set."

    # create a project
    project = uuid.uuid4().hex.replace('-', '_')
    project_id = f"cbds-{project}"
    print(project_id)

    run(runner, ["--debug", "init", project_id, "--approve"],
        expected_files=[".g3t", ".git"])

    # create a test file
    test_file = pathlib.Path("my-project-data/hello.txt")
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_text('hello\n')

    # add the file
    run(runner, ["--debug", "add", str(test_file)], expected_files=["MANIFEST/my-project-data/hello.txt.dvc"])

    # should create a dvc file
    dvc_path = pathlib.Path("MANIFEST/my-project-data/hello.txt.dvc")
    assert dvc_path.exists(), f"{dvc_path} does not exist."
    with open(dvc_path) as f:
        yaml_data = yaml.safe_load(f)
    assert yaml_data
    dvc = DVC.model_validate(yaml_data)
    assert dvc, "DVC file not parsed."

    run(runner, ["--debug", "meta", "init"])

    run(runner, ["--debug", "commit", "-am", "\"initial commit\""])

    run(runner, ["--debug", "meta", "validate"])

    run(runner, ["--debug", "meta", "graph"], expected_files=["meta.html"])

    # TODO fix meta dataframe when no Patient
    # result = runner.invoke(cli, ["--debug", "meta", "dataframe"])
    # print(result.stdout)
    # assert result.exit_code == 0, 'meta dataframe failed'
    # meta_csv = pathlib.Path("meta.csv")
    # assert meta_csv.exists(), f"{meta_csv} does not exist."

    run(runner, ["--debug", "push"])

    run(runner, ["--debug", "ls"], expected_output=["my-project-data/hello.txt"])
