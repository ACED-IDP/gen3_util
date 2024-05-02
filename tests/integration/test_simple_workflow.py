import os
import pathlib
import uuid

import pytest
import yaml
from click.testing import CliRunner
from g3t.cli import cli
from g3t.git import DVC


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
    print(cli)
    result = runner.invoke(cli, ["--debug", "init", f"cbds-{project}", "--approve"])
    print(result.stdout)
    assert result.exit_code == 0
    assert pathlib.Path(".g3t").exists()
    assert pathlib.Path(".git").exists()

    # create a test file
    test_file = pathlib.Path("my-project-data/hello.txt")
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_text('hello\n')

    # add the file
    result = runner.invoke(cli, ["--debug", "add", str(test_file)])
    print(result.stdout)
    assert result.exit_code == 0

    # should create a dvc file
    dvc_path = pathlib.Path("MANIFEST/my-project-data/hello.txt.dvc")
    assert dvc_path.exists(), f"{dvc_path} does not exist."
    with open(dvc_path) as f:
        yaml_data = yaml.safe_load(f)
    assert yaml_data
    dvc = DVC.model_validate(yaml_data)
    assert dvc, "DVC file not parsed."

    try:
        result = runner.invoke(cli, ["--debug", "meta init"])
        print(result.stdout)
        assert False, "DEBUG 2"
        assert result.exit_code == 0
        document_reference_path = pathlib.Path("META/DocumentReference.ndjson")
        assert document_reference_path.exists(), f"{document_reference_path} does not exist."

        assert False, "DEBUG 3"
    except Exception as e:
        print(e)
        raise e
