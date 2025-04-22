import os
from pathlib import Path

import yaml
from click.testing import CliRunner

from gen3_tracker.config import ensure_auth, default
from gen3_tracker.git import DVC
from tests import run
import pytest

from tests.integration import validate_document_in_grip, validate_document_in_elastic


def test_rm_uncommitted(runner: CliRunner, project_id, tmpdir) -> None:
    """Ensure we can remove uncommitted files."""
    # change to the temporary directory
    assert tmpdir.chdir()
    print(Path.cwd())

    _create_project(project_id, runner)

    # rm the second test file before committing
    run(
        runner,
        ["--debug", "rm", str("my-project-data/hello2.txt")],
    )

    # create the meta file
    run(
        runner,
        ["--debug", "meta", "init"],
        expected_files=[Path("META/DocumentReference.ndjson")],
    )

    # commit the changes, delegating to git
    run(runner, ["--debug", "commit", "-am", "initial commit"])

    # push to the server
    run(runner, ["--debug", "push"])

    # list the files from indexd
    run(runner, ["--debug", "ls"], expected_output=["my-project-data/hello.txt"])

    # list the files from indexd, should not include the removed file
    with pytest.raises(AssertionError):
        run(runner, ["--debug", "ls"], expected_output=["my-project-data/hello2.txt"])


def test_rm_committed(runner: CliRunner, project_id, tmpdir) -> None:
    """Ensure we can remove committed files."""
    # change to the temporary directory
    assert tmpdir.chdir()
    print(Path.cwd())

    _create_project(project_id, runner)

    # create the meta file
    run(
        runner,
        ["--debug", "meta", "init"],
        expected_files=[Path("META/DocumentReference.ndjson")],
    )

    # commit the changes
    run(runner, ["--debug", "commit", "-am", "initial commit"])

    # rm the second test file after committing
    run(
        runner,
        ["--debug", "rm", str("my-project-data/hello2.txt")],
    )

    # re-create the meta file
    run(
        runner,
        ["--debug", "meta", "init"],
        expected_files=[Path("META/DocumentReference.ndjson")],
    )

    # commit the re-created meta
    run(runner, ["--debug", "commit", "-am", "updated commit"])

    # push to the server
    run(runner, ["--debug", "push", "--fhir-server"])

    # list the files from indexd
    run(runner, ["--debug", "ls"], expected_output=["my-project-data/hello.txt"])

    # list the files from indexd, should not include the removed file
    with pytest.raises(AssertionError):
        run(runner, ["--debug", "ls"], expected_output=["my-project-data/hello2.txt"])

    # check the files exist in the graph and flat databases
    # we will need the object_id of the file to do that
    # should create a dvc file
    dvc_path = Path("MANIFEST/my-project-data/hello.txt.dvc")
    assert dvc_path.exists(), f"{dvc_path} does not exist."
    with open(dvc_path) as f:
        yaml_data = yaml.safe_load(f)
    assert yaml_data
    dvc = DVC.model_validate(yaml_data)
    assert dvc, "DVC file not parsed."

    # capture expected object_id
    dvc.project_id = project_id
    object_id = dvc.object_id
    auth = ensure_auth(config=default())
    validate_document_in_grip(object_id, auth=auth, project_id=project_id)
    validate_document_in_elastic(object_id, auth=auth)


def _create_project(project_id, runner, add_files=True, files=("my-project-data/hello.txt", "my-project-data/hello2.txt")) -> list[str]:
    """Create a project and add files to it."""

    assert os.environ.get(
        "G3T_PROFILE"
    ), "G3T_PROFILE environment variable must be set."
    print(project_id)
    run(
        runner,
        ["--debug", "init", project_id, "--approve"],
        expected_files=[Path(".g3t"), Path(".git")],
    )

    for path in files:
        # create a test file
        test_file = Path(path)
        test_file.parent.mkdir(parents=True, exist_ok=True)
        test_file.write_text("hello\n")

        # add the file
        if add_files:
            run(
                runner,
                ["--debug", "add", str(test_file)],
                expected_files=[Path(f"MANIFEST/{path}.dvc")],
            )

    return files
