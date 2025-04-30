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

    # remove the project from the server.
    # TODO note, this does not remove the files from the bucket (UChicago bug)
    # See https://ohsucomputationalbio.slack.com/archives/C043HPV0VMY/p1714065633867229
    run(
        runner,
        [
            "--debug",
            "projects",
            "empty",
            "--project_id",
            project_id,
            "--confirm",
            "empty",
        ],
    )


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
    assert not Path("META/Bundle.ndjson").exists(), "Did not expect a Bundle file."

    # commit the re-created meta
    run(runner, ["--debug", "commit", "-am", "updated commit"])

    # push to the server
    run(runner, ["--debug", "push"])

    # list the files from indexd
    run(runner, ["--debug", "ls"], expected_output=["my-project-data/hello.txt"])

    # list the files from indexd, should not include the removed file
    with pytest.raises(AssertionError):
        run(runner, ["--debug", "ls"], expected_output=["my-project-data/hello2.txt"])

    # check the files exist in the graph and flat databases
    # we will need the object_id of the file to do that
    # should create a dvc file
    dvc = read_dvc()

    # capture expected object_id
    dvc.project_id = project_id
    object_id = dvc.object_id
    auth = ensure_auth(config=default())

    ok = ''
    try:
        validate_document_in_grip(object_id, auth=auth, project_id=project_id)
    except Exception as e:
        ok = ok + f"Grip validation failed: {e}"

    try:
        validate_document_in_elastic(object_id, auth=auth)
    except Exception as e:
        ok = ok + f" Elastic validation failed: {e}"

    assert ok == '', ok

    # remove the project from the server.
    # TODO note, this does not remove the files from the bucket (UChicago bug)
    # See https://ohsucomputationalbio.slack.com/archives/C043HPV0VMY/p1714065633867229
    run(
        runner,
        [
            "--debug",
            "projects",
            "empty",
            "--project_id",
            project_id,
            "--confirm",
            "empty",
        ],
    )


def test_rm_pushed(runner: CliRunner, project_id, tmpdir) -> None:
    """Ensure we can remove pushed files."""
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

    dvc = read_dvc(file_path="MANIFEST/my-project-data/hello2.txt.dvc")
    # capture expected object_id
    dvc.project_id = project_id
    expected_missing_object_id = dvc.object_id

    # commit the changes
    run(runner, ["--debug", "commit", "-am", "initial commit"])

    # push the changes
    run(runner, ["--debug", "push"])

    # rm the second test file after pushing
    run(
        runner,
        ["--debug", "rm", str("my-project-data/hello2.txt")],
    )

    # re-create the meta file, with a bundle
    run(
        runner,
        ["--debug", "meta", "init", "--bundle"],
        expected_files=[Path("META/DocumentReference.ndjson"), Path("META/Bundle.ndjson")],
    )

    # commit the re-created meta
    run(runner, ["--debug", "commit", "-am", "updated commit"])

    # push to the server
    run(runner, ["--debug", "push", "--overwrite"])

    # list the files from indexd
    run(runner, ["--debug", "ls"], expected_output=["my-project-data/hello.txt"])

    # list the files from indexd, should not include the removed file
    with pytest.raises(AssertionError):
        run(runner, ["--debug", "ls"], expected_output=["my-project-data/hello2.txt"])

    # check the files exist in the graph and flat databases
    # we will need the object_id of the file to do that
    # should create a dvc file
    dvc = read_dvc()
    # capture expected object_id
    dvc.project_id = project_id
    object_id = dvc.object_id
    auth = ensure_auth(config=default())

    ok = ''

    try:
        validate_document_in_grip(object_id, auth=auth, project_id=project_id)
    except Exception as e:
        ok = ok + f"Grip validation failed: {e}"

    try:
        validate_document_in_elastic(object_id, auth=auth)
    except Exception as e:
        ok = ok + f" Elastic validation failed: {e}"

    try:
        validate_document_in_grip(expected_missing_object_id, auth=auth, project_id=project_id)
        ok = ok + f" Grip validation failed should not have found: {expected_missing_object_id}"
    except Exception:
        pass

    try:
        validate_document_in_elastic(expected_missing_object_id, auth=auth)
        ok = ok + f" Elastic validation failed should not have found: {expected_missing_object_id}"
    except Exception:
        pass

    assert ok == '', ok

    # remove the project from the server.
    # TODO note, this does not remove the files from the bucket (UChicago bug)
    # See https://ohsucomputationalbio.slack.com/archives/C043HPV0VMY/p1714065633867229
    run(
        runner,
        [
            "--debug",
            "projects",
            "empty",
            "--project_id",
            project_id,
            "--confirm",
            "empty",
        ],
    )


def test_rm_commit_all(runner: CliRunner, project_id, tmpdir) -> None:
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

    dvc = read_dvc(file_path="MANIFEST/my-project-data/hello2.txt.dvc")
    # capture expected object_id
    dvc.project_id = project_id
    expected_missing_object_id = dvc.object_id

    # commit the changes - note missing `-a` and no targets, assuming all
    run(runner, ["--debug", "commit", "-m", "initial commit"])

    # push the changes
    run(runner, ["--debug", "push"])

    # rm the second test file after pushing
    run(
        runner,
        ["--debug", "rm", str("my-project-data/hello2.txt")],
    )

    # re-create the meta file, with a bundle
    run(
        runner,
        ["--debug", "meta", "init", "--bundle"],
        expected_files=[Path("META/DocumentReference.ndjson"), Path("META/Bundle.ndjson")],
    )

    # commit the re-created meta
    run(runner, ["--debug", "commit", "-am", "updated commit"])

    # push to the server
    run(runner, ["--debug", "push", "--overwrite"])

    # list the files from indexd
    run(runner, ["--debug", "ls"], expected_output=["my-project-data/hello.txt"])

    # list the files from indexd, should not include the removed file
    with pytest.raises(AssertionError):
        run(runner, ["--debug", "ls"], expected_output=["my-project-data/hello2.txt"])

    # check the files exist in the graph and flat databases
    # we will need the object_id of the file to do that
    # should create a dvc file
    dvc = read_dvc()
    # capture expected object_id
    dvc.project_id = project_id
    object_id = dvc.object_id
    auth = ensure_auth(config=default())

    ok = ''

    try:
        validate_document_in_grip(object_id, auth=auth, project_id=project_id)
    except Exception as e:
        ok = ok + f"Grip validation failed: {e}"

    try:
        validate_document_in_elastic(object_id, auth=auth)
    except Exception as e:
        ok = ok + f" Elastic validation failed: {e}"

    try:
        validate_document_in_grip(expected_missing_object_id, auth=auth, project_id=project_id)
        ok = ok + f" Grip validation failed should not have found: {expected_missing_object_id}"
    except Exception:
        pass

    try:
        validate_document_in_elastic(expected_missing_object_id, auth=auth)
        ok = ok + f" Elastic validation failed should not have found: {expected_missing_object_id}"
    except Exception:
        pass

    assert ok == '', ok

    # remove the project from the server.
    # TODO note, this does not remove the files from the bucket (UChicago bug)
    # See https://ohsucomputationalbio.slack.com/archives/C043HPV0VMY/p1714065633867229
    run(
        runner,
        [
            "--debug",
            "projects",
            "empty",
            "--project_id",
            project_id,
            "--confirm",
            "empty",
        ],
    )


def test_rm_pushed_links(runner: CliRunner, project_id, tmpdir) -> None:
    """Ensure we can remove symlinks that have been pushed."""
    # change to the temporary directory
    assert tmpdir.chdir()
    print(Path.cwd())

    _create_project(project_id, runner, add_files=False)

    # create symlinks to the test files, even though they are local to this project's dir
    # this is accommodation since working from a temporary directory
    os.symlink("my-project-data/hello.txt", "hello.txt")
    os.symlink("my-project-data/hello2.txt", "hello2.txt")
    os.symlink("my-project-data/does-not-exist.txt", "hello3.txt")

    # Get the path of the platform temporary directory e.g. /tmp
    # we use the actual string '/tmp' as opposed to using the tempfile module provided in tmpdit
    # to ensure we can link to a file outside the project working dir
    temp_dir = '/tmp'
    if os.environ.get('TMP', None):
        temp_dir = os.environ.get('TMP')
    test_file = Path(temp_dir) / "hello-g3t-integration-test.txt"
    test_file.write_text("hello\n")
    os.symlink(str(test_file), "hello4.txt")

    run(
        runner,
        ["--debug", "add", "hello.txt"]
    )
    run(
        runner,
        ["--debug", "add", "hello2.txt"]
    )
    # should fail since the target file does not exist
    run(
        runner,
        ["--debug", "add", "hello3.txt"],
        expected_exit_code=1
    )

    # should work since the target file exists
    run(
        runner,
        ["--debug", "add", "hello4.txt"]
    )

    # create the meta files
    run(
        runner,
        ["--debug", "meta", "init"],
        expected_files=[Path("META/DocumentReference.ndjson")],
    )

    dvc = read_dvc(file_path="MANIFEST/hello4.txt.dvc")
    # capture expected object_id
    dvc.project_id = project_id
    expected_missing_object_id = dvc.object_id

    # commit the changes
    run(runner, ["--debug", "commit", "-am", "initial commit"])

    # push the changes
    run(runner, ["--debug", "push"])

    # rm the second test file after pushing
    run(
        runner,
        ["--debug", "rm", str("hello2.txt")],
    )

    # rm the 4th test file after pushing
    run(
        runner,
        ["--debug", "rm", str("hello4.txt")],
    )

    # re-create the meta file, with a bundle
    run(
        runner,
        ["--debug", "meta", "init", "--bundle"],
        expected_files=[Path("META/DocumentReference.ndjson"), Path("META/Bundle.ndjson")],
    )

    # commit the re-created meta
    run(runner, ["--debug", "commit", "-am", "updated commit"])

    # push to the server
    run(runner, ["--debug", "push", "--overwrite"])

    # list the files from indexd
    run(runner, ["--debug", "ls"], expected_output=["hello.txt"])

    # list the files from indexd, should not include the removed files
    with pytest.raises(AssertionError):
        run(runner, ["--debug", "ls"], expected_output=["hello2.txt"])

    with pytest.raises(AssertionError):
        run(runner, ["--debug", "ls"], expected_output=["hello4.txt"])

    # check the files exist in the graph and flat databases
    # we will need the object_id of the file to do that
    # should create a dvc file
    dvc = read_dvc("MANIFEST/hello.txt.dvc")
    # capture expected object_id
    dvc.project_id = project_id
    object_id = dvc.object_id
    auth = ensure_auth(config=default())

    ok = ''

    try:
        validate_document_in_grip(object_id, auth=auth, project_id=project_id)
    except Exception as e:
        ok = ok + f"Grip validation failed: {e}"

    try:
        validate_document_in_elastic(object_id, auth=auth)
    except Exception as e:
        ok = ok + f" Elastic validation failed: {e}"

    try:
        validate_document_in_grip(expected_missing_object_id, auth=auth, project_id=project_id)
        ok = ok + f" Grip validation failed should not have found: {expected_missing_object_id}"
    except Exception:
        pass

    try:
        validate_document_in_elastic(expected_missing_object_id, auth=auth)
        ok = ok + f" Elastic validation failed should not have found: {expected_missing_object_id}"
    except Exception:
        pass

    assert ok == '', ok

    # remove the project from the server.
    # TODO note, this does not remove the files from the bucket (UChicago bug)
    # See https://ohsucomputationalbio.slack.com/archives/C043HPV0VMY/p1714065633867229
    run(
        runner,
        [
            "--debug",
            "projects",
            "empty",
            "--project_id",
            project_id,
            "--confirm",
            "empty",
        ],
    )


def read_dvc(file_path="MANIFEST/my-project-data/hello.txt.dvc"):
    dvc_path = Path(file_path)
    assert dvc_path.exists(), f"{dvc_path} does not exist."
    with open(dvc_path) as f:
        yaml_data = yaml.safe_load(f)
    assert yaml_data
    dvc = DVC.model_validate(yaml_data)
    assert dvc, "DVC file not parsed."
    return dvc


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
