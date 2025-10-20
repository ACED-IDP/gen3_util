import os
import shutil
import pandas as pd
import yaml
from click.testing import CliRunner

from gen3_tracker.config import ensure_auth, default
from gen3_tracker.git import DVC, run_command
from pathlib import Path
from tests.integration import validate_document_in_elastic, validate_document_in_grip
from tests import run


def test_simple_workflow(runner: CliRunner, project_id, tmpdir) -> None:
    """Test the init command."""
    # change to the temporary directory
    assert tmpdir.chdir()
    print(Path.cwd())

    assert os.environ.get(
        "G3T_PROFILE"
    ), "G3T_PROFILE environment variable must be set."

    print(project_id)

    run(
        runner,
        ["--debug", "init", project_id, "--approve"],
        expected_files=[".g3t", ".git"],
    )

    # check ping
    run(
        runner,
        ["--debug", "ping"],
        expected_output=["bucket_programs", "your_access", "endpoint", "username"],
    )

    # create a test file
    test_file = Path("my-project-data/hello.txt")
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_text("hello\n")

    # add the file
    run(
        runner,
        ["--debug", "add", str(test_file)],
        expected_files=["MANIFEST/my-project-data/hello.txt.dvc"],
    )

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

    # create the meta file
    run(
        runner,
        ["--debug", "meta", "init"],
        expected_files=["META/DocumentReference.ndjson"],
    )

    # commit the changes, delegating to git
    run(runner, ["--debug", "commit", "-am", "initial commit"])

    # validate the meta files
    run(runner, ["--debug", "meta", "validate"])

    # update the file
    test_file = Path("my-project-data/hello.txt")
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_text("hello UPDATE\n")
    # re-add the file
    run(
        runner,
        ["--debug", "add", str(test_file)],
        expected_files=["MANIFEST/my-project-data/hello.txt.dvc"],
    )
    run(
        runner,
        ["--debug", "meta", "init"],
        expected_files=["META/DocumentReference.ndjson"],
    )
    run(runner, ["--debug", "commit", "-am", "updated"])
    run(runner, ["--debug", "meta", "validate"])

    # create a visualisation
    run(runner, ["--debug", "meta", "graph"], expected_files=["meta.html"])

    # create a dataframe
    run(
        runner,
        ["--debug", "meta", "dataframe", "DocumentReference"],
        expected_files=["DocumentReference.csv"],
    )

    # push to the server
    run(runner, ["--debug", "push"])

    # list the files from indexd
    run(runner, ["--debug", "ls"], expected_output=["my-project-data/hello.txt"])

    # check the files exist in the graph and flat databases
    auth = ensure_auth(config=default())
    validate_document_in_grip(object_id, auth=auth, project_id=project_id)
    validate_document_in_elastic(object_id, auth=auth)

    # clone the project in new directory
    clone_dir = Path("clone")
    os.mkdir(clone_dir)
    os.chdir("clone")
    run(runner, ["--debug", "clone", project_id])

    # create a new directory, cd to it
    os.mkdir("clone")
    os.chdir("clone")

    # clone the project
    run(runner, ["--debug", "clone", project_id])
    # pull the data
    run(runner, ["--debug", "pull"])
    # check the files exist in the cloned directory

    # check the files exist in the cloned directory
    run_command("ls -l")

    assert Path(
        "my-project-data/hello.txt"
    ).exists(), "hello.txt does not exist in the cloned directory."

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


def test_can_approve_collaborator_with_write_permissions(
    runner: CliRunner, project_id, tmpdir
) -> None:
    """Test the collaborator add command."""
    # change to the temporary directory
    assert tmpdir.chdir()
    print(Path.cwd())

    assert os.environ.get(
        "G3T_PROFILE"
    ), "G3T_PROFILE environment variable must be set."

    print(project_id)

    run(
        runner,
        ["--debug", "init", project_id, "--approve"],
        expected_files=[".g3t", ".git"],
    )

    # TODO fix `collaborator rm`
    # arborist logs:  "Policy `data_upload` does not exist for user `xxx@xxx.xxx`: not revoking. Check if it is assigned through a group."
    # username = auth.curl('/user/user').json()['username']
    # run(runner, ["--debug", "collaborator", "rm", username, "--approve"], expected_output=[username])

    # add a user with write permissions
    run(
        runner,
        ["--debug", "collaborator", "add", "foo@bar.com", "--write", "--approve"],
    )

    # add a user from another directory (without config)
    os.mkdir("empty")
    os.chdir("empty")
    program, project = project_id.split("-")
    run(
        runner,
        [
            "--debug",
            "collaborator",
            "add",
            "foo2@bar.com",
            f"/programs/{program}/projects/{project}",
            "--write",
            "--approve",
        ],
    )


def test_simple_fhir_server_workflow(runner: CliRunner, project_id, tmpdir) -> None:
    """Test the init command."""
    # change to the temporary directory
    assert tmpdir.chdir()
    print(Path.cwd())

    assert os.environ.get(
        "G3T_PROFILE"
    ), "G3T_PROFILE environment variable must be set."

    print(project_id)

    run(
        runner,
        ["--debug", "init", project_id, "--approve"],
        expected_files=[".g3t", ".git"],
    )

    # create a test file
    test_file = Path("my-project-data/hello.txt")
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_text("hello\n")

    # add the file
    run(
        runner,
        ["--debug", "add", str(test_file)],
        expected_files=["MANIFEST/my-project-data/hello.txt.dvc"],
    )

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

    # create the meta file
    run(
        runner,
        ["--debug", "meta", "init"],
        expected_files=["META/DocumentReference.ndjson"],
    )

    # commit the changes, delegating to git
    run(runner, ["--debug", "commit", "-am", "initial commit"])

    # validate the meta files
    run(runner, ["--debug", "meta", "validate"])

    # push to the server
    run(runner, ["--debug", "push", "--fhir-server"])

    # list the files from indexd
    run(runner, ["--debug", "ls"], expected_output=["my-project-data/hello.txt"])

    # check the files exist in the graph and flat databases
    auth = ensure_auth(config=default())

    # elastic not currently working with this dataframer etl image with specific metadata.
    # validate_document_in_elastic(object_id, auth=auth)
    validate_document_in_grip(object_id, auth=auth, project_id=project_id)

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


def test_push_fails_with_invalid_doc_ref_creation_date(
    runner: CliRunner, project_id: str, tmp_path: Path
):

    # check
    assert os.environ.get(
        "G3T_PROFILE"
    ), "G3T_PROFILE environment variable must be set."

    # copy fixture to temp test dir
    project_dir = "fhir-gdc-examples"
    fixtures_path = Path(os.path.dirname(__file__)).parent / "fixtures"
    fhir_gdc_dir = fixtures_path / project_dir
    modified_doc_ref_path = (
        fixtures_path
        / "negative-examples/fhir-gdc-DocumentReference-invalid-date.ndjson"
    )

    # init project
    new_project_dir = tmp_path / project_dir
    shutil.copytree(fhir_gdc_dir, new_project_dir)
    shutil.copy(
        modified_doc_ref_path, new_project_dir / "META" / "DocumentReference.ndjson"
    )

    # get invalid date from fixture
    doc_ref_content = pd.read_json(modified_doc_ref_path, lines=True)["content"][0]
    invalid_date = doc_ref_content[0]["attachment"]["creation"]

    # ensure that push fails and writes to logs
    log_file_path = "logs/publish.log"
    os.chdir(new_project_dir)
    run(runner, ["init", project_id, "--approve"])
    result = run(
        runner,
        ["push", "--skip_validate", "--overwrite"],
        expected_exit_code=1,
        expected_files=[log_file_path],
    )

    # ensure push has useful useful error logs
    assert (
        log_file_path in result.output
    ), f"expected log file path in stdout, instead got:\n{result.output}"

    # ensure saved log file contains info about invalid date
    with open(log_file_path, "r") as log_file:
        lines = log_file.readlines()
        str_lines = str(lines)

        for keyword in ["/content/0/attachment/creation", "jsonschema", invalid_date]:
            assert (
                keyword in str_lines
            ), f'expected log file to contain keyword "{keyword}", instead got: \n{str_lines}'


def test_push_fails_with_no_write_permissions(
    runner: CliRunner, project_id: str, tmp_path: Path
):

    # setup
    assert os.environ.get(
        "G3T_PROFILE"
    ), "G3T_PROFILE environment variable must be set."
    os.chdir(tmp_path)

    # initialize project without approving permissions
    log_file_path = "logs/publish.log"
    run(runner, ["init", project_id], expected_files=[".g3t", ".git"])

    # create test file
    test_file = Path("my-project-data/hello.txt")
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_text("hello\n")

    # prepare test file for submission
    run(
        runner,
        ["add", str(test_file)],
        expected_files=["MANIFEST/my-project-data/hello.txt.dvc"],
    )
    run(runner, ["meta", "init"], expected_files=["META/DocumentReference.ndjson"])
    print("current directory:", os.getcwd())
    run(runner, ["commit", "-m", "initial commit"])

    # push
    result = run(runner, ["push"], expected_exit_code=1, expected_files=[log_file_path])

    # ensure stdout mentions log files
    assert (
        log_file_path in result.output
    ), f"expected log file path in stdout, instead got:\n{result.output}"

    # check valid error messages within
    with open(log_file_path, "r") as log_file:
        # grab last line
        line = [_ for _ in log_file.readlines()][-1]
        for output in ["401", "permission"]:
            assert (
                "401" in line
            ), f"expected {log_file_path} to contain {output}, instead got: \n{line}"
