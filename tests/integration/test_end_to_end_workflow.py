import os
import pathlib
import pytest

import yaml
from click.testing import CliRunner

from gen3_tracker.config import ensure_auth, default
from gen3_tracker.git import DVC, run_command
from tests.integration import run, validate_document_in_psql_graph, validate_document_in_elastic


def test_simple_workflow(runner: CliRunner, project_id, tmpdir) -> None:
    """Test the init command."""
    # change to the temporary directory
    assert tmpdir.chdir()
    print("working directory:", pathlib.Path.cwd())
    print("project_id:", project_id)

    assert os.environ.get("G3T_PROFILE"), "Profile not found. Make sure to set and export G3T_PROFILE."

    run(runner, ["--debug", "init", project_id, "--approve"], expected_files=[".g3t", ".git"])

    # create a test file
    test_file = pathlib.Path("my-project-data/hello.txt")
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_text('hello\n')

    # add the file
    dvc_file = pathlib.Path(f"MANIFEST/{test_file}.dvc")
    run(runner, ["--debug", "add", str(test_file)], expected_files=[dvc_file])

    # should create a dvc file
    assert dvc_file.exists(), f"generated DVC {dvc_file} does not exist."
    with open(dvc_file) as f:
        yaml_data = yaml.safe_load(f)
    assert yaml_data
    dvc = DVC.model_validate(yaml_data)
    assert dvc, "DVC file not parsed."

    # capture expected object_id
    dvc.project_id = project_id
    object_id = dvc.object_id
    print("object_id:", object_id)

    # create the meta file
    run(runner, ["--debug", "meta", "init"], expected_files=["META/DocumentReference.ndjson"])

    # commit the changes, delegating to git
    run(runner, ["--debug", "commit", "-am", "initial commit"])

    # validate the meta files
    run(runner, ["--debug", "meta", "validate"])

    # create a visualisation
    run(runner, ["--debug", "meta", "graph"], expected_files=["meta.html"])

    # create a dataframe
    run(runner,  ["--debug", "meta", "dataframe", '--data_type', 'DocumentReference'], expected_files=["meta.csv"])

    # push to the server
    run(runner, ["--debug", "push"])

    # list the files from indexd
    run(runner, ["--debug", "ls"], expected_output=["my-project-data/hello.txt"])

    # check the files exist in the graph and flat databases
    auth = ensure_auth(config=default())

    # TODO: replace with grip testing
    # validate_document_in_psql_graph(object_id, auth=auth)

    validate_document_in_elastic(object_id, auth=auth)

    # clone the project in new directory
    clone_dir = pathlib.Path("clone")
    os.mkdir(clone_dir)
    os.chdir(clone_dir)
    run(runner, ["--debug", "clone", project_id])

    # pull the data
    run(runner, ["--debug", "pull"])
    
    # check the files exist in the cloned directory
    assert test_file.exists(), "hello.txt does not exist in the cloned directory."

    # remove the project from the server.
    # TODO note, this does not remove the files from the bucket (UChicago bug)
    # See https://ohsucomputationalbio.slack.com/archives/C043HPV0VMY/p1714065633867229
    run(runner, ["--debug", "projects", "empty", "--project_id", project_id, "--confirm", "empty"])
    run(runner, ["--debug", "projects", "rm", "--project_id", project_id])

    # TODO fix `collaborator rm`
    # arborist logs:  "Policy `data_upload` does not exist for user `xxx@xxx.xxx`: not revoking. Check if it is assigned through a group."
    # username = auth.curl('/user/user').json()['username']
    # run(runner, ["--debug", "collaborator", "rm", username, "--approve"], expected_output=[username])

    # add a user with write permissions
    run(runner, ["--debug", "collaborator", "add", "foo@bar.com", "--write", "--approve"])

    # add a user from another directory (without config)
    os.mkdir("empty")
    os.chdir("empty")
    program, project = project_id.split("-")
    run(runner, ["--debug", "collaborator", "add", "foo2@bar.com", f"/programs/{program}/projects/{project}", "--write", "--approve"])
