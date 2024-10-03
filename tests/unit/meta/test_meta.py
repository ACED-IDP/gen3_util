import os
import pathlib

import yaml
from click.testing import CliRunner

import gen3_tracker.config
from gen3_tracker.git import run_command
from tests import run


def test_assert_object_id_invalid_on_project_id_change(runner: CliRunner, project_id, tmp_path: pathlib.Path) -> None:
    """Test object_id validation command."""
    # change to the temporary directory
    os.chdir(tmp_path)
    print(pathlib.Path.cwd())
    print(project_id)

    run(runner, ["--debug", "--profile", "local", "init", project_id, "--no-server"],
        expected_files=[".g3t", ".git"])

    # create test files
    cmds = """
    mkdir my-project-data
    mkdir my-read-only-data
    echo "hello" > my-project-data/hello.txt
    echo "big-data" > my-read-only-data/big-file.txt
    ln -s $PWD/my-read-only-data/big-file.txt my-project-data/big-file.txt
    """.split('\n')
    for cmd in cmds:
        run_command(cmd, no_capture=True)

    assert pathlib.Path("my-project-data/hello.txt").exists(), "hello.txt does not exist."
    assert pathlib.Path("my-read-only-data/big-file.txt").exists(), "my-read-only-data/big-file.txt does not exist."
    assert pathlib.Path("my-project-data/big-file.txt").exists(), "my-project-data/big-file.txt does not exist."

    files = ["my-project-data/hello.txt", "my-project-data/big-file.txt"]
    patients = ["P1", "P2"]
    for f, p in zip(files, patients):
        run(runner, ["--debug", "add", str(f), "--patient", p], expected_files=[f"MANIFEST/{f}.dvc"])

    run(runner, ["--debug", "meta", "init"], expected_files=["META/DocumentReference.ndjson", "META/Patient.ndjson", "META/ResearchStudy.ndjson", "META/ResearchSubject.ndjson"])
    run(runner, ["--debug", "meta", "validate"])
    run(runner, ["commit", "-m",  "init", "MANIFEST/", "META/", ".g3t", ".gitignore"])

    # now change the project_id to something new
    # this should cause invalid object_id errors
    config = gen3_tracker.config.default()
    config.gen3.project_id = config.gen3.project_id + "XXXX"
    with open('.g3t/config.yaml', 'w') as f:
        yaml.dump(config.model_dump(), f)
    run(runner, ["commit", "-m",  "change-project_id", '.g3t/config.yaml'])

    # should error now
    run(runner, ["--debug", "meta", "validate"], expected_exit_code=1)
    run(runner, ["--debug", "push", "--dry-run"], expected_exit_code=1)
    # also check skip_validate
    run(runner, ["--debug", "push", "--dry-run", "--skip_validate"], expected_exit_code=0)

    # should pass now
    config.gen3.project_id = config.gen3.project_id.replace("XXXX", "")
    with open('.g3t/config.yaml', 'w') as f:
        yaml.dump(config.model_dump(), f)
    run(runner, ["commit", "-m",  "restore-project_id", '.g3t/config.yaml'])

    run(runner, ["--debug", "meta", "validate"], expected_exit_code=0)
    run(runner, ["--debug", "push", "--dry-run"], expected_exit_code=0)


def test_assert_add_specimen_after_init(runner: CliRunner, project_id, tmp_path: pathlib.Path) -> None:
    """Test meta skeleton handles re-add of data with new specimen"""
    # change to the temporary directory
    os.chdir(tmp_path)
    print(pathlib.Path.cwd())
    print(project_id)

    # init the project, no server
    run(runner, ["--debug", "--profile", "local", "init", project_id, "--no-server"],
        expected_files=[".g3t", ".git"])

    # create test files
    cmds = """
    mkdir my-project-data
    mkdir my-read-only-data
    echo "hello" > my-project-data/hello.txt
    echo "big-data" > my-read-only-data/big-file.txt
    ln -s $PWD/my-read-only-data/big-file.txt my-project-data/big-file.txt
    """.split('\n')
    for cmd in cmds:
        run_command(cmd, no_capture=True)

    assert pathlib.Path("my-project-data/hello.txt").exists(), "hello.txt does not exist."
    assert pathlib.Path("my-read-only-data/big-file.txt").exists(), "my-read-only-data/big-file.txt does not exist."
    assert pathlib.Path("my-project-data/big-file.txt").exists(), "my-project-data/big-file.txt does not exist."

    def _files_with_patients():
        files = ["my-project-data/hello.txt", "my-project-data/big-file.txt"]
        patients = ["P1", "P2"]
        for f, p in zip(files, patients):
            run(runner, ["--debug", "add", str(f), "--patient", p], expected_files=[f"MANIFEST/{f}.dvc"])

        run(runner, ["--debug", "meta", "init"], expected_files=["META/DocumentReference.ndjson", "META/Patient.ndjson", "META/ResearchStudy.ndjson", "META/ResearchSubject.ndjson"])
        run(runner, ["--debug", "meta", "validate"])
        run(runner, ["commit", "-m",  "init", "MANIFEST/", "META/", ".g3t", ".gitignore"])

    def _files_with_patients_and_specimens():
        files = ["my-project-data/hello.txt", "my-project-data/big-file.txt"]
        patients = ["P1", "P2"]
        specimens = ["S1", "S2"]
        for f, p, s in zip(files, patients, specimens):
            run(runner, ["--debug", "add", str(f), "--patient", p, "--specimen", s], expected_files=[f"MANIFEST/{f}.dvc"])

        run(runner, ["--debug", "meta", "init"], expected_files=["META/DocumentReference.ndjson", "META/Patient.ndjson", "META/ResearchStudy.ndjson", "META/ResearchSubject.ndjson", "META/Specimen.ndjson"])
        run(runner, ["--debug", "meta", "validate"])
        run(runner, ["commit", "-m", "init", "MANIFEST/", "META/", ".g3t", ".gitignore"])

    # create initial association between patients and files
    _files_with_patients()
    # now add association between patients, specimens and files
    _files_with_patients_and_specimens()
    # should still pass
    run(runner, ["--debug", "push", "--dry-run"], expected_exit_code=0)
