import json
import os
import pathlib
import subprocess
import uuid

import pytest
from click.testing import CliRunner

from gen3_util.cli.cli import cli

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


def import_from_directory(tmp_dir_name, project_id):
    """Import data from fixtures tests/fixtures/dir_to_study/."""
    params = f'meta  import dir tests/fixtures/dir_to_study/ {tmp_dir_name} --project_id {project_id}'.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    print(result.output)
    assert result.exit_code == 0
    expected_strings = ['DocumentReference', "size: 6013814", 'ResearchStudy', "count: 1"]
    for expected_string in expected_strings:
        assert expected_string in result.output, f"{expected_string} not found in {result.output}"

    result = runner.invoke(cli, f'meta validate {tmp_dir_name}'.split())
    print(result.output)
    assert result.exit_code == 0
    expected_strings = ["msg: OK"]
    for expected_string in expected_strings:
        assert expected_string in result.output, f"{expected_string} not found in {result.output}"


def ls():
    params = '--format json meta ls'.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    print(result.output)
    assert result.exit_code == 0
    expected_strings = ['"msg": "OK"', "is_metadata", "file_name"]
    for expected_string in expected_strings:
        assert expected_string in result.output, f"{expected_string} not found in {result.output}"
    return json.loads(result.output)


def meta_cp_upload(tmp_dir_name, project_id) -> dict:
    """Upload meta data file to data_bucket."""
    cmd_str = f'--format json meta push {tmp_dir_name} --project_id {project_id} --ignore_state'
    print(cmd_str)
    params = cmd_str.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    print('>>>', result.output, '<<<')
    assert result.exit_code == 0
    expected_strings = ['LOADED']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"{expected_string} not found in {result.output}"

    return json.loads(result.output)


def run_cmd(command_line) -> str:
    """Run a command line, return stdout."""
    try:
        return subprocess.check_output(command_line, shell=True).decode("utf-8").rstrip()
    except Exception as exc:
        raise exc


def meta_cp_download_via_gen3(did, tmp_dir_name):
    params = f'gen3 file download-single --path {tmp_dir_name} {did}'
    print(params)
    _ = run_cmd(params)


def meta_cp_download(did, tmp_dir_name):
    params = f'--format json meta cp {did} {tmp_dir_name}'.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    print(result.output)
    assert result.exit_code == 0
    expected_strings = ['Downloaded']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"{expected_string} not found in {result.output}"


def create_project(project_id):
    """Create a project in sheepdog."""
    params = f'--format json projects touch {project_id}'.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    print(result.output)
    assert result.exit_code == 0
    expected_strings = [project_id]
    for expected_string in expected_strings:
        assert expected_string in result.output, f"{expected_string} not found in {result.output}"


def add_policies(project_id):
    params = f'--format json projects add policies {project_id}'.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    result_output = result.output
    print(result_output)
    assert result.exit_code == 0
    expected_strings = [project_id]
    for expected_string in expected_strings:
        assert expected_string in result_output, f"{expected_string} not found in {result.output}"
    result_output = json.loads(result_output)
    assert 'Approve these requests' in result_output['msg']

    for command in result_output['commands']:
        command = command.replace('gen3_util ', '--format json ')
        result = runner.invoke(cli, command.split())
        result_output = result.output
        print(result_output)
        assert result.exit_code == 0
        result_output = json.loads(result_output)
        assert result_output['request']['status'] == 'SIGNED'


def files_cp_upload(tmp_dir_name, data_bucket, project_id):
    """Upload files to data_bucket."""
    runner = CliRunner()
    result = runner.invoke(cli, ['files', 'cp', '--project_id', project_id, '--ignore_state',
                                 f'{tmp_dir_name}/DocumentReference.ndjson', "bucket://"+data_bucket])

    print(result.output)
    assert result.exit_code == 0


def manifest_put(file_name,  project_id):
    """Upload a file with metadata to data_bucket."""
    runner = CliRunner()
    result = runner.invoke(cli, ['files', 'add', '--project_id', project_id, str(file_name)])

    print(result.output)
    assert result.exit_code == 0


def ensure_files_uploaded(project_id) -> list[str]:
    """Query files in indexd."""
    runner = CliRunner()
    result = runner.invoke(cli, ['--format', 'json', 'files', 'ls', '--project_id', project_id])
    assert result.exit_code == 0
    result_output = json.loads(result.output)
    assert 'records' in result_output, f"Should have records {result_output}"
    file_names = [_["file_name"] for _ in result_output['records']]
    assert sorted(file_names) == ['tests/fixtures/dir_to_study/file-1.txt', 'tests/fixtures/dir_to_study/file-2.csv',
                                  'tests/fixtures/dir_to_study/sub-dir/file-3.pdf',
                                  'tests/fixtures/dir_to_study/sub-dir/file-4.tsv',
                                  'tests/fixtures/dir_to_study/sub-dir/file-5'], result.output
    assert 'msg' in result_output, "Should have a msg"
    return [_["did"] for _ in result_output['records']]


def import_from_indexd(project_id) -> str:
    """Create data from indexd."""

    runner = CliRunner()
    result = runner.invoke(cli, f'meta create /tmp/{project_id} --project_id {project_id}'.split())
    assert result.exit_code == 0
    assert sorted([str(_).split('/')[-1] for _ in pathlib.Path(f"/tmp/{project_id}/").glob("*.ndjson")]) == ['DocumentReference.ndjson', 'ResearchStudy.ndjson']
    return f"/tmp/{project_id}"


def create_project_resource_in_arborist(project_id):
    """Create a project in arborist, sign the requests"""

    runner = CliRunner()
    result = runner.invoke(cli, f'--format json projects new --project_id {project_id}'.split())
    assert result.exit_code == 0, result.output

    result = runner.invoke(cli, '--format json access sign'.split())
    _, project = project_id.split('-')
    print(result.output)
    assert result.exit_code == 0
    assert project in result.output, result.output


def upload_manifest(project_id, profile):
    """Upload a manifest to indexd and bucket."""
    runner = CliRunner()
    result = runner.invoke(cli, f'--format json --profile {profile}  files push --project_id {project_id} --no_meta_data'.split())
    assert result.exit_code == 0, result.output


def rm_file(object_id, project_id, profile):
    """Remove a file from indexd and bucket."""
    runner = CliRunner()
    result = runner.invoke(cli, f'--format json  --profile {profile} files rm --object_id {object_id}'.split())
    assert result.exit_code == 0, result.output


def clone_project(project_id, profile, tmp_path):
    os.chdir(tmp_path)
    runner = CliRunner()
    result = runner.invoke(cli, f'--format json  --profile {profile} clone --project_id {project_id}'.split())
    assert result.exit_code == 0, result.output
    assert pathlib.Path(tmp_path, project_id).exists()
    assert pathlib.Path(tmp_path, project_id, 'META').exists()
    assert pathlib.Path(tmp_path, project_id, 'META', 'ResearchStudy.ndjson').exists()
    assert pathlib.Path(tmp_path, project_id, 'META', 'DocumentReference.ndjson').exists()


@pytest.mark.skip("deprecated")
def test_incremental_workflow(program, profile, tmp_path):
    """Test the workflow to create a project in incremental steps."""

    guid = str(uuid.uuid4())
    tmp_dir_name = f'tmp/{guid}'
    pathlib.Path(tmp_dir_name).mkdir(parents=True, exist_ok=True)
    print('created temporary directory', tmp_dir_name)

    project_id = f'{program}-TEST_INCREMENTAL_{guid.replace("-", "_")}'

    create_project_resource_in_arborist(project_id)

    c = 0
    for file_name in pathlib.Path('tests/fixtures/dir_to_study/').glob('**/*'):
        if file_name.is_file():
            manifest_put("file:///" + str(file_name), project_id)
            c += 1
    assert c > 0, "No files found"

    upload_manifest(project_id, profile)

    file_ids = ensure_files_uploaded(project_id)

    # create meta data, upload
    project_meta_data_dir = import_from_indexd(project_id)
    _ = meta_cp_upload(project_meta_data_dir, project_id)
    print(_)

    # remove a file
    rm_file(file_ids[0], project_id, profile)

    # should fail
    with pytest.raises(AssertionError):
        create_project_resource_in_arborist(project_id)

    # test clone
    clone_project(project_id, profile, tmp_path)
