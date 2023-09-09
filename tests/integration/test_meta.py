import json
import pathlib
import shutil
import subprocess
import uuid

from click.testing import CliRunner

from gen3_util.cli.cli import cli


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


def meta_cp_upload(tmp_dir_name, data_bucket, project_id) -> dict:
    """Upload meta data file to data_bucket."""
    params = f'--format json meta cp {tmp_dir_name} bucket://{data_bucket} --project_id {project_id} --ignore_state'.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    print(result.output)
    assert result.exit_code == 0
    expected_strings = ['Uploaded']
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


def import_metadata(project_id, object_id):
    """import data into sheepdog and elastic."""
    params = f'--format json jobs import {project_id} {object_id}'.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    print(params)
    print(result.output)
    assert result.exit_code == 0
    expected_strings = [project_id]
    for expected_string in expected_strings:
        assert expected_string in result.output, f"{expected_string} not found in {result.output}"


def files_cp_upload(tmp_dir_name, data_bucket, project_id):
    """Upload files to data_bucket."""
    runner = CliRunner()
    result = runner.invoke(cli, ['files', 'cp', '--project_id', project_id, '--ignore_state',
                                 f'{tmp_dir_name}/DocumentReference.ndjson', "bucket://"+data_bucket])

    print(result.output)
    assert result.exit_code == 0


def files_put(file_name, data_bucket, project_id):
    """Upload a file with meta data to data_bucket."""
    runner = CliRunner()
    result = runner.invoke(cli, ['files', 'put', '--project_id', project_id, '--ignore_state',
                                 str(file_name), "bucket://"+data_bucket])

    print(result.output)
    assert result.exit_code == 0


def ensure_files_uploaded(project_id):
    """Query files in indexd."""
    runner = CliRunner()
    result = runner.invoke(cli, ['--format', 'json', 'files', 'ls', '--project_id', project_id])
    assert result.exit_code == 0
    file_names = [_["file_name"] for _ in json.loads(result.output)['records']]
    assert sorted(file_names) == ['tests/fixtures/dir_to_study/file-1.txt', 'tests/fixtures/dir_to_study/file-2.csv',
                                  'tests/fixtures/dir_to_study/sub-dir/file-3.pdf',
                                  'tests/fixtures/dir_to_study/sub-dir/file-4.tsv',
                                  'tests/fixtures/dir_to_study/sub-dir/file-5'], result.output


def test_workflow(program, data_bucket):
    """Test the workflow to create a project in one step, aka a published project."""

    guid = str(uuid.uuid4())
    tmp_dir_name = f'tmp/{guid}'
    pathlib.Path(tmp_dir_name).mkdir(parents=True, exist_ok=True)
    print('created temporary directory', tmp_dir_name)

    project_id = f'{program}-TEST_{guid.replace("-", "_")}'

    create_project(project_id)
    # add_policies(project_id)

    import_from_directory(tmp_dir_name, project_id)
    files_cp_upload(tmp_dir_name, data_bucket, project_id)
    ensure_files_uploaded(project_id)

    meta_cp_upload(tmp_dir_name, data_bucket, project_id)
    program, project = project_id.split('-')
    records = ls()['records']
    found_our_project = False
    for _ in records:
        if f"/programs/{program}/projects/{project}" not in _['authz']:
            continue
        found_our_project = True
        meta_cp_download(_['did'], tmp_dir_name)
        stat = pathlib.Path(f"{tmp_dir_name}/{_['file_name']}").stat()
        assert stat.st_size == _['size']
        # should also be able to use `gen3 file download-single`
        meta_cp_download_via_gen3(_['did'], tmp_dir_name)
        # import metadata
        import_metadata(project_id, _['did'])
    assert found_our_project, f"Did not find our project {project_id} in {records}"

    shutil.rmtree(tmp_dir_name)


def import_from_indexd(project_id) -> str:
    """Create data from indexd."""

    runner = CliRunner()
    result = runner.invoke(cli, f'meta import indexd /tmp/{project_id} --project_id {project_id}'.split())
    assert result.exit_code == 0
    assert sorted([str(_).split('/')[-1] for _ in pathlib.Path(f"/tmp/{project_id}/").glob("*.ndjson")]) == ['DocumentReference.ndjson', 'ResearchStudy.ndjson']
    return f"/tmp/{project_id}"


def create_project_resource_in_arborist(project_id):
    """Create a project in arborist, sign the requests"""

    runner = CliRunner()
    result = runner.invoke(cli, f'--format json projects add resource {project_id}'.split())
    assert result.exit_code == 0

    request = json.loads(result.output)
    # get the `commands` convenience command lines to sign the requests
    commands = request['commands']
    for command in commands:
        command = command.replace('gen3_util', '')
        result = runner.invoke(cli, f'--format json {command}'.split())
        print(result.output)
        assert result.exit_code == 0


def test_incremental_workflow(program, data_bucket):
    """Test the workflow to create a project in incremental steps."""

    guid = str(uuid.uuid4())
    tmp_dir_name = f'tmp/{guid}'
    pathlib.Path(tmp_dir_name).mkdir(parents=True, exist_ok=True)
    print('created temporary directory', tmp_dir_name)

    project_id = f'{program}-TEST_INCREMENTAL_{guid.replace("-", "_")}'

    create_project_resource_in_arborist(project_id)

    for file_name in pathlib.Path('tests/fixtures/dir_to_study/').glob('**/*'):
        if file_name.is_file():
            files_put(file_name, data_bucket, project_id)

    ensure_files_uploaded(project_id)

    # create meta data, upload
    project_meta_data_dir = import_from_indexd(project_id)
    object_id = meta_cp_upload(project_meta_data_dir, data_bucket, project_id)['object_id']
    print(f"uploaded meta data {object_id}")
    import_metadata(project_id, object_id)
    print(f"imported meta data {object_id} {project_id}")
    assert False, "TODO: test incremental workflow"
