import json
import pathlib

from click.testing import CliRunner

from gen3_util.cli.cli import cli


def import_from_directory():
    params = 'meta  import dir tests/fixtures/dir_to_study/ tmp/foo --project_id aced-foo'.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    print(result.output)
    assert result.exit_code == 0
    expected_strings = ['DocumentReference', "size: 6013814", 'ResearchStudy', "count: 1"]
    for expected_string in expected_strings:
        assert expected_string in result.output, f"{expected_string} not found in {result.output}"

    result = runner.invoke(cli, 'meta validate tmp/foo'.split())
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


def cp_upload(data_bucket):
    params = f'--format json meta cp tmp/foo/extractions bucket://{data_bucket} --project_id aced-foo --ignore_state'.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    print(result.output)
    assert result.exit_code == 0
    expected_strings = ['Uploaded']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"{expected_string} not found in {result.output}"


def cp_download(did):
    params = f'--format json meta cp {did} tmp/foo/'.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    print(result.output)
    assert result.exit_code == 0
    expected_strings = ['Downloaded']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"{expected_string} not found in {result.output}"


def test_workflow(data_bucket):
    import_from_directory()
    cp_upload(data_bucket)
    records = ls()['records']
    for _ in records:
        cp_download(_['did'])
        stat = pathlib.Path(f"tmp/foo/{_['file_name']}").stat()
        assert stat.st_size == _['size']
