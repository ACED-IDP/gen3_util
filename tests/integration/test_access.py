import json

from click.testing import CliRunner
from gen3_util.cli.cli import cli


def test_access_ls(caplog):
    """Ensure we can ls access."""
    runner = CliRunner()
    result = runner.invoke(cli, ['access', 'ls'])
    assert result.exit_code == 0
    expected_strings = ['OK']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Did not find {expected_string} in {expected_strings}"


def test_access_touch_read_only():
    """Ensure we can add a user with default read-only access."""
    runner = CliRunner()
    result = runner.invoke(cli, 'access touch bar@foo.com aced-Alcoholism'.split())
    assert result.exit_code == 0
    expected_strings = ['OK', 'request_id']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Did not find {expected_string} in {expected_strings}"


def test_access_touch_roles():
    """Ensure we can add a user with specific roles."""
    runner = CliRunner()
    result = runner.invoke(cli, 'access touch bar@foo.com aced-Alcoholism --roles storage_writer,file_uploader'.split())
    assert result.exit_code == 0
    expected_strings = ['OK', 'request_id']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Did not find {expected_string} in {expected_strings}"


def test_access_workflow():
    runner = CliRunner()
    result = runner.invoke(cli, '--format json access touch bar@foo.com aced-MCF10A'.split())
    assert result.exit_code == 0
    expected_strings = ['OK', 'request_id']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Did not find {expected_string} in {expected_strings}"
    request = json.loads(result.output)['request']
    assert request['request_id'], "Missing request id"
    request_id = request['request_id']
    assert request['status'] == 'DRAFT', f"unexpected status {request['status']}"

    print(f'--format json access access update {request_id} SUBMITTED')
    result = runner.invoke(cli, f'--format json access update {request_id} SUBMITTED'.split())
    assert result.exit_code == 0
    expected_strings = ['OK', 'request_id']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Did not find {expected_string} in {expected_strings}"
    request = json.loads(result.output)['request']
    assert request['status'] == 'SUBMITTED', f"unexpected status {request['status']}"

    result = runner.invoke(cli, f'--format json access update {request_id} APPROVED'.split())
    assert result.exit_code == 0
    expected_strings = ['OK', 'request_id']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Did not find {expected_string} in {expected_strings}"
    request = json.loads(result.output)['request']
    assert request['status'] == 'APPROVED', f"unexpected status {request['status']}"

    result = runner.invoke(cli, f'--format json access update {request_id} SIGNED'.split())
    assert result.exit_code == 0
    expected_strings = ['OK', 'request_id']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Did not find {expected_string} in {expected_strings}"
    request = json.loads(result.output)['request']
    assert request['status'] == 'SIGNED', f"unexpected status {request['status']}"
