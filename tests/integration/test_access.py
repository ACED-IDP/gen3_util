import json
import uuid

from click.testing import CliRunner
from gen3_util.cli.cli import cli


def test_access_ls(caplog):
    """Ensure we can ls access."""
    runner = CliRunner()
    result = runner.invoke(cli, ['access', 'ls'])
    result_output = result.output
    print('result_output', result_output)
    assert result.exit_code == 0
    # This looks like a mixup with 'gen3_util projects ping'
    # expected_strings = ['OK']
    # for expected_string in expected_strings:
    # assert expected_string in result.output, f"Did not find {expected_string} in {expected_strings}"


def test_access_touch_read_only():
    """Ensure we can add a user with default read-only access."""
    runner = CliRunner()
    result = runner.invoke(cli, 'access touch bar@foo.com --project_id aced-Alcoholism'.split())
    assert result.exit_code == 0
    expected_strings = ['OK', 'request_id']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Did not find {expected_string} in {expected_strings}"


def test_access_touch_roles():
    """Ensure we can add a user with specific roles."""
    runner = CliRunner()
    result = runner.invoke(cli, 'access touch bar@foo.com --project_id aced-Alcoholism --roles storage_writer,file_uploader'.split())
    assert result.exit_code == 0
    expected_strings = ['OK', 'request_id']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Did not find {expected_string} in {expected_strings}"


def test_access_touch_bad_email():
    """Ensure we catch invalid email."""
    runner = CliRunner()
    result = runner.invoke(cli, 'access touch barfoo.com aced-Alcoholism --roles storage_writer,file_uploader'.split())
    assert result.exit_code != 0


def test_access_touch_bad_project_id():
    """Ensure we catch invalid email."""
    runner = CliRunner()
    result = runner.invoke(cli, 'access touch bar@foo.com aced-Alcoholism-XXX --roles storage_writer,file_uploader'.split())
    assert result.exit_code != 0


def test_access_workflow():
    """This access request in particular creates 409s if you run the test twice"""
    runner = CliRunner()

    user_name = str(uuid.uuid4())
    result = runner.invoke(cli, f'--format json access touch {user_name}@foo.com --project_id aced-MCF10A'.split())
    print(result.output)
    assert result.exit_code == 0
    expected_strings = ['OK', 'request_id']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Did not find {expected_string} in {expected_strings}"

    request = json.loads(result.output)
    print("THE VALEU OF REQUEST ", request)
    assert request['request_id'], "Missing request id"
    request_id = request['request_id']
    assert request['status'] == 'DRAFT', f"unexpected status {request['status']}"

    result = runner.invoke(cli, f'--format json access update {request_id} SUBMITTED'.split())
    assert result.exit_code == 0
    expected_strings = ['request_id']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Did not find {expected_string} in {expected_strings}"

    request = json.loads(result.output)["request"]

    assert request['status'] == 'SUBMITTED', f"unexpected status {request['status']}"

    result = runner.invoke(cli, f'--format json access update {request_id} APPROVED'.split())

    result_output = result.output
    print('result_output', result_output)
    assert result.exit_code == 0

    expected_strings = ['request_id']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Did not find {expected_string} in {expected_strings}"
    request = json.loads(result.output)["request"]

    assert request['status'] == 'APPROVED', f"unexpected status {request['status']}"

    result = runner.invoke(cli, f'--format json access update {request_id} SIGNED'.split())
    result_output = result.output
    print('result_output', result_output)
    assert result.exit_code == 0
    expected_strings = ['request_id']
    for expected_string in expected_strings:
        assert expected_string in "".join(result.output), f"Did not find {expected_string} in {expected_strings}"
    request = json.loads(result.output)["request"]
    assert request['status'] == 'SIGNED', f"unexpected status {request['status']}"
