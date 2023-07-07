
from click.testing import CliRunner

from gen3_util.cli.cli import cli


def test_files_upload_bad_source(caplog, data_bucket):
    """Ensure we detect missing input path."""
    runner = CliRunner()
    result = runner.invoke(cli, ['files', 'cp', '--project_id', 'prog-proj', 'foo', f'bucket://{data_bucket}'])
    assert result.exit_code == 1
    expected_strings = ['foo does not exist']
    for expected_string in expected_strings:
        assert expected_string in result.output, "Did not find project does not exist error"


def test_files_upload(caplog, data_bucket):
    """Ensure we upload files from generated DocumentReferences to existing project."""

    runner = CliRunner()

    params = 'meta  import dir tests/fixtures/dir_to_study/ tmp/Alcoholism --project_id aced-Alcoholism'.split()
    result = runner.invoke(cli, params)
    print(result.output)
    assert result.exit_code == 0, "Could not create project from directory"

    result = runner.invoke(cli, ['files', 'cp', '--project_id', 'aced-XXXX', 'tmp/Alcoholism', 'bucket://bar'])
    assert result.exit_code == 1
    print(result.output)
    expected_strings = ['aced-XXXX does not exist']
    for expected_string in expected_strings:
        assert expected_string in result.output, "Did not find project does not exist error"

    result = runner.invoke(cli, ['files', 'cp', '--project_id', 'aced-Alcoholism', 'tmp/Alcoholism', 'bucket://bar'])
    assert result.exit_code == 1
    print(result.output)
    expected_strings = ['bar not in configured buckets']
    for expected_string in expected_strings:
        assert expected_string in result.output, "Did not find bucket does not exist error"

    result = runner.invoke(cli, ['files', 'cp', '--project_id', 'aced-Alcoholism', '--ignore_state',
                                 'tmp/Alcoholism/DocumentReference.ndjson', "bucket://"+data_bucket])
    assert result.exit_code == 0
    print(result.output)
    expected_strings = ['OK']
    for expected_string in expected_strings:
        assert expected_string in result.output, "Did not find OK message"
