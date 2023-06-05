
from click.testing import CliRunner
from gen3_util.cli.cli import cli


def test_project_ls(caplog, data_bucket):
    """Ensure we can retrieve project listing."""
    runner = CliRunner()
    result = runner.invoke(cli, ['buckets', 'ls'])
    assert result.exit_code == 0
    expected_strings = ['S3_BUCKETS', data_bucket]
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Did not find {expected_string}."
