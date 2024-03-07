
from click.testing import CliRunner
from gen3_util.repo.cli import cli


def test_project_ls(caplog):
    """Ensure we can retrieve project listing."""
    runner = CliRunner()
    result = runner.invoke(cli, ['util', 'buckets', 'ls'])
    assert result.exit_code == 0
    expected_strings = ['S3_BUCKETS', 'programs']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Did not find {expected_string}."
