
from click.testing import CliRunner
from gen3_util.cli.cli import cli


def test_project_ls(caplog):
    """Ensure we can retrieve project listing."""
    runner = CliRunner()
    result = runner.invoke(cli, ['buckets', 'ls'])
    assert result.exit_code == 0
    expected_strings = ['S3_BUCKETS', 'aced-ohsu']
    for expected_string in expected_strings:
        assert expected_string in result.output, "Did not find S3_BUCKETS."
