from click.testing import CliRunner
from gen3_util.cli.cli import cli


def test_config_ls(caplog):
    """Ensure we can ls access."""
    runner = CliRunner()
    result = runner.invoke(cli, ['config', 'ls'])
    assert result.exit_code == 0
