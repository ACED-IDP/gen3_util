import json

from click.testing import CliRunner
from gen3_util.cli.cli import cli


def test_access_ls(caplog):
    """Ensure we can ls access."""
    runner = CliRunner()

    cmds = [['access', 'ls'], ['access', 'ls', '--username', 'bob@example.com'], ['access', 'ls', '--mine'], ['access', 'ls', '--all']]
    for cmd in cmds:
        result = runner.invoke(cli, cmd)
        result_output = result.output
        assert result.exit_code == 0, f"cmd {cmd} failed with {result_output}"


def test_access_cat(caplog):
    """Ensure we can ls access."""
    runner = CliRunner()

    result = runner.invoke(cli, ['--format', 'json', 'access', 'ls', '--all'])
    assert result.exit_code == 0, f"cmd failed with {result.output}"
    result_output = json.loads(result.output)
    request_id = result_output['requests'][0]['request_id']
    result = runner.invoke(cli, ['--format', 'json', 'access', 'cat', request_id])
    assert result.exit_code == 0, f"cmd failed with {result.output}"
