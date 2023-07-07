from click.testing import CliRunner

from gen3_util.cli.cli import cli


def test_project_ls(caplog):
    """Ensure we can retrieve project listing."""
    runner = CliRunner()
    result = runner.invoke(cli, ['projects', 'ls'])
    assert result.exit_code == 0
    expected_strings = ['/programs/aced']
    for expected_string in expected_strings:
        assert expected_string in result.output, "Did not find project listing."


def test_project_ping(caplog):
    """Ensure we can retrieve connected message."""
    runner = CliRunner()
    result = runner.invoke(cli, ['projects', 'ping'])
    assert result.exit_code == 0
    expected_strings = ['OK', 'http', 'username']
    for expected_string in expected_strings:
        assert expected_string in result.output, "Did not find connected message."


def test_project_bad_ping(caplog):
    """Ensure we can descriptive error."""
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ['--cred', 'BAD_PATH', 'projects', 'ping'], )
    print(result.stdout)
    assert result.exit_code == 1
    expected_strings = ['Could not get access.']
    for expected_string in expected_strings:
        assert expected_string in result.stdout, "Did not find failure message."
