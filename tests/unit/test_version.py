from click.testing import CliRunner
from gen3_util.repo.cli import cli


def test_version():
    """Ensure it prints version"""
    runner = CliRunner()
    result = runner.invoke(cli, ['--version'])
    assert result.exit_code == 0
    # assert 'version' in result.output
    # _, version = result.output.split(':')
    version = result.output
    version = version.strip()
    major, minor, patch = version.split('.')
    assert int(major) >= 0
    assert int(minor) >= 0
    if '+' in patch:
        patch, build = patch.split('+')
    if 'rc' in patch:
        patch, build = patch.split('rc')
    assert int(patch) >= 0
