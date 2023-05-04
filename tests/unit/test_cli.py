from click.testing import CliRunner
from gen3_util.cli.cli import cli


def test_default_command(caplog):
    """Ensure it prints version"""
    runner = CliRunner()
    result = runner.invoke(cli)  # , ['--config', 'tests/fixtures/custom_config/config.yaml'])
    assert result.exit_code == 0
    expected_strings = ['Version']
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Should have printed {expected_string}"


def test_help(caplog):
    """Ensure it prints command groups"""
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    print(result.output)
    expected_strings = """
      projects  Manage Gen3 projects.
      meta      Manage meta data.
      files     Manage file buckets.
      config    Configure this utility.
    """.split()
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Should have printed {expected_string}"


def test_projects(caplog):
    """Ensure it prints project"""
    runner = CliRunner()
    result = runner.invoke(cli, ['projects', '--help'])
    assert result.exit_code == 0
    print(result.output)
    expected_strings = """
      ls     List all projects.
      touch  Create a project
      rm     Remove project.
    """.split()
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Should have printed {expected_string}"


def test_meta(caplog):
    """Ensure it prints meta"""
    runner = CliRunner()
    result = runner.invoke(cli, ['meta', '--help'])
    assert result.exit_code == 0
    print(result.output)
    expected_strings = """
      ls        List meta in a project.
      validate  Copy meta to/from the project bucket.
      cp        Copy meta to/from the project bucket.
      rm        Remove meta from a project.
    """.split()
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Should have printed {expected_string}"


def test_file(caplog):
    """Ensure it prints file"""
    runner = CliRunner()
    result = runner.invoke(cli, ['files', '--help'])
    assert result.exit_code == 0
    print(result.output)
    expected_strings = """
      ls  List files in a project.
      cp  Copy files to/from the project bucket.
      rm  Remove files from a project.
    """.split()
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Should have printed {expected_string}"


def test_config(caplog):
    """Ensure it prints file"""
    runner = CliRunner()
    result = runner.invoke(cli, ['config', '--help'])
    assert result.exit_code == 0
    print(result.output)
    expected_strings = """
      ls  Show defaults.
    """.split()
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Should have printed {expected_string}"
