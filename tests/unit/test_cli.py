from click.testing import CliRunner
from gen3_util.cli.cli import cli


def test_default_command(caplog):
    """Ensure it prints version if no other command"""
    runner = CliRunner()
    result = runner.invoke(cli, ['version'])
    assert result.exit_code == 0
    expected = ['version']
    for _ in expected:
        assert _ in result.output, f"Should have printed expected={_} actual={result.output}"


def test_any_command(caplog):
    """Ensure it does not print version if command provided"""
    runner = CliRunner()
    result = runner.invoke(cli, ['config'])
    assert result.exit_code == 0
    un_expected_strings = ['Version']
    for expected_string in un_expected_strings:
        assert expected_string not in result.output, "only print version if nothing else to do"


def test_help(caplog):
    """Ensure it prints command groups"""
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    print(result.output)
    expected_strings = """
        projects  Manage Gen3 projects.
        buckets   Manage Gen3 buckets.
        meta      Manage meta data.
        files     Manage file transfers.
        access    Manage access requests.
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
      new  Creates project resource with default policies.
      ls   List all projects user has access to.
      rm   Remove project.
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
  publish   Publish meta data on the portal meta_data_path
  cp        Copy meta to/from the project bucket.
  ls        Query buckets for submitted metadata.
  rm        Remove meta from a project.
  create    Create minimal study meta from files uploaded to indexd
  validate  Validate FHIR data in DIRECTORY.
  node      Retrieve simplified metadata for a node.
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
      ls        List files in a project.
      manifest  Manage file transfers using a manifest.
      rm        Remove files from a project.
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
