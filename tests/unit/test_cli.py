from click.testing import CliRunner
from gen3_util.repo.cli import cli


def test_any_command(caplog):
    """Ensure it does not print version if command provided"""
    runner = CliRunner()
    result = runner.invoke(cli, 'utilities config'.split())
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
  ping       Verify gen3-client and test connectivity.
  init       Create project, both locally and on remote.
  add        Add file to the index.
  commit     Record changes to the project.
  push       Submit committed changes to commons.
  status     Show the working tree status.
  clone      Clone meta and files from remote.
  pull       Download data files.
  utilities  Useful utilities.
        """.split()
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Should have printed {expected_string}"


def test_projects(caplog):
    """Ensure it prints project"""
    runner = CliRunner()
    result = runner.invoke(cli, 'utilities projects --help'.split())
    assert result.exit_code == 0
    print(result.output)
    expected_strings = """
      ls   List all projects user has access to.
      rm   Remove project.
    """.split()
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Should have printed {expected_string}"


def test_meta(caplog):
    """Ensure it prints meta"""
    runner = CliRunner()
    result = runner.invoke(cli, 'utilities meta --help'.split())
    assert result.exit_code == 0
    print(result.output)
    expected_strings = """
  create        Create minimal study metadata from uploaded files
  pull          Retrieve all FHIR meta data from portal
  to_tabular    Convert FHIR to tabular format
  from_tabular  Convert tabular to FHIR format
  validate      Validate FHIR data
  push          Publish FHIR meta data on the portal
    """.split('\n')
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Should have printed {expected_string}"


def test_file(caplog):
    """Ensure it prints file"""
    runner = CliRunner()
    result = runner.invoke(cli, 'utilities files --help'.split())
    assert result.exit_code == 0
    print(result.output)
    expected_strings = """
  ls      List uploaded files in a project bucket.
  add     Add file to the index.
  status  List files in index.
  push    Upload index to project bucket.
  rm      Remove files from the index or project bucket.
    """.split('\n')
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Should have printed {expected_string}"


def test_config(caplog):
    """Ensure it prints file"""
    runner = CliRunner()
    result = runner.invoke(cli, 'utilities config --help'.split())
    assert result.exit_code == 0, result.output
    print(result.output)
    expected_strings = """
      ls  Show defaults.
    """.split()
    for expected_string in expected_strings:
        assert expected_string in result.output, f"Should have printed {expected_string}"
