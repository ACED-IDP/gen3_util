from click.testing import CliRunner

from gen3_util.cli.cli import cli


def test_import_from_directory():
    params = 'meta  import dir tests/fixtures/dir_to_study/ tmp/foo --project_id aced-foo'.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    print(result.output)
    assert result.exit_code == 0
    expected_strings = ['DocumentReference', "'size': 6013814", 'ResearchStudy', "'count': 1"]
    for expected_string in expected_strings:
        assert expected_string in result.output, f"{expected_string} not found in {result.output}"

    result = runner.invoke(cli, 'meta validate tmp/foo'.split())
    print(result.output)
    assert result.exit_code == 0
    expected_strings = ["'msg': 'OK'"]
    for expected_string in expected_strings:
        assert expected_string in result.output, f"{expected_string} not found in {result.output}"
