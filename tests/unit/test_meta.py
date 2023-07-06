import json

from click.testing import CliRunner

from gen3_util.cli.cli import cli


def test_import_from_directory():
    params = 'meta  import dir tests/fixtures/dir_to_study/ tmp/foo --project_id aced-foo'.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    print(result.output)
    assert result.exit_code == 0
    expected_strings = ['DocumentReference', "size: 6013814", 'ResearchStudy', "count: 1"]
    for expected_string in expected_strings:
        assert expected_string in result.output, f"{expected_string} not found in {result.output}"

    result = runner.invoke(cli, 'meta validate tmp/foo'.split())
    print(result.output)
    assert result.exit_code == 0
    expected_strings = ["msg: OK"]
    for expected_string in expected_strings:
        assert expected_string in result.output, f"{expected_string} not found in {result.output}"


def test_meta_plugin():
    """
    This test is to test the plugin functionality of the meta command.

    tests/fixtures/dir_to_study_with_meta/
    ├── file-2.csv
    ├── p1
    │   ├── s1
    │   │   └── file-3.pdf
    │   ├── s2
    │   │   └── file-4.tsv
    │   └── s3
    │       └── file-5
    └── p2
        └── s4
            └── file-1.txt

    Returns:

    """
    params = '--format json meta  import dir tests/fixtures/dir_to_study_with_meta/ tmp/foometa --project_id aced-foometa --plugin_path ./tests/unit/plugins'.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    print(result.output)
    assert result.exit_code == 0
    _ = json.loads(result.output)
    assert _['summary']['DocumentReference']['size'] == 6013814, "DocumentReference size is not 6013814"
    assert _['summary']['DocumentReference']['count'] == 5, "DocumentReference count is not 5"
    assert _['summary']['Specimen']['count'] == 4, "Specimen count is not 4"
    assert _['summary']['Patient']['count'] == 2, "Patient count is not 2"
