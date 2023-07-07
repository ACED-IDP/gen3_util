import json
import sys
from collections import defaultdict

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

    expected_document_subjects = {
        'ResearchStudy/fbeb248f-b2b1-5324-96f6-a04b7c0752e0': [
            'file:///testsfixtures/dir_to_study_with_meta/file-2.csv'
        ],
        'Patient/88f1c8f7-8f00-54b0-9133-6b32a2909bf7': [
            'file:///testsfixtures/dir_to_study_with_meta/p2/s4/file-1.txt'
        ],
        'Patient/3e2e9a5f-7f05-5411-8557-7414feafe54f': [
          'file:///testsfixtures/dir_to_study_with_meta/p1/s2/file-4.tsv',
          'file:///testsfixtures/dir_to_study_with_meta/p1/s3/file-5',
          'file:///testsfixtures/dir_to_study_with_meta/p1/s1/file-3.pdf']
    }

    document_subjects = defaultdict(list)
    for line in open('tmp/foometa/DocumentReference.ndjson').readlines():
        _ = json.loads(line)
        document_subjects[_['subject']['reference']].append(_['content'][0]['attachment']['url'])

    print(document_subjects)
    assert expected_document_subjects == document_subjects, "DocumentReference subjects are not as expected"


def test_meta_plugin_using_pkg_name():
    """Import using a plugin package name"""
    sys.path.append('tests/unit/plugins')
    params = '--format json meta  import dir tests/fixtures/dir_to_study_with_meta/ tmp/foometa --project_id aced-foometa --plugin_path gen3_util_plugin_foo'.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    print(result.output)
    assert result.exit_code == 0
