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

    params = '--format json meta  import dir tests/fixtures/dir_to_study_with_meta/ tmp/foometa --project_id aced-foometa --plugin_path ./tests/unit/plugins/gen3_util_plugin_foo'.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    assert result.exit_code == 0
    _ = json.loads(result.output)
    assert _['summary']['DocumentReference']['size'] == 6013814, "DocumentReference size is not 6013814"
    assert _['summary']['DocumentReference']['count'] == 5, "DocumentReference count is not 5"
    assert _['summary']['Specimen']['count'] == 4, "Specimen count is not 4"
    assert _['summary']['Patient']['count'] == 2, "Patient count is not 2"
    assert _['summary']['ResearchSubject']['count'] == 2, "ResearchSubject count is not 2"
    assert _['summary']['ResearchStudy']['count'] == 1, "ResearchStudy count is not 1"

    expected_document_subjects = {
        'ResearchStudy/fbeb248f-b2b1-5324-96f6-a04b7c0752e0': [
            'file:///tests/fixtures/dir_to_study_with_meta/file-2.csv',
            'file:///tests/fixtures/dir_to_study_with_meta/p1/s3/file-5',
            'file:///tests/fixtures/dir_to_study_with_meta/p1/s1/file-3.pdf'],
        'Patient/88f1c8f7-8f00-54b0-9133-6b32a2909bf7': [
            'file:///tests/fixtures/dir_to_study_with_meta/p2/s4/file-1.txt'],
        'Patient/3e2e9a5f-7f05-5411-8557-7414feafe54f': [
            'file:///tests/fixtures/dir_to_study_with_meta/p1/s2/file-4.tsv']
    }

    document_subjects = defaultdict(list)
    for line in open('tmp/foometa/DocumentReference.ndjson').readlines():
        _ = json.loads(line)
        document_subjects[_['subject']['reference']].append(_['content'][0]['attachment']['url'])

    assert expected_document_subjects == document_subjects, "DocumentReference subjects are not as expected"

    expected_specimen_subjects = ['Patient/3e2e9a5f-7f05-5411-8557-7414feafe54f', 'Patient/3e2e9a5f-7f05-5411-8557-7414feafe54f', 'Patient/3e2e9a5f-7f05-5411-8557-7414feafe54f', 'Patient/88f1c8f7-8f00-54b0-9133-6b32a2909bf7']
    specimen_subjects = []
    for line in open('tmp/foometa/Specimen.ndjson').readlines():
        _ = json.loads(line)
        specimen_subjects.append(_['subject']['reference'])
    assert expected_specimen_subjects == sorted(specimen_subjects), "Specimen subjects are not as expected"

    expected_research_subject_patients = set(['Patient/3e2e9a5f-7f05-5411-8557-7414feafe54f', 'Patient/88f1c8f7-8f00-54b0-9133-6b32a2909bf7'])
    expected_research_subject_studies = set(['ResearchStudy/fbeb248f-b2b1-5324-96f6-a04b7c0752e0'])
    research_subject_patients = set()
    research_subject_studies = set()
    for line in open('tmp/foometa/ResearchSubject.ndjson').readlines():
        _ = json.loads(line)
        research_subject_patients.add(_['subject']['reference'])
        research_subject_studies.add(_['study']['reference'])

    assert sorted(expected_research_subject_patients) == sorted(research_subject_patients), "research_subject_patients are not as expected"
    assert sorted(expected_research_subject_studies) == sorted(research_subject_studies), "research_subject_studies subjects are not as expected"


def test_meta_plugin_using_pkg_name():
    """Import using a plugin package name"""
    sys.path.append('tests/unit/plugins')
    params = '--format json meta  import dir tests/fixtures/dir_to_study_with_meta/ tmp/foometa --project_id aced-foometa --plugin_path gen3_util_plugin_foo'.split()
    runner = CliRunner()
    result = runner.invoke(cli, params)
    print(result.output)
    assert result.exit_code == 0
