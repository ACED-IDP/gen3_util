import json
import os
import pathlib
import string
import uuid
import random
from time import sleep

from click.testing import CliRunner
from fhir.resources.bundle import Bundle, BundleEntry

import gen3_util.files.manifest
from gen3_util.common import create_id, read_ndjson_file
from gen3_util.config import init
from gen3_util.meta import directory_reader
from gen3_util.meta.skeleton import study_metadata

from nested_lookup import nested_lookup
from gen3_util.repo.cli import cli


def test_init_project(config, program, tmp_path, profile):
    """Test commit bundle."""

    # navigate to tmp_path
    os.chdir(tmp_path)

    # create project
    guid = str(uuid.uuid4())
    project = f'TEST_COMMIT_{guid.replace("-", "_")}'
    project_id = f'{program}-{project}'

    runner = CliRunner()

    result = runner.invoke(cli, f'--format json --profile {profile} init {project_id}'.split())
    assert result.exit_code == 0, result.output

    result = runner.invoke(cli, f'--format json --profile {profile} utilities access sign'.split())
    assert result.exit_code == 0, result.output

    result = runner.invoke(cli, f"--format json --profile {profile} utilities projects create /programs/{program}/projects/{project}".split())
    assert result.exit_code == 0, result.output

    # add a file, associated with a patient
    file_size = 1024
    local_path = pathlib.Path(tmp_path) / 'some_random_file.txt'
    local_path = local_path.relative_to(tmp_path)
    with open(local_path, 'w') as fp:
        fp.write(''.join(random.choices(string.ascii_letters, k=file_size)))

    result = runner.invoke(cli, f"--format json --profile {profile} add {str(local_path)} --patient P1".split())
    assert result.exit_code == 0, result.output

    # generate FHIR
    result = runner.invoke(cli, f"--format json --profile {profile} utilities meta create".split())
    assert result.exit_code == 0, result.output

    metadata_path = pathlib.Path(tmp_path) / 'META'
    metadata_ls = sorted(str(_.relative_to(tmp_path)) for _ in metadata_path.glob('**/*.*'))
    expected_metadata_ls = sorted(['META/.gitignore', 'META/README.md',  'META/DocumentReference.ndjson', 'META/Patient.ndjson', 'META/ResearchStudy.ndjson', 'META/ResearchSubject.ndjson'])
    assert metadata_ls == expected_metadata_ls, f"expected metadata files not found {metadata_ls}"

    result = runner.invoke(cli, f'--format json  --profile {profile} commit -m "test-commit"'.split())
    assert result.exit_code == 0, result.output

    result = runner.invoke(cli, f'--format json  --profile {profile} push'.split())
    assert result.exit_code == 0, result.output

    push = None
    for line in read_ndjson_file(pathlib.Path('.g3t/state') / project_id / 'commits' / 'completed.ndjson'):
        push = line

    published_job_uid = push['published_job']['output']['uid']
    print("published_job_uid", published_job_uid)

    sleep(5)
    while True:
        result = runner.invoke(cli, f'--format json  --profile {profile} utilities jobs get {published_job_uid}'.split())
        assert result.exit_code == 0, result.output
        try:
            output = json.loads(result.output)
            if output['status'] not in ['Completed', 'Unknown']:
                break
            sleep(5)
        except json.JSONDecodeError:
            print(f"Unable to decode {result.output}")
            break

    result = runner.invoke(cli, f'--format json  --profile {profile} push --re-run'.split())
    assert result.exit_code == 0, result.output

    push = None
    for line in read_ndjson_file(pathlib.Path('.g3t/state') / project_id / 'commits' / 'completed.ndjson'):
        push = line

    re_run_published_job_uid = push['published_job']['output']['uid']
    assert re_run_published_job_uid != published_job_uid, "re-run job should have a different uid"

    sleep(5)
    while True:
        result = runner.invoke(cli, f'--format json  --profile {profile} utilities jobs get {re_run_published_job_uid}'.split())
        assert result.exit_code == 0, result.output
        output = json.loads(result.output)
        if output['status'] not in ['Completed', 'Unknown']:
            break
        sleep(5)


def test_bundle(config, program, tmp_path):
    """Serialize and validate a bundle."""

    # navigate to tmp_path
    os.chdir(tmp_path)

    # create project
    guid = str(uuid.uuid4())
    project_id = f'{program}-TEST_COMMIT_{guid.replace("-", "_")}'
    config.gen3.project_id = project_id
    for _ in init(config, project_id):
        print(_)

    # add a file, associated with a patient
    file_size = 1024
    local_path = pathlib.Path(tmp_path) / 'some_random_file.txt'
    local_path = local_path.relative_to(tmp_path)
    with open(local_path, 'w') as fp:
        fp.write(''.join(random.choices(string.ascii_letters, k=file_size)))

    _ = gen3_util.files.manifest.put(config, str(local_path), project_id=project_id, md5=None)
    _['patient_id'] = "P1"
    gen3_util.files.manifest.save(config, project_id, [_])

    # generate FHIR
    metadata_path = pathlib.Path(tmp_path) / 'META'
    study_metadata(config=config, project_id=project_id, output_path=metadata_path,
                   overwrite=False, source='manifest')
    metadata_ls = sorted(str(_.relative_to(tmp_path)) for _ in metadata_path.glob('**/*.*'))
    expected_metadata_ls = sorted(['META/.gitignore', 'META/README.md',  'META/DocumentReference.ndjson', 'META/Patient.ndjson', 'META/ResearchStudy.ndjson', 'META/ResearchSubject.ndjson'])
    assert metadata_ls == expected_metadata_ls, f"expected metadata files not found {metadata_ls}"

    # create a bundle
    # see https://github.com/ACED-IDP/submission/issues/9
    bundle = Bundle(
        type='transaction',
        entry=[],
        identifier={"system": "https://aced-idp.org/project_id", "value": project_id}
    )

    # add resources to bundle
    references = []
    ids = []
    for parse_result in directory_reader(metadata_path, validate=True):
        _ = parse_result.resource
        ids.append(f"{_.resource_type}/{_.id}")
        assert _.identifier and len(_.identifier) > 0, f"{_.resource_type} does not have an 'identifier'"
        references.extend(nested_lookup('reference', parse_result.json_obj))
        entry = BundleEntry(
            resource=parse_result.resource,
            request={'method': 'PUT', 'url': parse_result.resource.resource_type}
        )
        bundle.entry.append(entry)

    # validate bundle
    check_local_path = True
    for entry in bundle.entry:
        # first check files exist and are in manifest
        if entry.resource.resource_type == 'DocumentReference':
            # check that file is on local system, TODO should we check against manifest?
            assert entry.resource.content[0].attachment.size == file_size, "size not set"
            if check_local_path:
                _ = entry.resource.content[0].attachment.url
                _ = _.replace('file:///', '')
                assert _ == str(local_path), "url not set"
                assert pathlib.Path(_).exists(), f"file not found {_}"
        # check ids are set and derive from identifier
        # document reference is an exception, since it is set by the manifest
        if entry.resource.resource_type != 'DocumentReference':
            assert entry.resource.id == create_id(entry.resource, project_id=project_id), \
                f"id must be derived from official identifier {entry.resource.resource_type}"

    # assert references exist
    references = set(references)
    ids = set(ids)
    print('ids', ids)
    print('references', references)
    assert references.issubset(ids), f"references not found {references - ids}"
