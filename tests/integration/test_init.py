import json
import os
import pathlib
import uuid

from click.testing import CliRunner
from gen3_util.cli.cli import cli
from gen3_util.common import PROJECT_DIRECTORIES, PROJECT_DIR


def test_init(tmp_path, program, profile):
    os.chdir(tmp_path)
    runner = CliRunner()

    guid = str(uuid.uuid4())
    project_id = f'{program}-TEST_INCREMENTAL_{guid.replace("-", "_")}'

    result = runner.invoke(cli, ['--format', 'json', '--profile', profile, 'init', '--project_id', project_id])
    assert result.exit_code == 0, f"cmd failed with {result.output}"
    result_output = result.output
    print(result_output)
    print(tmp_path)
    for _ in PROJECT_DIRECTORIES:
        assert pathlib.Path(tmp_path, _).exists(), f"{_} not found in {tmp_path}"

    result = runner.invoke(cli, ['--format', 'json', 'config', 'ls'])
    assert result.exit_code == 0, f"cmd failed with {result.output}"
    _ = json.loads(result.output)
    print(_)
    assert _['config']['gen3']['project_id'] == project_id
    assert _['config']['gen3']['profile'] == profile
    assert _['config']['state_dir'] == str(pathlib.Path(PROJECT_DIR) / 'gen3_util')

    result = runner.invoke(cli, ['--format', 'json', 'access', 'ls', '--all'])
    assert result.exit_code == 0, f"cmd failed with {result.output}"
    _ = ', '.join([_['policy_id'] for _ in json.loads(result.output)['requests']])
    print(_)
    program, project = project_id.split('-')
    policy_id_prefix = f"programs.{program}.projects.{project}"
    assert policy_id_prefix in _, f"policy_id_prefix {policy_id_prefix} not found in requests"
