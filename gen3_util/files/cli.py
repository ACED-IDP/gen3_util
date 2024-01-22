import json
import os
import sys
from json import JSONDecodeError
from pathlib import Path

import click
from requests import HTTPError

from gen3_util.repo import CLIOutput, ENV_VARIABLE_PREFIX
from gen3_util.repo import NaturalOrderGroup
from gen3_util.common import PROJECT_DIR
from gen3_util.config import Config
from gen3_util.files.lister import ls
from gen3_util.files.manifest import put as manifest_put, save as manifest_save, ls as manifest_ls, upload_indexd, \
    upload_files, rm as manifest_rm
from gen3_util.files.remover import rm
from gen3_util.meta.publisher import publish_meta_data
from gen3_util.meta.skeleton import study_metadata


@click.group(name='files', cls=NaturalOrderGroup)
@click.pass_obj
def file_group(config):
    """Manage file transfers."""
    pass


@file_group.command(name="ls")
@click.pass_obj
@click.option('--object_id', default=None, required=False, show_default=True,
              help="id of the object in the indexd database")
@click.option('--project_id', default=None, required=None, show_default=True,
              help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.option('--specimen', default=None, required=False, show_default=True,
              help="fhir specimen identifier", envvar='SPECIMEN_ID')
@click.option('--patient', default=None, required=False, show_default=True,
              help="fhir patient identifier", envvar='PATIENT_ID')
@click.option('--observation', default=None, required=False, show_default=True,
              help="fhir observation identifier", envvar='OBSERVATION_ID')
@click.option('--task', default=None, required=False, show_default=True,
              help="fhir task identifier", envvar='TASK_ID')
@click.option('--is_metadata', default=False, is_flag=True, required=False, show_default=True,
              help="Meta data",)
@click.option('--is_snapshot', default=False, is_flag=True, required=False, show_default=True,
              help="Meta data",)
@click.option('--md5', default=None, required=False, show_default=True,
              help="file's md5")
@click.option('-l', '--long', default=False, required=False, show_default=True, is_flag=True,
              help="long format")
def files_ls(config: Config, object_id: str, project_id: str, specimen: str, patient: str, observation: str, task: str, md5: str, is_metadata: bool, is_snapshot: bool, long: bool):
    """List uploaded files in a project bucket."""
    if not project_id:
        project_id = config.gen3.project_id
    with CLIOutput(config=config) as output:
        try:
            _ = {}
            if project_id:
                _['project_id'] = project_id
            if specimen:
                _['specimen_id'] = specimen
            if patient:
                _['patient_id'] = patient
            if task:
                _['task_id'] = task
            if observation:
                _['observation_id'] = observation
            if md5:
                _['md5'] = md5
            if is_metadata:
                _['is_metadata'] = is_metadata
            if is_snapshot:
                _['is_snapshot'] = is_snapshot
            results = ls(config, object_id=object_id, metadata=_)
            if not long:
                results = [_['file_name'] for _ in results['records']]
            output.update(results)
        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1


@file_group.command(name="add")
@click.argument('local_path', type=click.Path(exists=True, dir_okay=False))
# @click.argument('remote_path', required=False, default=None)
@click.option('--project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID", hidden=True)
# @click.option('--source_path', required=False, default=None, show_default=True,
#               help='Path on local file system')
@click.option('--specimen_id', default=None, required=False, show_default=True,
              help="fhir specimen identifier", envvar='SPECIMEN_ID')
@click.option('--patient_id', default=None, required=False, show_default=True,
              help="fhir patient identifier", envvar='PATIENT_ID')
@click.option('--task_id', default=None, required=False, show_default=True,
              help="fhir task identifier", envvar='TASK_ID')
@click.option('--observation_id', default=None, required=False, show_default=True,
              help="fhir observation identifier", envvar='OBSERVATION_ID')
@click.option('--md5', default=None, required=False, show_default=True,
              help="MD5 sum, if not provided, will be calculated before upload")
@click.pass_obj
def manifest_put_cli(config: Config, local_path: str, project_id: str, md5: str,
                     specimen_id: str, patient_id: str, observation_id: str, task_id: str):
    """Add file to the index.

    \b
    local_path: relative path to file or symbolic link on the local file system
    """
    # TODO deprecate `remote_path` insist on relative paths
    with CLIOutput(config=config) as output:
        try:
            assert Path(PROJECT_DIR).exists(), "Please add files from the project root directory."
            assert Path(local_path).absolute().is_relative_to(Path.cwd().absolute()), \
                f"{local_path} must be relative to the project root, please move the file or create a symbolic link"

            if not project_id:
                project_id = config.gen3.project_id
            _ = manifest_put(config, local_path, project_id=project_id, md5=md5)
            _['observation_id'] = observation_id
            _['patient_id'] = patient_id
            _['specimen_id'] = specimen_id
            _['task_id'] = task_id
            _['remote_path'] = None
            output.update(_)
            manifest_save(config, project_id, [_])
        except Exception as e:
            output.exit_code = 1
            output.update({'msg': str(e)})


@file_group.command(name="status")
@click.option('--project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.option('--object_id', default=None, required=False, show_default=True,
              help="file id")
@click.pass_obj
def _manifest_ls(config: Config, project_id: str, object_id: str):
    """List files in index.
    """

    with CLIOutput(config=config) as output:
        _ = manifest_ls(config, project_id=project_id, object_id=object_id)
        output.update(_)


@file_group.command(name="push")
@click.option('--project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project authorization", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.option('--restricted_project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project, additional authorization", envvar='RESTRICTED_PROJECT_ID')
@click.option('--upload-path', default='.', show_default=True, help="gen3-client upload path")
@click.option('--duplicate_check', default=False, is_flag=True, show_default=True, help="Update files records")
@click.option('--manifest_path', default=None, show_default=True, help="Provide your own manifest file.")
@click.option('--no_meta_data', 'meta_data', default=True, is_flag=True, show_default=True, help="Generate and submit metadata.")
@click.option('--wait', default=False, is_flag=True, show_default=True, help="Wait for metadata completion.")
@click.pass_obj
def _manifest_upload(config: Config, project_id: str, duplicate_check: bool, upload_path: str, manifest_path: str, restricted_project_id: str, meta_data: bool, wait: bool):
    """Upload index to project bucket.

    """

    os.chdir(upload_path)

    with CLIOutput(config=config) as output:
        print("Updating file index...", file=sys.stderr)
        try:
            manifest_entries = upload_indexd(config, project_id=project_id, duplicate_check=duplicate_check, manifest_path=manifest_path, restricted_project_id=restricted_project_id)
            output.update({'manifest_entries': manifest_entries})
        except (AssertionError, HTTPError) as e:
            print(f"upload_indexd failed with {e}", file=sys.stderr)
            raise e

        completed_process = upload_files(config=config, project_id=project_id, manifest_entries=manifest_entries, profile=config.gen3.profile, upload_path=upload_path, overwrite_files=False)
        if completed_process.returncode != 0:
            click.secho(f"upload_files failed with {completed_process.returncode}", fg='red')
            exit(1)

        if meta_data:
            print("Updating metadata...", file=sys.stderr)
            meta_data_path = config.state_dir / f"{project_id}-meta_data"
            new_record_count = study_metadata(config=config, overwrite=duplicate_check, project_id=project_id, source='manifest', output_path=meta_data_path)
            if new_record_count > 0:
                _ = publish_meta_data(config, str(meta_data_path), ignore_state=duplicate_check, project_id=project_id, wait=wait)
                try:
                    _ = json.loads(_['output'])
                    output.update({'job': {'publish_meta_data': _}})
                    print(f"Meta data update underway, check status with: gen3_util jobs get {_['uid']}", file=sys.stderr)
                except JSONDecodeError:
                    print("Error publishing metadata:", _)


@file_group.command(name="rm")
@click.option('--local', default=False, required=False, show_default=True, is_flag=True,
              help="Remove file only from local project.")
@click.option('--project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.option('--object_id', default=None, required=False, show_default=True,
              help="file UUID", envvar='OBJECT_ID')
@click.option('--local_path', default=None, required=False, show_default=True,
              help="file local path")
@click.pass_obj
def files_rm(config: Config, object_id: str, local: bool, project_id: str, local_path: str):
    """Remove files from the index or project bucket."""
    with CLIOutput(config=config) as output:
        try:
            if not local:
                _ = rm(config, object_id=object_id)
                output.update(_)
            else:
                _ = manifest_rm(config, project_id=project_id, object_id=object_id, file_name=local_path)
                output.update(_)
        except HTTPError as e:
            msg = str(e)
            if 'not found' in msg.lower():
                msg = f"object_id {object_id} not found. {msg}"
            output.exit_code = 1
            output.update({'msg': msg})


# Hidden commands ............................................................
@file_group.command(name="export", hidden=True)
@click.option('--project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.option('--object_id', default=None, required=False, show_default=True, help="file UUID", envvar='OBJECT_ID')
@click.pass_obj
def manifest_export(config: Config, project_id: str, object_id: str):
    """Export the local manifest
    """
    with CLIOutput(config=config) as output:
        output.update(manifest_ls(config, project_id=project_id, object_id=object_id))
