import json
import os
import sys
from datetime import datetime
from json import JSONDecodeError
from pathlib import Path

import click
from requests import HTTPError

from gen3_util.repo import CLIOutput, ENV_VARIABLE_PREFIX
from gen3_util.repo import NaturalOrderGroup
from gen3_util.common import PROJECT_DIR
from gen3_util.config import Config
from gen3_util.files.middleware import files_ls_driver
from gen3_util.files.manifest import put as manifest_put, save as manifest_save, ls as manifest_ls, upload_indexd, \
    upload_files, rm as manifest_rm
from gen3_util.files.remover import rm
from gen3_util.meta.publisher import publish_meta_data
from gen3_util.meta.skeleton import study_metadata
from urllib.parse import urlparse


@click.group(name='files', cls=NaturalOrderGroup)
@click.pass_obj
def file_group(config):
    """Manage file transfers."""
    pass


@file_group.command(name="ls")
@click.pass_obj
@click.option('--object_id', default=None, required=False, show_default=True,
              help="id of the object in the indexd database")
@click.option('--project_id', default=None, required=False, show_default=True,
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
    files_ls_driver(config, object_id, project_id, specimen, patient, observation, task, md5, is_metadata, is_snapshot, long)


@file_group.command(name="add")
@click.argument('path')
# @click.argument('remote_path', required=False, default=None)
@click.option('--project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID", hidden=True)
@click.option('--specimen', default=None, required=False, show_default=True,
              help="fhir specimen identifier", envvar=f'{ENV_VARIABLE_PREFIX}SPECIMEN')
@click.option('--patient', default=None, required=False, show_default=True,
              help="fhir patient identifier", envvar=f'{ENV_VARIABLE_PREFIX}PATIENT')
@click.option('--task', default=None, required=False, show_default=True,
              help="fhir task identifier", envvar=f'{ENV_VARIABLE_PREFIX}TASK')
@click.option('--observation', default=None, required=False, show_default=True,
              help="fhir observation identifier", envvar=f'{ENV_VARIABLE_PREFIX}OBSERVATION')
@click.option('--md5', default=None, required=False, show_default=True,

              help="MD5 sum, if not provided, will be calculated before upload, required for non-local files")
@click.option('--size', default=None, required=False, show_default=True, type=int,
              help="file size in bytes, required for non-local files")
@click.option('--no-bucket', 'no_bucket', default=False, required=False, show_default=True, is_flag=True,
              envvar=f'{ENV_VARIABLE_PREFIX}NO_BUCKET',
              help="Do not upload to bucket, only add to index. (Uses symlink or scp to download).")
@click.option('--modified', default=None, required=False, show_default=True,
              help="Modified datetime of the file, required for non-local files")
@click.option('--verbose', default=False, required=False, is_flag=True, show_default=True, help="Show all output")
@click.pass_obj
def manifest_put_cli(config: Config, path: str, project_id: str, md5: str,
                     specimen: str, patient: str, observation: str, task: str, no_bucket: bool, size: int, modified: datetime, verbose: bool):
    """Add file to the index.

    \b
    local_path: relative path to file or symbolic link on the local file system
    """
    with CLIOutput(config=config) as output:
        try:
            _parsed = urlparse(path, allow_fragments=False)
            if _parsed.scheme != '' and _parsed.scheme in ['s3', 'gs', 'abfs']:
                local_path = _parsed.path.lstrip('/')
                url = path
            elif _parsed.scheme != '':
                raise ValueError(f"Unsupported url scheme {_parsed.scheme}")
            elif _parsed.scheme == '' and Path(path).exists():
                local_path = path
                url = None
                assert Path(PROJECT_DIR).exists(), "Please add files from the project root directory."
                assert Path(local_path).absolute().is_relative_to(Path.cwd().absolute()), \
                    f"{local_path} must be relative to the project root, please move the file or create a symbolic link"
            if url:
                assert all([size, md5, modified]), "size, md5, and modified are required for bucket objects"

            if not project_id:
                project_id = config.gen3.project_id

            if modified:
                try:
                    modified = datetime.fromisoformat(modified)
                except ValueError:
                    modified = click.DateTime()(modified)

            _ = manifest_put(config, local_path, project_id=project_id, md5=md5, size=size, modified=modified)

            _['observation_id'] = observation
            _['patient_id'] = patient
            _['specimen_id'] = specimen
            _['task_id'] = task

            _['remote_path'] = None
            _['no_bucket'] = no_bucket
            _['url'] = url
            output.update(_)
            assert manifest_save(config, project_id, [_])
        except (AssertionError, Exception) as e:
            output.exit_code = 1
            output.update({'msg': str(e)})
            if verbose:
                raise e


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
@click.option('--overwrite', default=False, is_flag=True, show_default=True, help="Update files records")
@click.option('--manifest_path', default=None, show_default=True, help="Provide your own manifest file.")
@click.option('--no_meta_data', 'meta_data',
              default=True, is_flag=True, show_default=True, help="Generate and submit metadata.")
@click.option('--wait', default=False, is_flag=True, show_default=True, help="Wait for metadata completion.")
@click.pass_obj
def _manifest_upload(config: Config, project_id: str, overwrite: bool, upload_path: str, manifest_path: str,
                     restricted_project_id: str, meta_data: bool, wait: bool):
    """Upload index to project bucket.

    """

    os.chdir(upload_path)
    with CLIOutput(config=config) as output:
        click.echo("Updating file index...", file=sys.stdout)
        try:
            manifest_entries = upload_indexd(
                config=config,
                project_id=project_id,
                duplicate_check=overwrite,
                manifest_path=manifest_path,
                restricted_project_id=restricted_project_id
            )
            output.update({'manifest_entries': manifest_entries})
        except (AssertionError, HTTPError) as e:
            click.echo(f"upload_indexd failed with {e}", file=sys.stderr)
            raise e

        completed_process = upload_files(config=config, project_id=project_id, manifest_entries=manifest_entries, profile=config.gen3.profile, upload_path=upload_path, overwrite_files=False)
        if completed_process.returncode != 0:
            click.secho(f"upload_files failed with {completed_process.returncode}", fg='red')
            exit(1)

        if meta_data:
            click.echo("Updating metadata...", file=sys.stdout)
            meta_data_path = config.state_dir / f"{project_id}-meta_data"
            new_record_count = study_metadata(config=config, overwrite=overwrite, project_id=project_id, source='manifest', output_path=meta_data_path)
            if new_record_count > 0:
                _ = publish_meta_data(config, str(meta_data_path), ignore_state=overwrite, project_id=project_id, wait=wait)
                try:
                    _ = json.loads(_['output'])
                    output.update({'job': {'publish_meta_data': _}})
                    click.echo(f"Meta data update underway, check status with: gen3_util jobs get {_['uid']}", file=sys.stdout)
                except JSONDecodeError:
                    click.echo("Error publishing metadata:", _)


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
