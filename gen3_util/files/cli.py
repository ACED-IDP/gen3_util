import pathlib

import click

from gen3_util.cli import CLIOutput
from gen3_util.cli import NaturalOrderGroup
from gen3_util.config import Config
from gen3_util.files.downloader import cp as download
from gen3_util.files.lister import ls
from gen3_util.files.remover import rm
from gen3_util.files.uploader import cp as upload, put


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
              help="Gen3 program-project")
@click.option('--specimen_id', default=None, required=False, show_default=True,
              help="fhir specimen identifier")
@click.option('--patient_id', default=None, required=False, show_default=True,
              help="fhir patient identifier")
@click.option('--observation_id', default=None, required=False, show_default=True,
              help="fhir observation identifier")
@click.option('--task_id', default=None, required=False, show_default=True,
              help="fhir task identifier")
@click.option('--md5', default=None, required=False, show_default=True,
              help="file's md5")
def files_ls(config: Config, object_id: str, project_id: str, specimen_id: str, patient_id: str, observation_id: str, task_id: str, md5: str):
    """List files in a project."""
    with CLIOutput(config=config) as output:
        _ = {}
        if project_id:
            _['project_id'] = project_id
        if specimen_id:
            _['specimen_id'] = specimen_id
        if patient_id:
            _['patient_id'] = patient_id
        if task_id:
            _['task_id'] = task_id
        if observation_id:
            _['observation_id'] = observation_id
        if md5:
            _['md5'] = md5
        output.update(ls(config, object_id=object_id, metadata=_))


@file_group.command(name="cp")
@click.argument('from_', )
@click.argument('to_', )
@click.option('--worker_count', default=10, show_default=True,
              help="Number of worker processes")
@click.option('--ignore_state', default=False, is_flag=True, show_default=True,
              help="Upload file, even if already uploaded")
@click.option('--project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project")
@click.option('--source_path', required=False, default=None, show_default=True,
              help='Path on local file system')
# @click.option('--specimen_id', default=None, required=False, show_default=True,
#               help="fhir specimen identifier")
# @click.option('--patient_id', default=None, required=False, show_default=True,
#               help="fhir patient identifier")
# @click.option('--task_id', default=None, required=False, show_default=True,
#               help="fhir task identifier")
# @click.option('--observation_id', default=None, required=False, show_default=True,
#               help="fhir observation identifier")
@click.option('--disable_progress_bar', default=False, is_flag=True, show_default=True,
              help="Show progress bar")
@click.option('--duplicate_check', default=False, is_flag=True, show_default=True,
              help="Update metadata in indexd")
@click.pass_obj
def files_cp(config: Config, from_: str, to_: str, worker_count: int, ignore_state: bool, project_id: str,
             source_path: str, disable_progress_bar: bool, duplicate_check: bool):
    """Copy files to/from the project bucket.

    \b
    from_: Source url or path to DocumentReference.ndjson
    """

    with CLIOutput(config=config) as output:

        if pathlib.Path(from_).exists():
            _ = upload(config, from_, worker_count=worker_count, ignore_state=ignore_state, project_id=project_id,
                       source_path=source_path, disable_progress_bar=disable_progress_bar, duplicate_check=duplicate_check)
            output.update(_)
            if len(_.incomplete) > 0:
                raise ValueError("Not all transfers complete.")
        else:
            output.update(download(config, from_, to_))


@file_group.command(name="put")
@click.argument('from_', )
@click.option('--worker_count', default=10, show_default=True,
              help="Number of worker processes")
@click.option('--ignore_state', default=False, is_flag=True, show_default=True,
              help="Upload file, even if already uploaded")
@click.option('--project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project")
@click.option('--source_path', required=False, default=None, show_default=True,
              help='Path on local file system')
@click.option('--specimen_id', default=None, required=False, show_default=True,
              help="fhir specimen identifier")
@click.option('--patient_id', default=None, required=False, show_default=True,
              help="fhir patient identifier")
@click.option('--task_id', default=None, required=False, show_default=True,
              help="fhir task identifier")
@click.option('--observation_id', default=None, required=False, show_default=True,
              help="fhir observation identifier")
@click.option('--md5', default=None, required=False, show_default=True,
              help="MD5 sum, if not provided, will be calculated before upload")
@click.option('--disable_progress_bar', default=False, is_flag=True, show_default=True,
              help="Show progress bar")
@click.option('--duplicate_check', default=False, is_flag=True, show_default=True,
              help="Update metadata in indexd")
@click.pass_obj
def files_put(config: Config, from_: str, worker_count: int, ignore_state: bool, project_id: str,
              source_path: str, disable_progress_bar: bool, duplicate_check: bool,
              specimen_id: str, patient_id: str, observation_id: str, task_id: str, md5: str):
    """Copy one file to the project bucket.

    \b
    from_: path to file
    """

    with CLIOutput(config=config) as output:
        _ = put(config, from_, worker_count=worker_count, ignore_state=ignore_state, project_id=project_id,
                source_path=source_path, disable_progress_bar=disable_progress_bar, duplicate_check=duplicate_check,
                specimen_id=specimen_id, patient_id=patient_id, observation_id=observation_id, task_id=task_id,
                md5=md5
                )
        output.update(_)
        if len(_.incomplete) > 0:
            raise ValueError("Not all transfers complete.")


@file_group.command(name="rm")
@click.pass_obj
def files_rm(config: Config):
    """Remove files from a project."""
    rm(config)
