import os

import click

from gen3_util.cli import CLIOutput
from gen3_util.cli import NaturalOrderGroup
from gen3_util.config import Config
from gen3_util.files.lister import ls
from gen3_util.files.remover import rm

from gen3_util.files.manifest import put as manifest_put, save as manifest_save, ls as manifest_ls, upload_indexd, \
    upload_files


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
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.option('--specimen_id', default=None, required=False, show_default=True,
              help="fhir specimen identifier", envvar='SPECIMEN_ID')
@click.option('--patient_id', default=None, required=False, show_default=True,
              help="fhir patient identifier", envvar='PATIENT_ID')
@click.option('--observation_id', default=None, required=False, show_default=True,
              help="fhir observation identifier", envvar='OBSERVATION_ID')
@click.option('--task_id', default=None, required=False, show_default=True,
              help="fhir task identifier", envvar='TASK_ID')
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


@file_group.group(name='manifest', cls=NaturalOrderGroup)
@click.pass_obj
def manifest_group(config):
    """Manage file transfers using a manifest."""
    pass


@manifest_group.command(name="put")
@click.argument('file_name', )
@click.option('--project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project", envvar='PROJECT_ID')
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
def _manifest_put(config: Config, file_name: str, project_id: str, md5: str,
                  specimen_id: str, patient_id: str, observation_id: str, task_id: str):
    """Add file meta information to the manifest.

    \b
    file_name: path to file
    """

    with CLIOutput(config=config) as output:
        _ = manifest_put(config, file_name, project_id=project_id, md5=md5)
        _['observation_id'] = observation_id
        _['patient_id'] = patient_id
        _['specimen_id'] = specimen_id
        _['task_id'] = task_id
        output.update(_)
        manifest_save(config, project_id, [_])


@manifest_group.command(name="ls")
@click.option('--project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.option('--object_id', default=None, required=False, show_default=True,
              help="file id")
@click.pass_obj
def _manifest_ls(config: Config, project_id: str, object_id: str):
    """Read current manifest.
    """

    with CLIOutput(config=config) as output:
        _ = manifest_ls(config, project_id=project_id, object_id=object_id)
        output.update(_)


@manifest_group.command(name="upload")
@click.option('--project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.option('--profile', show_default=True, help="gen3-client profile", envvar='PROFILE')
@click.option('--upload-path', default='.', show_default=True, help="gen3-client upload path")
@click.option('--duplicate_check', default=False, is_flag=True, show_default=True, help="Update files records")
@click.pass_obj
def _manifest_upload(config: Config, project_id: str, profile: str, duplicate_check: bool, upload_path: str):
    """Add manifest to the index and upload files to the project bucket.
    """

    assert profile, "Please provide a profile for gen3-client"
    os.chdir(upload_path)

    with CLIOutput(config=config) as output:
        manifest_entries = upload_indexd(config, project_id=project_id, duplicate_check=duplicate_check)
        output.update(manifest_entries)
        completed_process = upload_files(config=config, project_id=project_id, manifest_entries=manifest_entries, profile=profile, upload_path=upload_path)
        assert completed_process.returncode == 0, f"upload_files failed with {completed_process.returncode}"


@manifest_group.command(name="rm")
@click.option('--project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.option('--object_id', default=None, required=False, show_default=True,
              help="file UUID", envvar='OBJECT_ID')
@click.pass_obj
def _manifest_rm(config: Config, project_id: str, object_id: str):
    """Remove file(s) from local manifest.
    """

    with CLIOutput(config=config) as output:
        # _ = manifest_rm(config, project_id=project_id, object_id=object_id)
        output.update({'msg': 'not implemented'})  # TODO implement


@file_group.command(name="rm")
@click.pass_obj
def files_rm(config: Config):
    """Remove files from a project."""
    rm(config)
