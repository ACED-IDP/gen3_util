import os
import sys

import click

from gen3_util import gen3_client_profile
from gen3_util.cli import CLIOutput
from gen3_util.cli import NaturalOrderGroup
from gen3_util.config import Config, ensure_auth
from gen3_util.files.lister import ls
from gen3_util.files.remover import rm

from gen3_util.files.manifest import put as manifest_put, save as manifest_save, ls as manifest_ls, upload_indexd, \
    upload_files, rm as manifest_rm


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
@click.argument('local_path', )
@click.argument('remote_path', required=False, default=None)
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
def _manifest_put(config: Config, local_path: str, remote_path: str, project_id: str, md5: str,
                  specimen_id: str, patient_id: str, observation_id: str, task_id: str):
    """Add file meta information to the manifest.

    \b
    local_path: path to file on local file system
    remote_path: name of the file in bucket, defaults to local_path
    """

    with CLIOutput(config=config) as output:
        _ = manifest_put(config, local_path, project_id=project_id, md5=md5)
        _['observation_id'] = observation_id
        _['patient_id'] = patient_id
        _['specimen_id'] = specimen_id
        _['task_id'] = task_id
        _['remote_path'] = remote_path
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
              help="Gen3 program-project authorization", envvar='PROJECT_ID')
@click.option('--restricted_project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project, additional authorization", envvar='RESTRICTED_PROJECT_ID')
@click.option('--profile', show_default=True, help="gen3-client profile", envvar='GEN3_PROFILE')
@click.option('--upload-path', default='.', show_default=True, help="gen3-client upload path")
@click.option('--duplicate_check', default=False, is_flag=True, show_default=True, help="Update files records")
@click.option('--manifest_path', default=None, show_default=True, help="Provide your own manifest file.")
@click.pass_obj
def _manifest_upload(config: Config, project_id: str, profile: str, duplicate_check: bool, upload_path: str, manifest_path: str, restricted_project_id: str):
    """Upload to index and project bucket.  Uses local manifest, or manifest_path.

    """

    if not profile:
        auth = ensure_auth(config.gen3.refresh_file)
        profile = gen3_client_profile(endpoint=auth.endpoint)

    os.chdir(upload_path)

    with CLIOutput(config=config) as output:
        print("Updating file index...", file=sys.stderr)
        manifest_entries = upload_indexd(config, project_id=project_id, duplicate_check=duplicate_check, manifest_path=manifest_path, restricted_project_id=restricted_project_id)
        output.update(manifest_entries)
        completed_process = upload_files(config=config, project_id=project_id, manifest_entries=manifest_entries, profile=profile, upload_path=upload_path)
        assert completed_process.returncode == 0, f"upload_files failed with {completed_process.returncode}"


@manifest_group.command(name="export")
@click.option('--project_id', default=None, required=False, show_default=True,
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.option('--object_id', default=None, required=False, show_default=True, help="file UUID", envvar='OBJECT_ID')
@click.pass_obj
def manifest_export(config: Config, project_id: str, object_id: str):
    """Export the local manifest
    """
    with CLIOutput(config=config) as output:
        output.update(manifest_ls(config, project_id=project_id, object_id=object_id))


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
        _ = manifest_rm(config, project_id=project_id, object_id=object_id)
        output.update(_)


@file_group.command(name="rm")
@click.option('--object_id', default=None, required=True, show_default=True,
              help="file UUID", envvar='OBJECT_ID')
@click.pass_obj
def files_rm(config: Config, object_id: str):
    """Remove files from a project."""
    with CLIOutput(config=config) as output:
        output.update(rm(config, object_id))
