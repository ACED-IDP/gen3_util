import asyncio
import json
import os
import pathlib
import subprocess
from json import JSONDecodeError

import click
from gen3.jobs import Gen3Jobs

from gen3_util.cli import NaturalOrderGroup, CLIOutput
from gen3_util.common import unzip_collapse
from gen3_util.config import Config, ensure_auth
from gen3_util.meta.bundler import meta_to_bundle
from gen3_util.meta.delta import get as delta_get
from gen3_util.meta.downloader import cp as cp_download
from gen3_util.meta.importer import import_indexd
from gen3_util.meta.lister import ls
from gen3_util.meta.publisher import publish_meta_data
from gen3_util.meta.tabular import transform_dir_to_tabular, transform_dir_from_tabular
from gen3_util.meta.uploader import cp as cp_upload
from gen3_util.meta.validator import validate


@click.group(name='meta', cls=NaturalOrderGroup)
@click.pass_obj
def meta_group(config):
    """Manage meta data."""
    pass


meta_group.add_command(import_indexd)


@meta_group.command(name="pull")
@click.argument('meta_data_path')
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.option('--force', '-f', default=False, show_default=True, is_flag=True,
              help="Overwrite existing files")
@click.pass_obj
def _meta_pull(config: Config, meta_data_path: str,  project_id: str, force: bool):
    """Retrieve all FHIR meta data from portal

    \b
    meta_data_path: meta_data directory"""

    if not project_id:
        click.secho("--project_id is required", fg='red')
        exit(1)

    if not project_id.count('-') == 1:
        click.secho("--project_id must be of the form program-project", fg='red')
        exit(1)

    auth = ensure_auth(profile=config.gen3.profile)

    try:

        _ = meta_pull(auth, config, force, meta_data_path, project_id)

    except JSONDecodeError:
        print("jobs_client.async_run_job_and_wait() (raw):", _)


def meta_pull(auth, config, force, meta_data_path, project_id) -> dict:
    """Retrieve all FHIR meta data from portal"""
    # delivered to sower job in env['ACCESS_TOKEN']
    jobs_client = Gen3Jobs(auth_provider=auth)
    # delivered to sower job in env['INPUT_DATA']
    args = {'project_id': project_id, 'method': 'get'}
    _ = asyncio.run(jobs_client.async_run_job_and_wait('fhir_import_export', args))
    output = json.loads(_['output'])
    try:
        object_id = output['object_id']
        assert object_id, "object_id not found in output"
        zip_file_name = None
        for line in output['logs']:
            if not line.startswith('Uploaded'):
                continue
            zip_file_name = pathlib.Path(line.split()[1]).name

        cmd = f"gen3-client download-single --profile {config.gen3.profile} --guid {object_id} " \
              f"--download-path {meta_data_path}".split()

        if force:
            cmd.append('--no-prompt')

        download_results = subprocess.run(cmd)

        if download_results.returncode != 0:
            click.secho(f"gen3-client download-single failed {download_results}", fg='red')
            exit(1)

        output['logs'].append(f"Downloaded {object_id} to {meta_data_path}/{zip_file_name}")

        unzip_collapse(zip_file=f"{meta_data_path}/{zip_file_name}", extract_to=meta_data_path)
        output['logs'].append(f"Unzipped {meta_data_path}/{zip_file_name} to {meta_data_path}")

        os.remove(f"{meta_data_path}/{zip_file_name}")
        output['logs'].append(f"Removed {meta_data_path}/{zip_file_name}")

        if 'user' in output:
            del output['user']

    except Exception as e:
        output['logs'].append(f"Error: {str(e)}")

    return output


@meta_group.command(name="to_tabular")
@click.argument('meta_data_path')
@click.argument('tabular_data_path')
@click.option('--file_type', type=click.Choice(['tsv', 'xlsx']), default='tsv', help='Output file format: tsv or excel')
@click.pass_obj
def meta_to_tabular(config: Config, meta_data_path: str, tabular_data_path: str, file_type: str):
    """Convert FHIR to tabular format (experimental)

    \b
    meta_data_path: meta_data FHIR directory
    tabular_data_path: tabular data directory"""

    with CLIOutput(config=config) as console_output:
        try:
            msgs = []
            for _ in transform_dir_to_tabular(meta_data_path, tabular_data_path, file_type):
                msgs.append(_)
            console_output.update({'output_files': msgs})
        except Exception as e:
            console_output.update({'msg': f"Error: {e}"})
            console_output.exit_code = 1


@meta_group.command(name="from_tabular")
@click.argument('meta_data_path')
@click.argument('tabular_data_path')
@click.pass_obj
def meta_from_tabular(config: Config, meta_data_path: str, tabular_data_path: str):
    """Convert tabular to FHIR format (experimental)

    \b
    meta_data_path: meta_data FHIR directory
    tabular_data_path: tabular data directory"""

    with CLIOutput(config=config) as console_output:
        try:
            msgs = []
            for _ in transform_dir_from_tabular(meta_data_path, tabular_data_path):
                msgs.append(_)
            console_output.update({'output_files': msgs})
        except Exception as e:
            console_output.update({'msg': f"Error: {e}"})
            raise e
            console_output.exit_code = 1


@meta_group.command(name="validate")
@click.argument('directory')
@click.pass_obj
def meta_validate(config: Config, directory):
    """Validate FHIR data"""
    with CLIOutput(config) as output:
        output.update(validate(config, directory))


@meta_group.command(name="bundle")
@click.argument('directory', type=click.Path(exists=True))
@click.argument('gz_file_path', type=click.Path(file_okay=True, dir_okay=False, writable=True))
@click.pass_obj
def meta_bundle(config: Config, directory, gz_file_path):
    """Bundle FHIR data into zip file

    \b
    directory: meta_data directory
    output_file: zip file
    """
    with CLIOutput(config) as output:
        output.update(meta_to_bundle(config, config.gen3.project_id, pathlib.Path(directory), pathlib.Path(gz_file_path)))


@meta_group.command(name="push")
@click.argument('meta_data_path')
@click.option('--ignore_state', default=False, is_flag=True, show_default=True,
              help="Upload file, even if already uploaded")
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.pass_obj
def meta_push(config: Config, meta_data_path: str,  project_id: str, ignore_state: bool):
    """Publish FHIR meta data on the portal

    \b
    meta_data_path: meta_data directory"""

    _ = publish_meta_data(config, meta_data_path, ignore_state, project_id)
    try:
        output = json.loads(_['output'])

        with CLIOutput(config=config) as console_output:
            console_output.update(output)

    except JSONDecodeError:
        print("jobs_client.async_run_job_and_wait() (raw):", _)


# Hidden commands ............................................................
@meta_group.command(name="cp", hidden=True)
@click.argument('from_')
@click.argument('to_')
@click.option('--ignore_state', default=False, is_flag=True, show_default=True,
              help="Upload file, even if already uploaded")
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.pass_obj
def meta_cp(config: Config, from_: str, to_: str, project_id: str, ignore_state: bool):
    """Copy meta to/from the project bucket.

    \b
    from_: metadata directory
    to_: destination  bucket"""
    with CLIOutput(config=config) as output:
        if pathlib.Path(from_).is_dir():
            if not project_id:
                click.secho("--project_id is required for uploads", fg='red')
                exit(1)
            output.update(cp_upload(config, from_, project_id, ignore_state))
        else:
            pathlib.Path(to_).parent.mkdir(parents=True, exist_ok=True)
            output.update(cp_download(config, from_, to_))


@meta_group.command(name="ls", hidden=True)
@click.pass_obj
def meta_ls(config: Config):
    """Query buckets for submitted metadata."""
    with CLIOutput(config=config) as output:
        output.update(ls(config))


@meta_group.command(name="node", hidden=True)
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.option('--node_id', default=None, show_default=True,
              help="Gen3 node id")
@click.pass_obj
def delta(config: Config, project_id: str, node_id: str):
    """Retrieve simplified metadata for a node."""
    with CLIOutput(config) as output:
        output.update(delta_get(config, project_id, node_id))
