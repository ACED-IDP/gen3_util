import json
import pathlib

import asyncio
import click
from gen3.jobs import Gen3Jobs

from gen3_util.cli import NaturalOrderGroup, CLIOutput
from gen3_util.config import Config, ensure_auth
from gen3_util.meta.downloader import cp as cp_download
from gen3_util.meta.lister import ls
from gen3_util.meta.remover import rm
from gen3_util.meta.uploader import cp as cp_upload
from gen3_util.meta.validator import validate

from gen3_util.meta.importer import import_indexd
from gen3_util.meta.delta import get as delta_get


@click.group(name='meta', cls=NaturalOrderGroup)
@click.pass_obj
def meta_group(config):
    """Manage meta data."""
    pass


@meta_group.command(name="publish")
@click.argument('from_')
@click.option('--ignore_state', default=False, is_flag=True, show_default=True,
              help="Upload file, even if already uploaded")
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project")
@click.pass_obj
def meta_publish(config: Config, from_: str,  project_id: str, ignore_state: bool):
    """Publish meta data on the portal

    \b
    from_: meta data directory"""

    msgs = []
    with CLIOutput(config=config) as output:
        assert pathlib.Path(from_).is_dir(), f"{from_} is not a directory"
        assert project_id is not None, "--project_id is required for uploads"
        upload_result = cp_upload(config, from_, project_id, ignore_state)
        msgs.append(upload_result['msg'])
        object_id = upload_result['object_id']

        auth = ensure_auth(config.gen3.refresh_file)
        jobs_client = Gen3Jobs(auth_provider=auth)
        args = {'object_id': object_id, 'project_id': project_id, 'method': 'put'}

        _ = asyncio.run(jobs_client.async_run_job_and_wait('fhir_import_export', args))
        _ = json.loads(_['output'])
        output.update(_)


@meta_group.command(name="cp")
@click.argument('from_')
@click.argument('to_')
@click.option('--ignore_state', default=False, is_flag=True, show_default=True,
              help="Upload file, even if already uploaded")
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project")
@click.pass_obj
def meta_cp(config: Config, from_: str, to_: str, project_id: str, ignore_state: bool):
    """Copy meta to/from the project bucket.

    \b
    from_: meta data directory
    to_: destination  bucket"""
    with CLIOutput(config=config) as output:
        if pathlib.Path(from_).is_dir():
            assert project_id is not None, "--project_id is required for uploads"
            output.update(cp_upload(config, from_, to_, project_id, ignore_state))
        else:
            pathlib.Path(to_).parent.mkdir(parents=True, exist_ok=True)
            output.update(cp_download(config, from_, to_))


@meta_group.command(name="ls")
@click.pass_obj
def meta_ls(config: Config):
    """Query buckets for submitted metadata."""
    with CLIOutput(config=config) as output:
        output.update(ls(config))


@meta_group.command(name="rm")
@click.pass_obj
def meta_rm(config: Config):
    """Remove meta from a project."""
    rm(config)


@meta_group.group(name="create")
@click.pass_obj
def meta_create(config: Config):
    """Create meta data from imported files"""
    pass


meta_create.add_command(import_indexd)


@meta_group.command(name="validate")
@click.argument('directory')
@click.pass_obj
def meta_validate(config: Config, directory):
    """Validate FHIR data in DIRECTORY."""
    with CLIOutput(config) as output:
        output.update(validate(config, directory))


@meta_group.command(name="node")
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project")
@click.option('--node_id', default=None, show_default=True,
              help="Gen3 node id")
@click.pass_obj
def delta(config: Config, project_id: str, node_id: str):
    """Retrieve simplified metadata for a node."""
    with CLIOutput(config) as output:
        output.update(delta_get(config, project_id, node_id))
