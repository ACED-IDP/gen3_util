import pathlib

import click

from gen3_util.cli import NaturalOrderGroup, CLIOutput
from gen3_util.config import Config
from gen3_util.meta.downloader import cp as cp_download
from gen3_util.meta.lister import ls
from gen3_util.meta.remover import rm
from gen3_util.meta.uploader import cp as cp_upload
from gen3_util.meta.validator import validate

from gen3_util.meta.importer import cli as importer_cli


@click.group(name='meta', cls=NaturalOrderGroup)
@click.pass_obj
def meta_group(config):
    """Manage meta data."""
    pass


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


@meta_group.group(name="import")
@click.pass_obj
def meta_import(config: Config):
    """Import study from directory listing."""
    pass


meta_import.add_command(importer_cli)


@meta_group.command(name="validate")
@click.argument('directory')
@click.pass_obj
def meta_validate(config: Config, directory):
    """Validate FHIR data in DIRECTORY."""
    with CLIOutput(config) as output:
        output.update(validate(config, directory))
