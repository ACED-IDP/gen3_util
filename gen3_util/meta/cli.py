import click

from gen3_util.cli import NaturalOrderGroup, CLIOutput
from gen3_util.config import Config
from gen3_util.meta.lister import ls
from gen3_util.meta.remover import rm
from gen3_util.meta.uploader import cp
from gen3_util.meta.validator import validate

from gen3_util.meta.importer import cli as importer_cli


@click.group(name='meta', cls=NaturalOrderGroup)
@click.pass_obj
def meta_group(config):
    """Manage meta data."""
    pass


@meta_group.command(name="ls")
@click.pass_obj
def meta_ls(config: Config):
    """List meta in a project."""
    ls(config)


@meta_group.command(name="cp")
@click.pass_obj
def meta_cp(config: Config):
    """Copy meta to/from the project bucket."""
    cp(config)


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
