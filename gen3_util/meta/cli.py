import click

from gen3_util.cli.common import NaturalOrderGroup
from gen3_util.config import Config
from gen3_util.meta.lister import ls
from gen3_util.meta.remover import rm
from gen3_util.meta.uploader import cp
from gen3_util.meta.validator import validate


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


@meta_group.command(name="validate")
@click.pass_obj
def meta_validate(config: Config):
    """Validate meta data."""
    validate(config)


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
