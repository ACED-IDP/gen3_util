import click

from gen3_util.cli.common import NaturalOrderGroup
from gen3_util.config import Config
from gen3_util.files.lister import ls
from gen3_util.files.remover import rm
from gen3_util.files.uploader import cp


@click.group(name='files', cls=NaturalOrderGroup)
@click.pass_obj
def file_group(config):
    """Manage file buckets."""
    pass


@file_group.command(name="ls")
@click.pass_obj
def files_ls(config: Config):
    """List files in a project."""
    ls(config)


@file_group.command(name="cp")
@click.pass_obj
def files_cp(config: Config):
    """Copy files to/from the project bucket."""
    cp(config)


@file_group.command(name="rm")
@click.pass_obj
def files_rm(config: Config):
    """Remove files from a project."""
    rm(config)
