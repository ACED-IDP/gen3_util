import click

from gen3_util.cli.common import NaturalOrderGroup
from gen3_util.config import Config
from gen3_util.files import _is_upload
from gen3_util.files.lister import ls
from gen3_util.files.remover import rm
from gen3_util.files.uploader import cp as upload
from gen3_util.files.downloader import cp as download


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
@click.argument('from')
@click.argument('to')
@click.pass_obj
def files_cp(config: Config, from_: str, to_: str):
    """Copy files to/from the project bucket.

    Args:
        FROM: Source url or path
        TO: Destination url or path
    """
    if _is_upload(from_, to_):
        upload(config, from_, to_)
    else:
        download(config, from_, to_)


@file_group.command(name="rm")
@click.pass_obj
def files_rm(config: Config):
    """Remove files from a project."""
    rm(config)
