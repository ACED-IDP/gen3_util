import click

from gen3_util.cli.common import NaturalOrderGroup
from gen3_util.config import Config
from gen3_util.projects.creator import touch
from gen3_util.projects.lister import ls
from gen3_util.projects.remover import rm


@click.group(name='projects', cls=NaturalOrderGroup)
@click.pass_obj
def project_group(config: Config):
    """Manage Gen3 projects."""
    pass


@project_group.command(name="ls")
@click.pass_obj
def project_ls(config: Config):
    """List all projects."""
    ls(config)


@project_group.command(name="touch")
@click.pass_obj
def project_touch(config: Config):
    """Create a project"""
    touch(config)


@project_group.command(name="rm")
@click.pass_obj
def project_rm(config: Config):
    """Remove project."""
    rm(config)
