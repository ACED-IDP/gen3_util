import click

from gen3_util.access.requestor import add_policies
from gen3_util.cli import CLIOutput
from gen3_util.cli import NaturalOrderGroup
from gen3_util.config import Config
from gen3_util.projects.lister import ls
from gen3_util.projects.remover import rm


@click.group(name='projects', cls=NaturalOrderGroup)
@click.pass_obj
def project_group(config: Config):
    """Manage Gen3 projects."""
    pass


@project_group.command(name="new")
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.pass_obj
def new_project(config: Config, project_id: str):
    """Creates project resource with default policies.
    """
    with CLIOutput(config=config) as output:
        output.update(add_policies(config, project_id))


@project_group.command(name="ls")
@click.pass_obj
def project_ls(config: Config):
    """List all projects user has access to."""
    with CLIOutput(config=config) as output:
        output.update(ls(config))


@project_group.command(name="rm")
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.pass_obj
def project_rm(config: Config, project_id: str):
    """Remove project.
    """
    with CLIOutput(config=config) as output:
        output.update(rm(config, project_id))
