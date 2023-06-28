import click

from gen3_util.cli import CLIOutput
from gen3_util.cli import NaturalOrderGroup
from gen3_util.config import Config, ensure_auth
from gen3_util.projects.creator import touch
from gen3_util.projects.lister import ls
from gen3_util.projects.remover import rm


@click.group(name='projects', cls=NaturalOrderGroup)
@click.pass_obj
def project_group(config: Config):
    """Manage Gen3 projects."""
    pass


@project_group.command(name="ping")
@click.pass_obj
def ping(config: Config):
    """Test connectivity to Gen3 endpoint."""
    with CLIOutput(config=config) as output:
        auth = ensure_auth(config.gen3.refresh_file, validate=True)
        output.update({'endpoint': auth.endpoint})


@project_group.command(name="ls")
@click.pass_obj
def project_ls(config: Config):
    """List all projects user has access to."""
    with CLIOutput(config=config) as output:
        output.update(ls(config))


@project_group.command(name="touch")
@click.argument('project_id', required=False)
@click.option('--all/--no-all', '-a', 'all_', help='Create all configured projects', is_flag=True, default=False)
@click.pass_obj
def project_touch(config: Config, project_id: str, all_: bool):
    """Create a project
    PROJECT_ID: <program-name>-<project-name>
    """
    with CLIOutput(config=config) as output:
        output.update(touch(config, project_id, all_))


@project_group.command(name="rm")
@click.argument('project_id')
@click.pass_obj
def project_rm(config: Config, project_id: str):
    """Remove project.
    PROJECT_ID: <program-name>-<project-name>
    """
    with CLIOutput(config=config) as output:
        output.update(rm(config, project_id))
