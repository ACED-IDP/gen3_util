import click

from gen3_util.access.requestor import add_policies, add_user
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
        output.update({'endpoint': auth.endpoint, 'username': auth.curl('/user/user').json()['username']})


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


@project_group.group(name="add")
def project_add():
    """Add policies and users to a project."""
    pass


@project_add.command(name="user")
@click.argument('project_id')
@click.argument('user_name')
@click.option('--write/--no-write', '-a', help='Give user write privileges', is_flag=True, default=False)
@click.pass_obj
def project_add_user(config: Config, user_name: str, project_id: str, write: bool):
    """Add user to project.
    PROJECT_ID: <program-name>-<project-name>
    USER_NAME: user's email
    """
    with CLIOutput(config=config) as output:
        output.update(add_user(config, project_id, user_name, write))


@project_add.command(name="resource")
@click.argument('project_id')
@click.pass_obj
def project_add_policies(config: Config, project_id: str):
    """Creates project resource with default policies.
    PROJECT_ID: <program-name>-<project-name>
    """
    with CLIOutput(config=config) as output:
        output.update(add_policies(config, project_id))
