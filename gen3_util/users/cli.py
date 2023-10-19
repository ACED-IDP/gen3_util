import click

from gen3_util.access.requestor import add_user, rm_user
from gen3_util.cli import CLIOutput
from gen3_util.cli import NaturalOrderGroup
from gen3_util.config import Config


@click.group(name='users', cls=NaturalOrderGroup)
@click.pass_obj
def users_group(config: Config):
    """Manage users membership in projects."""
    pass


@users_group.command(name="add")
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.option('--username', default=None, show_default=True,
              help="Email of user", required=True)
@click.option('--write/--no-write', '-w', help='Give user write privileges', is_flag=True, default=False, show_default=True)
@click.pass_obj
def project_add_user(config: Config, username: str, project_id: str, write: bool):
    """Add user to project."""
    with CLIOutput(config=config) as output:
        output.update(add_user(config, project_id, username, write))


@users_group.command(name="rm")
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar='PROJECT_ID')
@click.option('--username', default=None, show_default=True,
              help="Email of user", required=True)
@click.pass_obj
def project_rm_user(config: Config, username: str, project_id: str):
    """Remove user from project."""
    with CLIOutput(config=config) as output:
        output.update(rm_user(config, project_id, username))
