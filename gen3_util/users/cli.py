import click
from requests import HTTPError

from gen3_util.access.requestor import add_user, rm_user
from gen3_util.repo import CLIOutput, ENV_VARIABLE_PREFIX
from gen3_util.repo import NaturalOrderGroup
from gen3_util.config import Config


@click.group(name='users', cls=NaturalOrderGroup)
@click.pass_obj
def users_group(config: Config):
    """Manage project membership."""
    pass


@users_group.command(name="add")
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.option('--username', default=None, show_default=True,
              help="Email of user", required=True)
@click.option('--write/--no-write', '-w', help='Give user write privileges', is_flag=True, default=False, show_default=True)
@click.option('--delete/--no-delete', '-d', help='Give user delete privileges', is_flag=True, default=False, show_default=True)
@click.option('--verbose', default=False, required=False, is_flag=True, show_default=True, help="Show all output")
@click.pass_obj
def project_add_user(config: Config, username: str, project_id: str, write: bool, delete: bool, verbose: bool):
    """Add user to project."""
    with CLIOutput(config=config) as output:
        try:
            output.update(add_user(config, project_id, username, write, delete))
        except (AssertionError, ValueError, HTTPError, Exception) as e:
            output.update({'msg': str(e)})
            output.exit_code = 1
            if verbose:
                raise e


@users_group.command(name="rm")
@click.option('--project_id', default=None, show_default=True,
              help="Gen3 program-project", envvar=f"{ENV_VARIABLE_PREFIX}PROJECT_ID")
@click.option('--username', default=None, show_default=True,
              help="Email of user", required=True)
@click.option('--verbose', default=False, required=False, is_flag=True, show_default=True, help="Show all output")
@click.pass_obj
def project_rm_user(config: Config, username: str, project_id: str, verbose):
    """Remove user from project."""
    with CLIOutput(config=config) as output:
        try:
            output.update(rm_user(config, project_id, username))
        except (AssertionError, ValueError, HTTPError, Exception) as e:
            output.update({'msg': str(e)})
            output.exit_code = 1
            if verbose:
                raise e
