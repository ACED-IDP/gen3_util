import click

from gen3_util.access.requestor import ls, cat, touch, update
from gen3_util.cli import CLIOutput
from gen3_util.cli import NaturalOrderGroup
from gen3_util.config import Config


@click.group(name='access', cls=NaturalOrderGroup)
@click.pass_obj
def access_group(config: Config):
    """Manage access requests."""
    pass


@access_group.command(name="touch")
@click.argument('user_name')
@click.argument('resource_path')
@click.pass_obj
def access_touch(config: Config,  user_name: str, resource_path: str):
    """Create a request for read access.

    \b
    user_name (str): user's email
    resource_path (str): /programs/XXX/project/YYY

    """
    with CLIOutput(config=config) as output:
        output.update(touch(config, resource_path))


@access_group.command(name="update")
@click.argument('request_id')
@click.argument('status')
@click.pass_obj
def access_update(config: Config, request_id: str, status: str):
    """Update the request's approval workflow.

    \b
    request_id (str): uuid of an existing request
    status (str): new status see {ALLOWED_REQUEST_STATUSES}
    """
    with CLIOutput(config=config) as output:
        output.update(update(config, request_id, status))


@access_group.command(name="ls")
@click.option('--mine',  is_flag=True, show_default=True, default=False, help="List current user's requests. Otherwise, list all the requests the current user has access to see.")
@click.pass_obj
def access_ls(config: Config, mine: bool):
    """List current user's requests."""
    with CLIOutput(config=config) as output:
        output.update(ls(config, mine))


@access_group.command(name="cat")
@click.argument('request_id')
@click.pass_obj
def access_cat(config: Config, request_id: str):
    """Show details of a specific request.

    \b
    request_id (str): uuid of an existing request
    """
    with CLIOutput(config=config) as output:
        output.update(cat(config, request_id))
