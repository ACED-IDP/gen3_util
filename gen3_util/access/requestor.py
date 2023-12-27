import click

from gen3_util.access.requestor import ls, cat, update, LogAccess
from gen3_util.cli import CLIOutput
from gen3_util.cli import NaturalOrderGroup
from gen3_util.config import Config


@click.group(name='access', cls=NaturalOrderGroup)
@click.pass_obj
def access_group(config: Config):
    """Manage access requests."""
    pass


@access_group.command(name="sign")
@click.option('--username', required=False, help='Sign all requests for user within a project')
@click.pass_obj
def sign(config: Config, username: str):
    """Sign all policies for a project.
    \b
    """

    with CLIOutput(config=config) as output:
        access = ls(config, mine=False, username=username, active=True)
        unsigned_requests = [_ for _ in access.requests if _['status'] != 'SIGNED']

        if len(unsigned_requests) == 0:
            output.update(LogAccess(**{
                'msg': "No unsigned requests found"
            }))
        else:
            msg = "Signed requests"
            signed_requests = []
            for request in unsigned_requests:
                signed_requests.append(update(config, request_id=request['request_id'], status='SIGNED').request)
            output.update(LogAccess(**{
                'msg': msg,
                'requests': signed_requests,
            }))


@access_group.command(name="ls")
@click.option('--username', required=False, default=None, help='Sign all requests for user within a project')
@click.option('--mine',  is_flag=True, show_default=True, default=False, help="List current user's requests. Otherwise, list all the requests the current user has access to see.")
@click.option('--active', is_flag=True, show_default=True, default=False, help='Only unsigned requests')
@click.pass_obj
def access_ls(config: Config, mine: bool, active: bool, username: str):
    """List current user's requests."""
    with CLIOutput(config=config) as output:
        output.update(ls(config, mine, active, username))


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
