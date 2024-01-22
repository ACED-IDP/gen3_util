import click

from gen3_util.access import create_request
from gen3_util.access.requestor import ls, cat, update, LogAccess
from gen3_util.access.submitter import ensure_program_project
from gen3_util.repo import CLIOutput
from gen3_util.repo import NaturalOrderGroup
from gen3_util.common import validate_email
from gen3_util.config import Config, ensure_auth


@click.group(name='access', cls=NaturalOrderGroup)
@click.pass_obj
def access_group(config: Config):
    """Manage access requests."""
    pass


@access_group.command(name="add")
@click.argument('user_name')
@click.option('--resource_path', default=None, required=False, show_default=True,
              help="Gen3 authz /programs/<program>")
@click.option('--roles', show_default=True, default=None, help='Add comma-delimited role permissions to the access request, ex: --roles "storage_writer,file_uploader"')
@click.pass_obj
def access_touch(config: Config,  resource_path: str, user_name: str, roles: str):
    """Create a request a specific role.

    \b
    USER_NAME (str): user's email

    """
    msgs = validate_email(user_name)
    assert msgs == [], f"Invalid email address: {user_name} {msgs}"

    assert user_name, "required"

    request = {"username": user_name, "resource_path": resource_path}
    if roles is not None:
        roles = list(map(str, roles.split(',')))
        request.update({"role_ids": roles})

    with CLIOutput(config=config) as output:
        try:
            output.update(create_request(config=config, request=request))
        except AssertionError as e:
            output.update({'msg': str(e)})
            output.exit_code = 1


@access_group.command(name="sign")
@click.option('--username', required=False, help='Sign all requests for user within a project')
@click.pass_obj
def sign(config: Config, username: str):
    """Sign all policies for a project.
    \b
    """
    with CLIOutput(config=config) as output:
        auth = ensure_auth(config=config)
        access = ls(config, mine=False, username=username, active=True, auth=auth)
        unsigned_requests = [_ for _ in access.requests if _['status'] != 'SIGNED']

        if len(unsigned_requests) == 0:
            output.update(LogAccess(**{
                'msg': "No unsigned requests found"
            }))
        else:
            msg = f"Signing {len(unsigned_requests)} requests."

            signed_requests = []
            for request in unsigned_requests:
                signed_requests.append(update(config, request_id=request['request_id'], status='SIGNED', auth=auth).request)

            msg = f"Signed {len(unsigned_requests)} requests."
            distinct_policy_ids = sorted(
                set(
                    [
                        _['policy_id'].replace('_reader', '').replace('_writer', '')
                        for _ in unsigned_requests if _['policy_id'].startswith('programs.')
                    ]
                )
            )
            submitter_msgs = []
            for policy_id in distinct_policy_ids:
                _ = policy_id.split('.')
                project_id = f"{_[1]}-{_[3]}"
                submitter_msgs.append(ensure_program_project(config, project_id, auth=auth))

            output.update(LogAccess(**{
                'msg': msg + ' ' + '/n'.join(submitter_msgs),
                'requests': signed_requests,
            }))


@access_group.command(name="ls")
@click.option('--username', required=False, default=None, help='Sign all requests for user within a project')
@click.option('--mine',  is_flag=True, show_default=True, default=False, help="List current user's requests. Otherwise, list all the requests the current user has access to see.")
@click.option('--all', 'active', is_flag=True, show_default=True, default=True, help='Only unsigned requests')
@click.pass_obj
def access_ls(config: Config, mine: bool, active: bool, username: str):
    """List current user's requests."""
    with CLIOutput(config=config) as output:
        try:
            msg = 'OK'
            access = ls(config, mine, active, username)
            if not access.requests or len(access.requests) == 0:
                msg = 'No unsigned requests'
            output.update({'requests': access.requests, 'msg': msg})
        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1


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
