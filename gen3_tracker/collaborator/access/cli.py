import click

from gen3_tracker.common import validate_email
from gen3_tracker.config import Config, ensure_auth
from gen3_tracker.common import CLIOutput
from gen3_tracker import NaturalOrderGroup


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
@click.option('--steward', show_default=True, is_flag=True, default=False, help='Add steward role to the program')
@click.pass_obj
def access_touch(config: Config,  resource_path: str, user_name: str, roles: str, steward: bool):
    """Create a request a specific role.

    \b
    USER_NAME (str): user's email

    """
    from gen3_tracker.collaborator.access import create_request

    with CLIOutput(config=config) as output:
        try:

            msgs = validate_email(user_name)
            assert msgs == [], f"Invalid email address: {user_name} {msgs}"

            assert user_name, "user_name required"

            request = {"username": user_name, "resource_path": resource_path}
            if steward:
                roles = 'requestor_reader_role,requestor_updater_role'

            assert roles, "roles required"
            roles = roles.split(',')
            for role in roles:
                request.update({"role_ids": [role]})
                print(request)
                output.update(create_request(config=config, request=request))
        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1


@access_group.command(name="sign")
@click.option('--username', required=False, help='Sign all requests for user within a project')
@click.option('--request_id', required=False, help='Sign only this request')
@click.pass_obj
def sign(config: Config, username: str, request_id: str):
    """Sign all policies for a project.
    \b
    """
    with CLIOutput(config=config) as output:
        from gen3_tracker.collaborator.access.requestor import ls, update, LogAccess

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
            problem_requests = []
            click.secho("signing requests...", fg='green')
            for request in unsigned_requests:
                if request_id and request['request_id'] != request_id:
                    continue
                try:
                    signed_requests.append(
                        update(config, request_id=request['request_id'], status='SIGNED', auth=auth).request
                    )
                except Exception as e:
                    print(f"Error signing request {request}: {e}")
                    problem_requests.append(request)

            msg = f"Signed {len(signed_requests)} requests. There were {len(problem_requests)} problems. System administrators will create new projects."

            output.update(LogAccess(**{
                'msg': msg,
                'requests': signed_requests,
            }))


@access_group.command(name="ls")
@click.option('--username', required=False, default=None, help='Sign all requests for user within a project')
@click.option('--mine',  is_flag=True, show_default=True, default=False, help="List current user's requests. Otherwise, list all the requests the current user has access to see.")
@click.option('--all', 'active', is_flag=True, show_default=True, default=True, help='Only unsigned requests')
@click.pass_obj
def access_ls(config: Config, mine: bool, active: bool, username: str):
    """List current user's requests."""
    from gen3_tracker.collaborator.access.requestor import ls

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
    from gen3_tracker.collaborator.access.requestor import cat

    with CLIOutput(config=config) as output:
        output.update(cat(config, request_id))
