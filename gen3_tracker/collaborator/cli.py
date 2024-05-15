import sys

import click
from halo import Halo

from gen3_tracker import NaturalOrderGroup, ENV_VARIABLE_PREFIX
from gen3_tracker.collaborator.access.requestor import update
from gen3_tracker.common import CLIOutput, assert_config, ERROR_COLOR, validate_email
from gen3_tracker.config import Config, ensure_auth


@click.group(name='collaborator', cls=NaturalOrderGroup)
@click.pass_context
def collaborator(ctx):
    """Manage project membership."""
    cmd = ctx.invoked_subcommand
    if cmd not in ['add-steward', 'pending', 'approve', 'add']:
        config: Config = ctx.obj
        assert_config(config)


@collaborator.command(name="add")
@click.argument('username', required=True, type=str)
@click.argument('resource_path', required=False, type=str)
@click.option('--write/--no-write', '-w', help='Give user write privileges', is_flag=True, default=False, show_default=True)
# @click.option('--delete/--no-delete', '-d', help='Give user delete privileges', is_flag=True, default=False, show_default=True)
@click.option('--approve', '-a', help='Approve the addition (privileged)', is_flag=True, default=False, show_default=True)
@click.pass_obj
def project_add_user(config: Config, username: str, resource_path: str, write: bool, approve: bool):
    """Add user to project."""
    import gen3_tracker.collaborator.access.requestor
    from gen3_tracker.collaborator.access.requestor import add_user, update

    assert username, "username (email) required"
    if resource_path:
        resource_path = resource_path.split('/')
        program = resource_path[2]
        project = resource_path[4]
        project_id = f"{program}-{project}"
    else:
        project_id = config.gen3.project_id
        program = config.gen3.program
        project = config.gen3.project

    assert program, "program required"
    assert project, "project required"

    with CLIOutput(config=config) as output:
        try:
            with Halo(text='Searching', spinner='line', placement='right', color='white'):
                auth = ensure_auth(config=config)
                existing_requests = gen3_tracker.collaborator.access.requestor.ls(config=config, mine=False, auth=auth, username=username).requests
                existing_requests = [r for r in existing_requests if r['policy_id'].startswith(f'programs.{program}.projects.{project}')]
                needs_approval = []
                needs_adding = False
                if not existing_requests:
                    needs_adding = True
                else:
                    if write and not any(r['policy_id'].endswith('writer') for r in existing_requests):
                        needs_adding = True
                if needs_adding:
                    click.secho(f"There are no existing requests for {username}, adding them to project.", fg='yellow')
                    _ = add_user(config, project_id, username, write, delete=False, auth=auth)
                    needs_approval.extend(_.requests)
                else:
                    for request in existing_requests:
                        if request['status'] != 'SIGNED':
                            needs_approval.append(request)

            if approve and not needs_approval:
                click.secho(f"User {username} already has approved requests for {project_id}.", fg='yellow')
                output.update({'existing': [{'policy_id': r['policy_id'], 'request_id': r['request_id'], 'status': r['status'], 'username': r['username']} for r in existing_requests]})
            elif not approve and needs_approval:
                output.update(
                    {
                        'needs_approval': [{'policy_id': r['policy_id'], 'request_id': r['request_id'], 'status': r['status'], 'username': r['username']} for r in needs_approval],
                        'msg': f"An authorized user must approve these requests to add {username} to {project_id} see --approve",
                     })
            else:
                approvals = []
                with Halo(text='Approving', spinner='line', placement='right', color='white'):
                    for request in needs_approval:
                        approvals.append(update(config, request_id=request['request_id'], status='SIGNED', auth=auth).request)
                output.update({'approved': [{'policy_id': r['policy_id'], 'request_id': r['request_id'], 'status': r['status'], 'username': r['username']} for r in approvals]})

        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1
            if config.debug:
                raise e


@collaborator.command(name="rm")
@click.argument('username', required=True, type=str)
@click.option('--approve', '-a', help='Approve the removal (privileged)', is_flag=True, default=False, show_default=True)
@click.pass_obj
def project_rm_user(config: Config, username: str, approve: bool):
    """Remove user from project."""
    from gen3_tracker.collaborator.access.requestor import rm_user, update

    with CLIOutput(config=config) as output:
        try:
            assert username, "username (email) required"
            project_id = config.gen3.project_id

            approvals = []
            needs_approval = []
            auth = ensure_auth(config=config)
            with Halo(text='Removing', spinner='line', placement='right', color='white'):
                _ = rm_user(config, project_id, username)
                needs_approval.extend(_.requests)
            if approve:
                with Halo(text='Approving', spinner='line', placement='right', color='white'):
                    for request in needs_approval:
                        _ = update(config, request_id=request['request_id'], status='SIGNED', auth=auth).request
                        approvals.append(_)
                output.update({'approved': [{'policy_id': r['policy_id'], 'request_id': r['request_id'], 'status': r['status']} for r in approvals]})
            else:
                output.update({'needs_approval': [{'policy_id': r['policy_id'], 'request_id': r['request_id'], 'status': r['status']} for r in needs_approval]})

        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1
            if config.debug:
                raise e


@collaborator.command(name="ls")
@click.pass_obj
def project_rm_user(config: Config):
    """List all users in project."""
    import gen3_tracker.collaborator.access.requestor

    with CLIOutput(config=config) as output:
        try:
            program = config.gen3.program
            project = config.gen3.project

            with Halo(text='Searching', spinner='line', placement='right', color='white'):
                auth = ensure_auth(config=config)
                existing_requests = gen3_tracker.collaborator.access.requestor.ls(config=config, mine=False, auth=auth).requests
                existing_requests = [r for r in existing_requests if r['policy_id'].startswith(f'programs.{program}.projects.{project}')]
                output.update({'existing': [{'policy_id': r['policy_id'], 'request_id': r['request_id'], 'status': r['status'], 'username': r['username']} for r in existing_requests]})

        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1
            if config.debug:
                raise e


@collaborator.command(name="pending")
@click.pass_obj
def project_ls_pending(config: Config):
    """Show all pending requests."""
    import gen3_tracker.collaborator.access.requestor

    with CLIOutput(config=config) as output:
        try:
            with Halo(text='Searching', spinner='line', placement='right', color='white'):
                auth = ensure_auth(config=config)
                existing_requests = gen3_tracker.collaborator.access.requestor.ls(config=config, mine=False,
                                                                                  auth=auth).requests
                needs_approval = []
                for request in existing_requests:
                    if request['status'] != 'SIGNED':
                        needs_approval.append(request)

                output.update(
                    {'existing': [{'policy_id': r['policy_id'], 'request_id': r['request_id'], 'status': r['status'], 'username': r['username']} for r in needs_approval]}
                )

        except Exception as e:
            click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
            output.exit_code = 1
            if config.debug:
                raise e


@collaborator.command(name="approve")
@click.option('--request_id', required=False, help='Sign only this request')
@click.option('--all',  'all_requests', required=False, is_flag=True, help='Sign all requests')
@click.pass_obj
def project_approve_request(config: Config, request_id: str, all_requests: bool):
    """Sign an existing request (privileged)."""
    import gen3_tracker.collaborator.access.requestor
    from gen3_tracker.collaborator.access.requestor import update

    with CLIOutput(config=config) as output:
        try:
            assert request_id or all_requests, "request_id or --all required"
            with Halo(text='Signing', spinner='line', placement='right', color='white'):
                auth = ensure_auth(config=config)
                program = config.gen3.program
                project = config.gen3.project
                if all_requests:
                    existing_requests = gen3_tracker.collaborator.access.requestor.ls(config=config, mine=False,
                                                                                      auth=auth).requests

                    if program and project:
                        existing_requests = [r for r in existing_requests if
                                             r['policy_id'].startswith(f'programs.{program}.projects.{project}')]
                    needs_approval = []
                    for request in existing_requests:
                        if request['status'] != 'SIGNED':
                            needs_approval.append(request)

                    approved = []
                    assert len(needs_approval) > 0, "No requests to approve.  You must be a privileged user to approve requests."
                    for request in needs_approval:
                        approved.append(update(config, request_id=request['request_id'], status='SIGNED', auth=auth).request)
                    output.update(
                        {'approved': [{'policy_id': r['policy_id'], 'request_id': r['request_id'], 'status': r['status'], 'username': r['username']} for r in approved]}
                    )
                else:
                    r = update(config, request_id=request_id, status='SIGNED', auth=auth).request
                    output.update(
                        {'approved': [{'policy_id': r['policy_id'], 'request_id': r['request_id'], 'status': r['status'], 'username': r['username']}]}
                    )

        except Exception as e:
            click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
            output.exit_code = 1
            if config.debug:
                raise e


@collaborator.command(name="add-steward", hidden=True)
@click.argument('user_name', required=True)
@click.argument('resource_path', required=True)
@click.option('--approve', '-a', help='Approve the addition (privileged)', is_flag=True, default=False, show_default=True)
@click.pass_obj
def add_steward(config: Config,  resource_path: str, user_name: str, approve: bool):
    """Add a data steward user with approval rights to a program.

    \b
    USER_NAME (str): user's email
    RESOURCE_PATH (str): Gen3 authz /programs/<program>
    """
    from gen3_tracker.collaborator.access import create_request

    with CLIOutput(config=config) as output:
        try:

            msgs = validate_email(user_name)
            assert msgs == [], f"Invalid email address: {user_name} {msgs}"
            assert user_name, "user_name required"

            request = {"username": user_name, "resource_path": resource_path}
            roles = ['requestor_reader_role', 'requestor_updater_role']

            needs_approval = []
            approvals = []
            with Halo(text='Adding', spinner='line', placement='right', color='white'):
                auth = ensure_auth(config=config)

                user = auth.curl('/user/user').json()
                is_privileged = False
                for _ in user['authz']['/programs']:
                    if _['method'] == 'update' and _['service'] == 'requestor':
                        is_privileged = True
                        break
                assert is_privileged, "You must be a privileged user to add a steward."

                for role in roles:
                    request.update({"role_ids": [role]})
                    needs_approval.append(create_request(config=config, request=request))

            if approve:
                with Halo(text='Approving', spinner='line', placement='right', color='white'):
                    for request in needs_approval:
                        approvals.append(update(config, request_id=request['request_id'], status='SIGNED', auth=auth).request)
                output.update({'approved': [{'policy_id': r['policy_id'], 'request_id': r['request_id'], 'status': r['status'], 'username': r['username']} for r in approvals]})
            else:
                output.update({'needs_approval': [{'policy_id': r['policy_id'], 'request_id': r['request_id'], 'status': r['status'], 'username': r['username']} for r in needs_approval]})

        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1
            if config.debug:
                raise e
