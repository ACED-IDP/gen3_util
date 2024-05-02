import sys

import click
from halo import Halo

import g3t.collaborator.access.requestor
from g3t.collaborator.access.requestor import add_user, rm_user, update
from g3t import NaturalOrderGroup, ENV_VARIABLE_PREFIX
from g3t.common import CLIOutput, assert_config, ERROR_COLOR
from g3t.config import Config, ensure_auth


@click.group(name='collaborator', cls=NaturalOrderGroup)
@click.pass_obj
def collaborator(config: Config):
    """Manage project membership."""
    assert_config(config)


@collaborator.command(name="add")
@click.argument('username', required=True, type=str)
@click.option('--write/--no-write', '-w', help='Give user write privileges', is_flag=True, default=False, show_default=True)
# @click.option('--delete/--no-delete', '-d', help='Give user delete privileges', is_flag=True, default=False, show_default=True)
@click.option('--approve', '-a', help='Approve the addition (privileged)', is_flag=True, default=False, show_default=True)
@click.pass_obj
def project_add_user(config: Config, username: str, write: bool, approve: bool):
    """Add user to project."""
    assert username, "username (email) required"
    project_id = config.gen3.project_id
    program = config.gen3.program
    project = config.gen3.project

    with CLIOutput(config=config) as output:
        try:
            with Halo(text='Searching', spinner='line', placement='right', color='white'):
                auth = ensure_auth(config=config)
                existing_requests = g3t.collaborator.access.requestor.ls(config=config, mine=False, auth=auth, username=username).requests
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
    with CLIOutput(config=config) as output:
        try:
            program = config.gen3.program
            project = config.gen3.project

            with Halo(text='Searching', spinner='line', placement='right', color='white'):
                auth = ensure_auth(config=config)
                existing_requests = g3t.collaborator.access.requestor.ls(config=config, mine=False, auth=auth).requests
                existing_requests = [r for r in existing_requests if r['policy_id'].startswith(f'programs.{program}.projects.{project}')]
                output.update({'existing': [{'policy_id': r['policy_id'], 'request_id': r['request_id'], 'status': r['status'], 'username': r['username']} for r in existing_requests]})

        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1
            if config.debug:
                raise e


@collaborator.command(name="approve")
@click.option('--request_id', required=False, help='Sign only this request')
@click.option('--all',  'all_requests', required=False, is_flag=True, help='Sign all requests')
@click.pass_obj
def project_approve_request(config: Config, request_id: str, all_requests: bool):
    """Sign an existing request (privileged)."""
    with CLIOutput(config=config) as output:
        try:
            assert request_id or all_requests, "request_id or --all required"
            with Halo(text='Signing', spinner='line', placement='right', color='white'):
                auth = ensure_auth(config=config)
                program = config.gen3.program
                project = config.gen3.project
                if all_requests:
                    existing_requests = g3t.collaborator.access.requestor.ls(config=config, mine=False,
                                                                             auth=auth).requests
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
                    r = update(config, request_id=request_id, status='SIGNED', auth=auth)
                    output.update(
                        {'approved': [{'policy_id': r['policy_id'], 'request_id': r['request_id'], 'status': r['status'], 'username': r['username']}]}
                    )

        except Exception as e:
            click.secho(str(e), fg=ERROR_COLOR, file=sys.stderr)
            output.exit_code = 1
            if config.debug:
                raise e
