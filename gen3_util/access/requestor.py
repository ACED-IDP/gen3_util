from typing import List

import yaml
from pydantic import BaseModel

from gen3_util.access import get_requests, get_request, create_request, update_request
from gen3_util.config import Config, ensure_auth

import importlib.resources as pkg_resources
from . import policies


class LogAccess(BaseModel):
    endpoint: str = None
    """The commons url"""
    requests: List[dict] = None
    """List of requests"""
    request: dict = None
    """A single request"""
    msg: str = None
    """A message"""
    commands: List[str] = None
    """A list of commands to run"""


def format_policy(policy: dict, project_id: str, user_name: str) -> dict:
    """Format policy by applying project_id and user_name.

    Parameters:
    policy (dict): policy to format
    project_id (str): project_id to apply to policy's resource_path PROGRAM PROJECT tokens
    user_name (str): user_name to apply to policy's username
    """
    if user_name and policy.get('username', None) is None:
        policy['username'] = user_name
    if project_id:
        program, project = project_id.split('-')
        if policy.get('resource_path', None):
            policy['resource_path'] = policy['resource_path'].replace('PROGRAM', program).replace('PROJECT', project)
        elif policy.get('policy_id', None):
            policy['policy_id'] = policy['policy_id'].replace('PROGRAM', program).replace('PROJECT', project)
        else:
            raise ValueError(f"No resource_path specified, can't apply project_id {policy}")
    else:
        if 'PROGRAM' in policy['resource_path'] or 'PROJECT' in policy['resource_path']:
            raise ValueError(f"specify project_id for {policy['resource_path']}")
    return policy


def ls(config: Config, mine: bool) -> LogAccess:
    """List requests."""
    auth = ensure_auth(config.gen3.refresh_file)
    requests = get_requests(auth=auth, mine=mine)
    return LogAccess(**{
        'endpoint': auth.endpoint,
        'requests': [_ for _ in requests],
    })


def cat(config: Config, request_id: str) -> dict:
    """Show a specific request requests."""
    auth = ensure_auth(config.gen3.refresh_file)
    request = get_request(auth=auth, request_id=request_id)
    return LogAccess(**{
        'endpoint': auth.endpoint,
        'requests': [request],
    })


def touch(config: Config, resource_path: str, user_name: str, roles: str) -> LogAccess:
    """Create requests."""

    assert resource_path, "required"
    assert user_name, "required"
    request = {"username": user_name, "resource_path": resource_path}
    if roles is not None:
        roles = list(map(str, roles.split(',')))
        request.update({"role_ids": roles})

    auth = ensure_auth(config.gen3.refresh_file)

    request = create_request(auth=auth, request=request)
    return LogAccess(**{
        'endpoint': auth.endpoint,
        'request': request,
    })


def cp(config: Config, request: dict) -> LogAccess:
    """List requests."""

    auth = ensure_auth(config.gen3.refresh_file)

    request = create_request(auth=auth, request=request)
    return LogAccess(**{
        'endpoint': auth.endpoint,
        'request': request,
    })


ALLOWED_REQUEST_STATUSES = """DRAFT SUBMITTED APPROVED SIGNED REJECTED""".split()


def update(config: Config, request_id: str, status: str) -> LogAccess:
    """Update request."""
    assert request_id, "required"
    assert status, "required"
    status = status.upper()
    assert status in ALLOWED_REQUEST_STATUSES, f"{status} not in {ALLOWED_REQUEST_STATUSES}"

    auth = ensure_auth(config.gen3.refresh_file)
    request = update_request(auth=auth, request_id=request_id, status=status)
    return LogAccess(**{
        'endpoint': auth.endpoint,
        'request': request,
    })


def add_user(config: Config, project_id: str, user_name: str, write: bool) -> LogAccess:
    """Add user to project by assigning them policies."""

    # implement read from resource_path
    policies_ = []
    file_names = ['add-user-read.yaml']
    if write:
        file_names.append('add-user-write.yaml')

    for file_name in file_names:
        with pkg_resources.open_text(policies, file_name) as f:
            policies_.extend([_ for _ in yaml.safe_load(f)['policies']])

    requests = []
    request_ids = []
    for policy in policies_:
        policy = format_policy(policy, project_id, user_name)
        requests.append(cp(request=policy, config=config).request)
        request_ids.append(requests[-1]['request_id'])

    commands = [f"gen3_util access update {request_id} SIGNED" for request_id in request_ids]
    msg = f"Approve these requests to add {user_name} to {project_id}"

    return LogAccess(**{
        'requests': requests,
        'commands': commands,
        'msg': msg,
    })


def add_policies(config: Config, project_id: str) -> LogAccess:
    """Add policies to project."""
    # implement read from resource_path
    policies_ = []
    file_names = ['add-project-default.yaml']

    for file_name in file_names:
        with pkg_resources.open_text(policies, file_name) as f:
            policies_.extend([_ for _ in yaml.safe_load(f)['policies']])

    requests = []
    request_ids = []
    for policy in policies_:
        policy = format_policy(policy, project_id, user_name=None)
        requests.append(cp(request=policy, config=config).request)
        request_ids.append(requests[-1]['request_id'])

    commands = [f"gen3_util access update {request_id} SIGNED" for request_id in request_ids]
    msg = f"Approve these requests to assign default policies to {project_id}"
    return LogAccess(**{
        'requests': requests,
        'msg': msg,
        'commands': commands,
    })
