from typing import List

from pydantic import BaseModel

from gen3_util.access import get_requests, get_request, create_request, update_request
from gen3_util.config import Config, ensure_auth


class LogAccess(BaseModel):
    endpoint: str
    """The commons url"""
    requests: List[dict] = None
    """List of requests"""
    request: dict = None
    """A single request"""


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
    """List requests."""
    auth = ensure_auth(config.gen3.refresh_file)
    request = create_request(auth=auth, resource_path=resource_path, user_name=user_name, roles=roles)
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
