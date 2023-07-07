import requests
from gen3.auth import Gen3Auth
from requests import HTTPError

from gen3_util.config import ensure_auth, Config


def _ensure_auth(auth, config):
    """Create auth from config, if we don't have one already."""
    if not auth:
        assert config
        auth = ensure_auth(config.gen3.refresh_file)
    return auth


def get_requests(config: Config = None, auth: Gen3Auth = None, mine: bool = False) -> dict:
    """Fetch information about the user."""
    auth = _ensure_auth(auth, config)
    if mine:
        # returns a list of dicts
        # https://github.com/uc-cdis/requestor/blob/master/src/requestor/routes/query.py#L200
        return auth.curl('/requestor/request/user').json()
    else:
        # returns a list of dicts
        # https://github.com/uc-cdis/requestor/blob/master/src/requestor/routes/query.py#L158
        return auth.curl('/requestor/request').json()


def get_request(config: Config = None, auth: Gen3Auth = None, request_id: str = None):
    """Get a specific request"""
    assert request_id, "required"
    auth = _ensure_auth(auth, config)
    # returns a dict
    # https://github.com/uc-cdis/requestor/blob/master/src/requestor/routes/query.py#L235
    return auth.curl(f'/requestor/request/{request_id}').json()


def create_request(config: Config = None, auth: Gen3Auth = None, request: dict = None):
    """Get a specific request"""
    auth = _ensure_auth(auth, config)
    one_of = ['policy_id', 'resource_paths', 'resource_path']
    assert any([k in request for k in one_of]), (f"one of {one_of} required", request)
    response = requests.post(
        auth.endpoint + "/" + 'requestor/request', json=request, auth=auth
    )
    try:
        response.raise_for_status()
    except HTTPError as e:
        print(e)
        print(response.text)
        raise e

    return response.json()


def update_request(config: Config = None, auth: Gen3Auth = None, request_id: str = None, status: str = None):
    """Update a specific request"""
    assert request_id, "required"
    assert status, "required"
    auth = _ensure_auth(auth, config)
    request = {'status': status}
    response = requests.put(
        auth.endpoint + "/" + f'requestor/request/{request_id}', json=request, auth=auth
    )
    response.raise_for_status()
    return response.json()
