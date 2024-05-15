import requests
from gen3.auth import Gen3Auth
from requests import HTTPError

from gen3_tracker.config import ensure_auth, Config


def _ensure_auth(auth, config):
    """Create auth from config, if we don't have one already."""
    if not auth:
        assert config
        auth = ensure_auth(config=config)
    return auth


def get_requests(config: Config = None, auth: Gen3Auth = None, mine: bool = False, active: bool = False, username: str = None) -> dict:
    """Fetch information about the user.
    Parameters
    ----------
    config : Config
        The config object.
    auth : Gen3Auth
        The auth object.
    mine : bool
        If True, return requests for the current user.
    active : bool
        If True, return only active (non-final requests) requests. see https://github.com/uc-cdis/requestor/blob/master/src/requestor/config-default.yaml#L63
    username : str
        If provided, return requests for this user.
    """
    auth = _ensure_auth(auth, config)
    if mine:
        # returns a list of dicts
        # https://github.com/uc-cdis/requestor/blob/master/src/requestor/routes/query.py#L200
        url = '/requestor/request/user'
        parms = []
        if active:
            parms.append("active")
        if len(parms) > 0:
            url = url + "?" + "&".join(parms)
        return auth.curl(url).json()
    else:
        # returns a list of dicts
        # https://github.com/uc-cdis/requestor/blob/master/src/requestor/routes/query.py#L158
        url = '/requestor/request'
        parms = []
        if username:
            parms.append(f"username={username}")
        if active:
            parms.append("active")
        if len(parms) > 0:
            url = url + "?" + "&".join(parms)
        return auth.curl(url).json()


def get_request(config: Config = None, auth: Gen3Auth = None, request_id: str = None):
    """Get a specific request"""
    assert request_id, "required"
    auth = _ensure_auth(auth, config)
    # returns a dict
    # https://github.com/uc-cdis/requestor/blob/master/src/requestor/routes/query.py#L235
    return auth.curl(f'/requestor/request/{request_id}').json()


def create_request(config: Config = None, auth: Gen3Auth = None, request: dict = None, revoke: bin = False):
    """Create a specific request"""
    auth = _ensure_auth(auth, config)
    one_of = ['policy_id', 'resource_paths', 'resource_path']
    assert any([k in request for k in one_of]), (f"one of {one_of} required", request)

    url = auth.endpoint + "/" + 'requestor/request'
    if revoke:
        url = url + "?revoke"

    response = requests.post(
        url, json=request, auth=auth
    )

    try:
        response.raise_for_status()
    except HTTPError as e:
        print(e)
        print(request)
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
