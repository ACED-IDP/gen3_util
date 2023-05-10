from gen3.auth import Gen3Auth

from gen3_util.config import ensure_auth, Config


def get_user(config: Config = None, auth: Gen3Auth = None) -> dict:
    """Fetch information about the user."""
    if not auth:
        assert config
        auth = ensure_auth(config.gen3.refresh_file)
    return auth.curl('/user/user').json()
