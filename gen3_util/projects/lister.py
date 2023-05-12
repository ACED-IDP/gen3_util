from typing import List

from pydantic import BaseModel

from gen3_util.config import Config, ensure_auth
from gen3_util.projects import get_user


class LogConfig(BaseModel):
    endpoint: str
    """The commons url"""
    projects: List[str]
    """List of projects"""


def ls(config: Config):
    """List projects."""
    auth = ensure_auth(config.gen3.refresh_file)
    user = get_user(auth=auth)
    return LogConfig(**{
        'endpoint': auth.endpoint,
        'projects': [_ for _ in user['authz'].keys() if _.startswith('/programs')],
    })
