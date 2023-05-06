from typing import List

from pydantic import BaseModel

from gen3_util.config import Config, ensure_auth


class LogConfig(BaseModel):
    endpoint: str
    """The commons url"""
    projects: List[str]
    """List of projects"""


def ls(config: Config):
    """List projects."""
    auth = ensure_auth(config.gen3.refresh_file, validate=True)
    return LogConfig(**{
        'endpoint': auth.endpoint,
        'projects': [_ for _ in auth.curl('/user/user').json()['authz'].keys() if _.startswith('/programs')]
    })
