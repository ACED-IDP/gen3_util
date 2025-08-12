from typing import List, Any, Union
from collections import defaultdict

from gen3.auth import Gen3Auth
from pydantic import BaseModel

from gen3_tracker.config import ensure_auth, Config


class ProjectSummary(BaseModel):
    """Summary of a project."""
    user_perms: bool = False
    """Project exists in sheepdog flag"""
    permissions: list[dict[str, Any]] = []


class ProjectSummaries(BaseModel):
    """Summary of projects, including messages."""
    endpoint: str
    """The commons url"""
    incomplete: Union[dict[str, ProjectSummary], list[str]] = {}
    """List of projects that require creation in sheepdog."""
    complete: Union[dict[str, ProjectSummary], list[str]] = {}
    """List of projects that exist in sheepdog."""
    messages: List[str] = []
    """List of messages"""


def get_user(config: Config = None, auth: Gen3Auth = None) -> dict:
    """Fetch information about the user."""
    if not auth:
        assert config
        auth = ensure_auth(config=config)
    return auth.curl('/user/user').json()


def recursive_defaultdict():
    return defaultdict(recursive_defaultdict)


def get_projects(auth) -> dict:
    """Return a dict of programs, projects and their existence flag."""

    # get the list of programs and projects from arborist
    user = get_user(auth=auth)
    arborist_projects = recursive_defaultdict()
    for _ in user['authz'].keys():
        if not all([_.startswith('/programs'), 'projects/' in _]):
            continue

        permissions = user['authz'][_]

        _ = _.replace('/programs/', '')
        _ = _.split('/')
        _program = _[0]
        _project = _[-1]
        arborist_projects[_program][_project]['permissions'] = permissions

        # Checking for all policies granted in reader and writer role in user.yaml
        has_read = any(_["method"] == "read" and _["service"] == "*" for _ in permissions)
        has_read_storage = any(_["method"] == "read-storage" and _["service"] == "*" for _ in permissions)
        has_create = any(_["method"] == "create" and _["service"] == "*" for _ in permissions)
        has_file_upload = any(_["method"] == "file_upload" and _["service"] == "fence" for _ in permissions)
        has_write_storage = any(_["method"] == "write-storage" and _["service"] == "*" for _ in permissions)
        has_update = any(_["method"] == "update" and _["service"] == "*" for _ in permissions)

        arborist_projects[_program][_project]['exists'] = False
        if all([has_read, has_read_storage, has_create, has_file_upload, has_write_storage, has_update]):
            arborist_projects[_program][_project]['exists'] = True

    return arborist_projects
