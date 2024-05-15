from typing import List, Any, Union
from collections import defaultdict

from gen3.auth import Gen3Auth
from pydantic import BaseModel

from gen3_tracker.config import ensure_auth, Config


class ProjectSummary(BaseModel):
    """Summary of a project."""
    exists: bool = False
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


def get_projects(auth, submission) -> dict:
    """Return a dict of programs, projects and their existence flag."""

    # get the list of programs in sheepdog
    response = submission.get_programs()
    assert 'links' in response, f'submission.get_program returned unexpected response: {response}'
    program_links = response['links']
    programs = [_.split('/')[-1] for _ in program_links]
    project_links = []
    sheepdog_projects = recursive_defaultdict()
    # add projects to it
    for program in programs:
        # print(program)
        project_links.extend(submission.get_projects(program)['links'])
        sheepdog_projects[program]['in_sheepdog'] = True
        sheepdog_projects[program]['projects'] = {}
    for _ in project_links:
        program, project = _.replace('/v0/submission/', '').split('/')
        sheepdog_projects[program]['projects'][project] = True

    # get the list of programs and projects from arborist, will be different from sheepdog
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
        arborist_projects[_program][_project]['exists'] = False
        if _program in sheepdog_projects and _project in sheepdog_projects[_program]['projects']:
            arborist_projects[_program][_project]['exists'] = True

    return arborist_projects
