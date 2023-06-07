from typing import List
from collections import defaultdict

from gen3.auth import Gen3Auth
from pydantic import BaseModel
from pydantic_yaml import YamlModel

from gen3_util.config import ensure_auth, Config


class ProjectSummary(BaseModel):
    """Summary of a project."""
    exists: bool
    """Project exists flag"""


class ProjectSummaries(YamlModel):
    """Summary of projects, including messages."""
    endpoint: str
    """The commons url"""
    projects: dict[str, ProjectSummary]
    """List of projects"""
    messages: List[str] = []
    """List of messages"""


def get_user(config: Config = None, auth: Gen3Auth = None) -> dict:
    """Fetch information about the user."""
    if not auth:
        assert config
        auth = ensure_auth(config.gen3.refresh_file)
    return auth.curl('/user/user').json()


def recursive_defaultdict():
    return defaultdict(recursive_defaultdict)


def get_projects(auth, submission) -> dict:
    """Return a dict of programs, projects and their existence flag."""

    response = submission.get_programs()
    assert 'links' in response, f'submission.get_program returned unexpected response: {response}'
    program_links = response['links']
    # print(program_links)
    programs = [_.split('/')[-1] for _ in program_links]
    project_links = []
    projects = recursive_defaultdict()
    for program in programs:
        # print(program)
        project_links.extend(submission.get_projects(program)['links'])
        projects[program]['exists'] = True
        projects[program]['projects'] = {}
    for _ in project_links:
        program, project = _.replace('/v0/submission/', '').split('/')
        projects[program]['projects'][project] = True
    user = get_user(auth=auth)
    for _ in user['authz'].keys():
        if not all([_.startswith('/programs'), 'projects/' in _]):
            continue
        _ = _.replace('/programs/', '')
        _ = _.split('/')
        _program = _[0]
        _project = _[-1]
        if _program not in programs:
            projects[_program]['exists'] = False
        if all([_program in programs, _project in projects[_program]['projects']]):
            continue
        projects[_program]['projects'][_project] = False
    return projects
