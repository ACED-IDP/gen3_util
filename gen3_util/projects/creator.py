
from gen3.submission import Gen3Submission

from gen3_util.config import Config, ensure_auth
from gen3_util.projects import get_projects
from gen3_util.projects.lister import ls


def touch(config: Config, project_id: str, all_: bool):
    """Create project in sheepdog database."""

    program = project = None

    if not all_:
        assert project_id, "PROJECT_ID is required"
        assert '-' in project_id, f'Invalid project_id: {project_id}'
        program, project = project_id.split('-')
        assert program and project, f'Invalid project_id: {project_id}'

    auth = ensure_auth(config.gen3.refresh_file)
    submission = Gen3Submission(auth)

    msgs = []

    project_ids = {}

    if all_:
        projects = get_projects(auth, submission)
    else:
        projects = {program: {'exists': False,  'projects': {project: False}}}

    for _program in projects:
        if not projects[_program]['exists']:
            response = submission.create_program(
                {'name': _program, 'type': 'program', "dbgap_accession_number": _program})
            msgs.append(f"Created program:{_program} {response['message']}")
        for _project in projects[_program]['projects']:
            if projects[_program]['projects'][_project]:
                msgs.append(f"Project {_program}-{_project} already exists")
            else:
                response = submission.create_project(program=_program,
                                                     json={'code': _project, 'type': 'project', "state": "open",
                                                           "dbgap_accession_number": _project, })
                msgs.append(f"Created project: {_program}-{_project} {response['message']}")
                project_ids[f"{_program}-{_project}"] = {'in_sheepdog': True}

    return ls(config, f"/programs/{_program}/projects/{_project}", msgs)
