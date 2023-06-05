from gen3.submission import Gen3Submission
from requests import HTTPError

from gen3_util.config import Config, ensure_auth
from gen3_util.projects import ProjectSummaries


def rm(config: Config, project_id: str):
    """Remove project."""
    assert '-' in project_id, f'Invalid project_id: {project_id}'
    program, project = project_id.split('-')
    assert program and project, f'Invalid project_id: {project_id}'

    auth = ensure_auth(config.gen3.refresh_file)
    submission = Gen3Submission(auth)

    try:
        response = submission.delete_project(program=program, project=project)
        response.raise_for_status()

        return ProjectSummaries(**{
            'endpoint': auth.endpoint,
            'projects': {project_id: {'exists': False}},
            'messages': [f'Deleted {project_id}']
        })

    except HTTPError as e:
        return ProjectSummaries(**{
            'endpoint': auth.endpoint,
            'projects': {project_id: {'exists': False}},
            'messages': [f'Error deleting {project_id}: {e} {e.response.text}']
        })
