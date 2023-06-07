
from gen3.submission import Gen3Submission

from gen3_util.config import Config, ensure_auth
from gen3_util.projects import ProjectSummaries, get_projects, ProjectSummary


def ls(config: Config):
    """List projects."""

    auth = ensure_auth(config.gen3.refresh_file)
    submission = Gen3Submission(auth)

    msgs = []
    projects = get_projects(auth, submission)

    project_messages = {}
    for _program in projects:
        for _project in projects[_program]['projects']:
            project_messages[
                f"/programs/{_program}/projects/{_project}"
            ] = ProjectSummary(exists=projects[_program]['projects'][_project])

    return ProjectSummaries(**{
        'endpoint': auth.endpoint,
        'projects': project_messages,
        'messages': msgs
    })
