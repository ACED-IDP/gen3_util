
from gen3.submission import Gen3Submission

from gen3_util.config import Config, ensure_auth
from gen3_util.projects import ProjectSummaries, get_projects, ProjectSummary


def ls(config: Config, resource_filter: str = None, msgs: list[str] = []):
    """List projects."""

    auth = ensure_auth(config.gen3.refresh_file)
    submission = Gen3Submission(auth)

    projects = get_projects(auth, submission)

    project_messages = {}
    for _program in projects:
        for _project in projects[_program]:
            if resource_filter and resource_filter != f"/programs/{_program}/projects/{_project}":
                continue
            project_messages[
                f"/programs/{_program}/projects/{_project}"
            ] = ProjectSummary(in_sheepdog=projects[_program][_project]['in_sheepdog'], permissions=projects[_program][_project]['permissions'])

    return ProjectSummaries(**{
        'endpoint': auth.endpoint,
        'projects': project_messages,
        'messages': msgs
    })
