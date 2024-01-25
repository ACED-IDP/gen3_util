from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission

from gen3_util.config import Config, ensure_auth
from gen3_util.projects import ProjectSummaries, get_projects, ProjectSummary


def ls(config: Config, resource_filter: str = None, msgs: list[str] = [], auth: Gen3Auth = None, full: bool = True) -> ProjectSummaries:
    """List projects."""

    if not auth:
        auth = ensure_auth(config=config)
    submission = Gen3Submission(auth)

    projects = get_projects(auth, submission)

    if full:
        project_messages = {}
        for _program in projects:
            for _project in projects[_program]:
                if resource_filter and resource_filter != f"/programs/{_program}/projects/{_project}":
                    continue
                project_messages[
                    f"/programs/{_program}/projects/{_project}"
                ] = ProjectSummary(
                    in_sheepdog=projects[_program][_project]['exists'],
                    permissions=projects[_program][_project]['permissions'],
                )
    else:
        project_messages = []
        any_incomplete = False
        for _program in projects:
            for _project in projects[_program]:
                if resource_filter and resource_filter != f"/programs/{_program}/projects/{_project}":
                    continue
                indicator = 'OK' if projects[_program][_project]['exists'] else 'incomplete'
                if indicator == 'incomplete':
                    any_incomplete = True
                project_messages.append(f"/programs/{_program}/projects/{_project} {indicator}")
        if any_incomplete:
            msgs.append("incomplete projects are missing sheepdog records")
        else:
            msgs.append("all projects exist in sheepdog")

    if len(project_messages) == 0:
        msgs.append("No projects found.")

    return ProjectSummaries(**{
        'endpoint': auth.endpoint,
        'projects': project_messages,
        'messages': msgs
    })
