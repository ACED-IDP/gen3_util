from gen3.auth import Gen3Auth

from gen3_tracker.config import Config, ensure_auth
from gen3_tracker.projects import ProjectSummaries, get_projects, ProjectSummary


def ls(config: Config, resource_filter: str = None, msgs: list[str] = [], auth: Gen3Auth = None, full: bool = True) -> ProjectSummaries:
    """List projects."""
    # improve startup time by importing only what is needed
    from gen3.submission import Gen3Submission

    if not auth:
        auth = ensure_auth(config=config)
    submission = Gen3Submission(auth)

    projects = get_projects(auth, submission)

    if full:
        project_messages = {'complete': {}, 'incomplete': {}}
        for _program in projects:
            for _project in projects[_program]:
                if resource_filter and resource_filter != f"/programs/{_program}/projects/{_project}":
                    continue
                _ = 'complete'
                if not projects[_program][_project]['exists']:
                    _ = 'incomplete'
                project_messages[_][
                    f"/programs/{_program}/projects/{_project}"
                ] = ProjectSummary(
                    in_sheepdog=projects[_program][_project]['exists'],
                    permissions=projects[_program][_project]['permissions'],
                )
    else:
        project_messages = {'complete': [], 'incomplete': []}
        any_incomplete = False
        for _program in projects:
            for _project in projects[_program]:
                if resource_filter and resource_filter != f"/programs/{_program}/projects/{_project}":
                    continue
                if not projects[_program][_project]['exists']:
                    any_incomplete = True
                    project_messages['incomplete'].append(f"/programs/{_program}/projects/{_project}")
                else:
                    project_messages['complete'].append(f"/programs/{_program}/projects/{_project}")
        if any_incomplete:
            msgs.append("incomplete projects are missing sheepdog records")
        else:
            msgs.append("all projects exist in sheepdog")

    if len(project_messages) == 0:
        msgs.append("No projects found.")

    return ProjectSummaries(**{
        'endpoint': auth.endpoint,
        'incomplete': project_messages['incomplete'],
        'complete': project_messages['complete'],
        'messages': msgs
    })
