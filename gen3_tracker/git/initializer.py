
from gen3_tracker.collaborator.access.requestor import add_policies
from gen3_tracker.config import ensure_auth
from gen3_tracker.projects.lister import ls as project_ls


def initialize_project_server_side(config, project_id, auth=None):
    """Initialize a project in the current directory."""
    # improve startup time by importing only what is needed
    from gen3.submission import Gen3Submission

    if auth is None:
        if not config.gen3.profile:
            return ["Disconnected mode, skipping server side initialization"]
        auth = ensure_auth(config=config)

    logs = []
    program, project = project_id.split('-')
    projects = project_ls(config, auth=auth)
    existing_project = [_ for _ in projects.complete if _.endswith(project)]
    policy_msgs = []
    if len(existing_project) > 0:
        submission_client = Gen3Submission(auth)
        links = submission_client.get_projects(program)['links']
        sheepdog = [_ for _ in links if _ == f"/v0/submission/{program}/{project}"]
        if len(sheepdog) > 0:
            logs.append(f"Project already exists on server: {program}-{project}")
        else:
            logs.append(f"Pending request for project creation. Admin must sign access request for: /{program}/projects/{project}")
    else:
        _ = add_policies(config, project_id, auth=auth)
        # print(_)
        policy_msgs.extend([_.msg, f"See {_.commands}"])
        logs.extend(policy_msgs)
    return logs
