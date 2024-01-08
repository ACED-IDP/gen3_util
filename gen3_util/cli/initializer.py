from gen3_util.access.requestor import add_policies
from gen3_util.config import ensure_auth, init
from gen3_util.projects.lister import ls as project_ls


def initialize_project(config, project_id):
    """Initialize a project in the current directory."""
    logs = []
    auth = ensure_auth(profile=config.gen3.profile)
    program, project = project_id.split('-')
    projects = project_ls(config, auth=auth)
    existing_project = [_ for _ in projects.projects if _.endswith(project)]
    if len(existing_project) > 0:
        raise AssertionError(f"Project already exists: {existing_project[0]}")
    _ = add_policies(config, project_id, auth=auth)
    policy_msgs = [_.msg, f"See {_.commands}"]
    for _ in init(config, project_id):
        logs.append(_)
    logs.extend(policy_msgs)
    return logs
