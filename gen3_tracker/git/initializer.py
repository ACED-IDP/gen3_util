
from gen3_tracker.collaborator.access.requestor import add_policies
from gen3_tracker.config import ensure_auth
from gen3_tracker.projects.lister import ls as project_ls
from typing import List


def initialize_project_server_side(config, project_id, auth=None) -> List[str] | bool:
    """Initialize a project in the current directory."""
    if auth is None:
        if not config.gen3.profile:
            return ["Disconnected mode, skipping server side initialization"]
        auth = ensure_auth(config=config)

    logs = []
    program, project = project_id.split('-')
    projects = project_ls(config, auth=auth)
    existing_project = [_ for _ in projects.complete if _.endswith(project)]
    policy_msgs = []
    # looking for prject to exist, but not reader permissions on project, then sign project.
    # If at least reader permissions then carry on.
    if len(existing_project) > 0:
        logs.append(f"Pending request for project creation. Admin must sign access request for: /{program}/projects/{project}")
        return logs, False
    else:
        _ = add_policies(config, project_id, auth=auth)
        policy_msgs.extend([_.msg, f"See {_.commands}"])
        logs.extend(policy_msgs)
    return logs, True
