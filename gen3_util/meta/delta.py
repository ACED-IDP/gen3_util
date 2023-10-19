from gen3.submission import Gen3Submission
from gen3_util.config import ensure_auth, Config


def get(config: Config, project_id: str, node_ids: list[str]):
    """Check if a node exists in the sheepdog database."""
    auth = ensure_auth(config.gen3.refresh_file)
    submission_client = Gen3Submission(auth)

    assert project_id, "project_id is required"
    assert '-' in project_id, "project_id must be in the form program-project"
    assert node_ids, "node_id is required"

    program, project = project_id.split('-')
    if isinstance(node_ids, str):
        node_ids = [node_ids]

    return {_: submission_client.export_record(program, project, _, fileformat='json') for _ in node_ids}
