from gen3.submission import Gen3Submission

from gen3_util.config import Config, gen3_services, ensure_auth


def ls(config: Config, object_id: str = None, metadata: dict = {}):
    """List files."""
    file_client, index_client, user, auth = gen3_services(config=config)
    if object_id:
        records = index_client.client.bulk_request(dids=[object_id])
        return {'records': [_.to_json() for _ in records]}

    params = {'metadata': metadata}
    records = index_client.client.list_with_params(params=params)
    return {'records': [_.to_json() for _ in records]}


def meta_nodes(config: Config, project_id: str):
    """Retrieve all the nodes in a project."""
    query = """
    {
      node(project_id: "PROJECT_ID") {
        id
        __typename
      }
    }
    """.replace('PROJECT_ID', project_id)

    auth = ensure_auth(config.gen3.refresh_file)
    response = Gen3Submission(auth).query(query)
    return response['data']['node']
