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


def meta_nodes(config: Config, project_id: str, gen3_type: str = 'document_reference'):
    """Retrieve all the nodes in a project."""

    auth = ensure_auth(config.gen3.refresh_file)

    offset = 0
    batch_size = 1000
    _nodes = []
    while True:
        query = """
        {
          node(project_id: "PROJECT_ID", of_type: "document_reference", first: FIRST, offset: OFFSET) {
            id
            __typename
          }
        }
        """.replace('PROJECT_ID', project_id).replace('FIRST', str(batch_size)).replace('OFFSET', str(offset))
        response = Gen3Submission(auth).query(query)
        if len(response['data']['node']) == 0:
            break
        _nodes.extend(response['data']['node'])
        offset += batch_size

    return _nodes
