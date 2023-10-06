from functools import lru_cache

from gen3.submission import Gen3Submission

from gen3_util.config import Config, gen3_services


def ls(config: Config, object_id: str = None, metadata: dict = {}):
    """List files."""
    file_client, index_client, user, auth = gen3_services(config=config)
    if object_id:
        records = index_client.client.bulk_request(dids=[object_id])
        return {'records': [_.to_json() for _ in records]}

    params = {'metadata': metadata}
    records = index_client.client.list_with_params(params=params)
    return {'records': [_.to_json() for _ in records]}


def meta_nodes(config: Config, project_id: str, auth, gen3_type: str = 'document_reference'):
    """Retrieve all the nodes in a project."""

    offset = 0
    batch_size = 1000
    _nodes = []
    submission_client = Gen3Submission(auth)
    while True:
        query = """
        {
          node(project_id: "PROJECT_ID", of_type: "document_reference", first: FIRST, offset: OFFSET) {
            id
            __typename
          }
        }
        """.replace('PROJECT_ID', project_id).replace('FIRST', str(batch_size)).replace('OFFSET', str(offset))
        response = submission_client.query(query)
        if len(response['data']['node']) == 0:
            break
        _nodes.extend(response['data']['node'])
        offset += batch_size

    return _nodes


@lru_cache(maxsize=None)
def meta_resource(submission_client: Gen3Submission, project_id: str, gen3_type: str, identifier: str):
    """Retrieve an existing node from the Gen3 Graph."""

    if identifier:
        query = """
        {
          GEN3_TYPE(project_id: "PROJECT_ID", identifier: "IDENTIFIER") {
            id
            resourceType
          }
        }
        """.replace('GEN3_TYPE', gen3_type) \
            .replace('PROJECT_ID', project_id) \
            .replace('IDENTIFIER', identifier)
    else:
        query = """
        {
          GEN3_TYPE(project_id: "PROJECT_ID") {
            id
            resourceType
          }
        }
        """.replace('GEN3_TYPE', gen3_type) \
            .replace('PROJECT_ID', project_id)

    response = submission_client.query(query)

    if len(response['data'][gen3_type]) > 0:
        return response['data'][gen3_type][0]
    return None
