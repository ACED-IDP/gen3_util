import requests

from gen3_tracker.config import ensure_auth, default
from gen3.query import Gen3Query


def validate_document_in_grip(did: str, auth=None, project_id=None):
    """Simple query to validate a document in the grip graph."""
    if not auth:
        auth = ensure_auth(config=default())
    token = auth.get_access_token()
    result = requests.get(f"{auth.endpoint}/grip/writer/graphql/CALIPER/get-vertex/{did}/{project_id}",
                          headers={"Authorization": f"bearer {token}"}
                          ).json()
    assert result['data']['gid'] == did


def validate_document_in_elastic(did, auth):
    """Simple query to validate a document in elastic."""
    query = Gen3Query(auth)
    result = query.graphql_query(
        query_string="""
            query($filter:JSON) {
              file(filter:$filter) {
                id
              }
            }
        """,
        variables={"filter": {"AND": [{"IN": {"id": [did]}}]}}
    )
    print(result)
    assert result['data']['file'][0]['id'] == did
