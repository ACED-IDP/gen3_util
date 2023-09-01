from gen3_util.config import Config, gen3_services


def ls(config: Config, object_id: str, metadata: dict):
    """List files."""
    file_client, index_client, user = gen3_services(config=config)
    if object_id:
        records = index_client.client.bulk_request(dids=[object_id])
        return {'records': [_.to_json() for _ in records]}

    params = {'metadata': metadata}
    records = index_client.client.list_with_params(params=params)
    return {'records': [_.to_json() for _ in records]}
