from gen3_util.config import Config, gen3_services


def ls(config: Config):
    """List meta."""
    file_client, index_client, user = gen3_services(config=config)
    records = index_client.client.list_with_params(limit=1000, params={'metadata': {'is_metadata': 'true'}})
    return {'records': [_.to_json() for _ in records]}
