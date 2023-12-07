from gen3_util.common import print_formatted
from gen3_util.config import Config, gen3_services


def rm(config: Config, object_id: str = None):
    """Remove files."""
    print_formatted(config, {'msg': 'file removal message goes here'})  # TODO implement
    file_client, index_client, user, auth = gen3_services(config=config)
    if object_id:
        resp = file_client.delete_file_locations(object_id)
        return resp

    return None
