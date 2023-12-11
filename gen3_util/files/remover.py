from gen3_util.common import print_formatted
from gen3_util.config import Config, gen3_services


def rm(config: Config, object_id: str = None):
    """Remove files."""
    file_client, _, _, _ = gen3_services(config=config)
    if object_id:
        resp = file_client.delete_file_locations(object_id)
        return {'response': resp.status_code, 'msg': resp.text}

    return None
