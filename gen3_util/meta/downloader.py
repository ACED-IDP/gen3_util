import pathlib

from gen3_util.config import Config, gen3_services


def cp(config: Config, object_id: str, path: pathlib.Path):
    """Download did from indexd."""
    file_client, _, _ = gen3_services(config=config)
    _ = file_client.download_single(object_id, path)
    return {"msg": "Downloaded" if _ else "Failed"}
