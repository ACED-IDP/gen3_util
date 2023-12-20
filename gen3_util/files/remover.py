from json import JSONDecodeError

from gen3.file import Gen3File

from gen3_util.config import Config, ensure_auth


def rm(config: Config, object_id: str) -> dict:
    """Remove files."""
    auth = ensure_auth(profile=config.gen3.profile)
    file_client = Gen3File(auth_provider=auth)
    response = file_client.delete_file_locations(guid=object_id)
    response.raise_for_status()
    try:
        _ = response.json()
    except JSONDecodeError:
        _ = {'msg': response.text}
    # delete_file_locations doesn't return anything useful
    if _ == {} or _ == {'msg': ''}:
        _ = {'msg': f"object_id {object_id} removed"}

    return _
