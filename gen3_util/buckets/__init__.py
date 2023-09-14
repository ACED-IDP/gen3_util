import logging

from gen3.auth import Gen3Auth

from gen3_util.config import ensure_auth, Config


def get_buckets(config: Config = None, auth: Gen3Auth = None) -> dict:
    """Fetch information about the buckets."""
    if not auth:
        assert config
        auth = ensure_auth(config.gen3.refresh_file)

    response = auth.curl('/user/data/buckets')

    # TODO - remove when no longer needed
    if response.status_code == 405:
        logging.getLogger(__name__).warning(
            "TODO /data/buckets response returned 405, "
            "see https://cdis.slack.com/archives/CDDPLU1NU/p1683566639636949 "
            "see quay.io/cdis/fence:feature_bucket_info_endpoint "
        )

    assert response.status_code == 200, (response.status_code, response.content)
    return response.json()


def get_program_bucket(config: Config, program: str, auth: Gen3Auth = None) -> str:
    """Get the bucket for a program."""
    buckets = get_buckets(config=config, auth=auth)
    bucket_name = None
    for k, v in buckets['S3_BUCKETS'].items():
        if program in v['programs']:
            bucket_name = k
            break
    # assert bucket_name, f"could not find bucket for {program}"
    return bucket_name
