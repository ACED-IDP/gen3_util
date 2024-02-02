from typing import Any

from gen3.jobs import Gen3Jobs

from gen3_util.common import print_formatted
from gen3_util.config import Config, ensure_auth


def ls(config: Config):
    """List jobs."""

    auth = ensure_auth(config=config)
    jobs_client = Gen3Jobs(auth_provider=auth)

    print_formatted(config, jobs_client.list_jobs())


def get(config: Config, job_id, auth=None) -> Any:
    """Get job."""

    if not auth:
        auth = ensure_auth(config=config)
    jobs_client = Gen3Jobs(auth_provider=auth)

    return jobs_client.get_status(job_id)
