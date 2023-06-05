import logging

from gen3_util.buckets import get_buckets
from gen3_util.projects import get_user


def assert_valid_project_id(config, project_id):
    """Assert that the project_id exists"""

    assert project_id, "project_id is missing"
    assert project_id.count('-') == 1, f"{project_id} should have a single '-' delimiter."

    program, project = project_id.split('-')

    user = get_user(config=config)

    program_exists = len([_ for _ in user['authz'].keys() if _.startswith(f'/programs/{program}')]) > 0
    project_exists = len(
        [_ for _ in user['authz'].keys() if _.startswith(f'/programs/{program}/projects/{project}')]) > 0

    if program_exists and project_exists:
        return

    logger = logging.getLogger(__name__)
    if not program_exists:
        logger.warning(f"program {program} does not exist")
    if not project_exists:
        logger.warning(f"project {project} does not exist")
    assert all([program_exists, project_exists]), f"{project_id} does not exist."


def assert_valid_bucket(config, bucket_name):
    buckets = get_buckets(config=config)
    bucket_names = [_ for _ in buckets['GS_BUCKETS']] + [_ for _ in buckets['S3_BUCKETS']]
    assert bucket_name in bucket_names, f"{bucket_name} not in configured buckets {bucket_names}"
