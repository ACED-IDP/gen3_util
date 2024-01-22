import sys

from gen3.jobs import Gen3Jobs

from gen3_util import Config
from gen3_util.repo.committer import commit_status
from gen3_util.common import read_ndjson_file, Push
from gen3_util.config import ensure_auth
from gen3_util.files.manifest import ls
from gen3_util.meta.lister import counts
from diskcache import Cache

INCOMPLETE_STATUSES = [None, 'Unknown', 'Running']


def status(config: Config, auth=None) -> list[str]:
    """Show project status."""
    logs = []
    if not auth and config.gen3.profile:
        auth = ensure_auth(config=config)
    if not auth:
        logs.append("Warning: disconnected mode.")

    # get the list of commits
    project_id = config.gen3.project_id

    pending_commits = commit_status(config, project_id)
    pending_commits = [
        {
            'commit_id': _['commit_id'],
            'message': _['message'],
            'resource_counts': _['resource_counts'],
            'manifest_files': [mf for mf in _['manifest']],
         } for _ in pending_commits
    ]
    remote = counts(config, auth=auth)

    manifest = ls(config, project_id=config.gen3.project_id)
    jobs_client = Gen3Jobs(auth_provider=auth)
    pushes = []
    completed_path = config.state_dir / project_id / 'commits' / 'completed.ndjson'

    cache_path = config.state_dir / project_id / '.cache'

    if completed_path.exists():
        for commits_dict in read_ndjson_file(completed_path):
            push = Push(**commits_dict)
            if push.published_job and push.published_job['output']['status'] in INCOMPLETE_STATUSES:
                with Cache(cache_path) as cache:
                    job = cache.get(push.published_job['output']['uid'])
                    if not job:
                        try:
                            job = jobs_client.get_status(push.published_job['output']['uid'])
                            if job and 'status' in job and job['status'] not in INCOMPLETE_STATUSES:
                                cache.set(push.published_job['output']['uid'], job)
                        except Exception as e:
                            print(f"Warning: {e}", file=sys.stderr)

                if job and 'status' in job:
                    push.published_job['output']['status'] = job['status']

            pushes.append(
                {
                    'published_timestamp': push.published_timestamp,
                    'published_job': push.published_job,
                    'commits':
                        [
                            f"{_.commit_id} {_.message}" for _ in push.commits
                        ]
                }
            )

    return {
        'logs': logs,
        'project_id': project_id,
        'local': {
            'pending_commits': pending_commits,
            'pushed_commits': pushes,
            'uncommitted_manifest': {
                'files': [_['file_name'] for _ in manifest]
            }
        },
        'remote': remote,
    }
