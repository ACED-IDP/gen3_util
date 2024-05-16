import json
import logging


def ls(config, object_id: str = None, metadata: dict = {}, auth=None):
    """List files."""
    from gen3_tracker import Config
    config: Config = config
    from gen3.index import Gen3Index
    index_client = Gen3Index(auth_provider=auth)

    if object_id:
        if ',' in object_id:
            object_ids = object_id.split(',')
        else:
            object_ids = [object_id]
        records = index_client.client.bulk_request(dids=object_ids)
        return {'records': [_.to_json() for _ in records]}

    params = {}
    project_id = metadata.get('project_id', None)
    if 'project_id' in metadata:
        program, project = project_id.split('-')
        params = {'authz': f"/programs/{program}/projects/{project}"}
        metadata.pop('project_id')
    params['metadata'] = metadata

    records = index_client.client.list_with_params(params=params)

    def _ensure_project_id(record):
        if 'project_id' not in record['metadata'] and project_id:
            record['metadata']['project_id'] = project_id
        return record

    records = [_ensure_project_id(_.to_json()) for _ in records]

    return {
        'records': records,
        'msg': f"Project {project_id} has {len(records)} files."
    }


def find_latest_snapshot(auth, config):
    """
    Find the latest snapshot for a project.
    Looks for a hierarchy of files in the indexd database and returns the latest one found. The hierarchy is:
        * the latest git snapshot
        * the latest SNAPSHOT.zip created by the fhir-import-export job on output
        * the latest meta.zip created by the fhir-import-export client on input
    """
    results = ls(config=config, metadata={'project_id': config.gen3.project_id}, auth=auth)
    records = 'records' in results and results['records'] or []
    file_names = [_['file_name'] for _ in records]
    git_records = [r for r in records if 'git' in r['file_name']]
    git_records = sorted(git_records, key=lambda d: d['file_name'])
    download_meta = None
    if len(git_records) > 0:
        # most recent metadata, file_name has a timestamp
        download_meta = git_records[-1]
    else:
        logger = logging.getLogger(__name__)
        logger.info(f"No git snapshot found for {config.gen3.project_id}")
        snapshot_records = [r for r in records if 'SNAPSHOT.zip' in r['file_name']]
        snapshot_records = sorted(snapshot_records, key=lambda d: d['file_name'])
        if len(snapshot_records) > 0:
            download_meta = snapshot_records[-1]
        else:
            logger.info(f"No SNAPSHOT found for {config.gen3.project_id}")
            meta_records = [r for r in records if 'meta.zip' in r['file_name']]
            meta_records = sorted(meta_records, key=lambda d: d['file_name'])
            if len(meta_records) > 0:
                download_meta = meta_records[-1]

    assert download_meta, f"No git, snapshot or meta files found for {config.gen3.project_id}, file_names: {file_names}"
    return download_meta
