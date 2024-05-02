import json


def ls(config, object_id: str = None, metadata: dict = {}, auth=None):
    """List files."""
    from g3t import Config
    config: Config = config
    from gen3.index import Gen3Index
    index_client = Gen3Index(auth_provider=auth)

    negate_params = {'metadata': {}}
    if metadata.get('is_metadata', False):
        metadata['is_metadata'] = 'true'
    # else:
    #     negate_params['metadata']['is_metadata'] = 'true'

    if metadata.get('is_snapshot', False):
        metadata['is_snapshot'] = 'true'

    if 'is_snapshot' not in metadata and 'is_metadata' not in metadata:
        negate_params['metadata']['is_snapshot'] = 'true'
        negate_params['metadata']['is_metadata'] = 'true'

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

    if len(negate_params['metadata']):
        params['negate_params'] = json.dumps(negate_params)

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
    results = ls(config=config, metadata={'project_id': config.gen3.project_id}, auth=auth)
    records = 'records' in results and results['records'] or []
    file_names = [_['file_name'] for _ in records]
    records = [r for r in records if 'git' in r['file_name']]
    records = sorted(records, key=lambda d: d['file_name'])
    assert len(records) > 0, f"No snapshot found for {config.gen3.project_id}, file_names: {file_names}"
    # print(f"Found {len(records)} metadata records {[_['file_name'] for _ in records]}", file=sys.stderr)
    # most recent metadata, file_name has a timestamp
    download_meta = records[-1]
    return download_meta
