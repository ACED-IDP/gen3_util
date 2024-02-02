
import pathlib

from gen3_util.config import Config
from gen3_util.repo import CLIOutput
from gen3_util.common import to_metadata_dict
from gen3_util.meta.skeleton import transform_manifest_to_indexd_keys
from gen3_util.files.lister import ls


def files_ls_driver(config: Config, object_id: str, project_id: str, specimen: str, patient: str, observation: str, task: str, md5: str, is_metadata: bool, is_snapshot: bool, long: bool):
    """List uploaded files in a project bucket."""

    if not project_id:
        project_id = config.gen3.project_id
    with (CLIOutput(config=config) as output):
        try:
            _ = to_metadata_dict(
                is_metadata=is_metadata,
                is_snapshot=is_snapshot,
                md5=md5,
                observation=observation,
                patient=patient,
                project_id=project_id,
                specimen=specimen,
                task=task)
            _ = transform_manifest_to_indexd_keys(_)
            results = ls(config, object_id=object_id, metadata=_)
            if not long:
                results = {
                    'downloaded': [_['file_name'] for _ in results['records'] if pathlib.Path(_['file_name']).exists()],
                    'indexed': [_['file_name'] for _ in results['records'] if not pathlib.Path(_['file_name']).exists()]
                }
            output.update(results)
        except Exception as e:
            output.update({'msg': str(e)})
            output.exit_code = 1
