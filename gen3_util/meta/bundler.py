import gzip
import pathlib
from collections import defaultdict

from fhir.resources.bundle import Bundle, BundleEntry
from gen3_util.meta import directory_reader


def meta_to_bundle(config, project_id: str, metadata_path: pathlib.Path, gz_file_path: pathlib.Path) -> dict:
    """Add metadata into a FHIR bundle, write to zip."""

    # create a bundle
    # see https://github.com/ACED-IDP/submission/issues/9
    bundle = Bundle(
        type='transaction',
        entry=[],
        identifier={"system": "https://aced-idp.org/project_id", "value": project_id}
    )

    logs = [f"Reading from {metadata_path}"]
    # add resources to bundle
    resource_counts = defaultdict(int)
    for parse_result in directory_reader(metadata_path, validate=True):
        _ = parse_result.resource
        entry = BundleEntry(
            resource=parse_result.resource,
            request={'method': 'PUT', 'url': parse_result.resource.resource_type}
        )
        bundle.entry.append(entry)
        resource_counts[_.resource_type] += 1

    # write bundle tp zip
    gz_file_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(gz_file_path, 'wb') as gz_file:
        gz_file.write(bundle.model_dump_json().encode('utf-8'))
    logs = [f"wrote {gz_file_path}"]
    return {
        'msg': f"created bundle {gz_file_path}",
        'resource_counts': dict(resource_counts),
        'logs': logs
    }
