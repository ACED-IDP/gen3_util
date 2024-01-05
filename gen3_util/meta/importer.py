import hashlib
import logging
import uuid

import click
import unicodedata

from gen3_util import ACED_NAMESPACE
from gen3_util.cli import CLIOutput
from gen3_util.config import Config

logger = logging.getLogger(__name__)


def create_research_study(name, description):
    """Creates bare-bones study."""
    study = {
        'title': name,
        'id': str(uuid.uuid5(ACED_NAMESPACE, name)),
        'description': description,
        'status': 'active',
        "resourceType": "ResearchStudy",
    }
    return study


def md5sum(file_name):
    """Calculate the hash and size."""
    md5_hash = hashlib.md5()
    file_name = unicodedata.normalize("NFKD", str(file_name))
    with open(file_name, "rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)

    return md5_hash.hexdigest()


@click.command('create')
@click.argument('output_path')
@click.option('--project_id', required=True,
              default=None,
              show_default=True,
              help='Gen3 program-project',
              envvar='PROJECT_ID'
              )
@click.option("--overwrite", is_flag=True, show_default=True, default=False, help="Ignore existing records.")
@click.option('--source',
              type=click.Choice(['manifest', 'indexd'], case_sensitive=False), show_default=True, default='manifest', help="Query manifest or indexd.")
@click.pass_obj
def import_indexd(config: Config, output_path, project_id, overwrite, source):
    """Create minimal study metadata from uploaded files
    """

    from gen3_util.meta.skeleton import study_metadata

    with CLIOutput(config=config) as output:
        output.update(study_metadata(config=config, project_id=project_id, output_path=output_path,
                                     overwrite=overwrite, source=source))
