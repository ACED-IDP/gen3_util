from gen3_util.common import print_formatted
from gen3_util.config import Config


def ls(config: Config):
    """List files."""
    print_formatted(config, {'msg': 'file listing goes here'})
