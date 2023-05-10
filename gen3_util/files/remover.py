from gen3_util.common import print_formatted
from gen3_util.config import Config


def rm(config: Config):
    """Remove files."""
    print_formatted(config, {'msg': 'file removal message goes here'})
