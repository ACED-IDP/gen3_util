from gen3_util.config import Config
from gen3_util.util import print_formatted


def rm(config: Config):
    """Remove project."""
    print_formatted(config, {'msg': 'Project removal message goes here'})
