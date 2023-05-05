from gen3_util.util import print_formatted
from gen3_util.config import Config


def rm(config: Config):
    """Remove meta."""
    print_formatted(config, {'msg': 'meta removal message goes here'})
