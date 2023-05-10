from gen3_util.common import print_formatted
from gen3_util.config import Config


def ls(config: Config):
    """List meta."""
    print_formatted(config, {'msg': 'meta listing goes here'})
