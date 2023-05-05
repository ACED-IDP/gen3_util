from gen3_util.config import Config
from gen3_util.util import print_formatted


def ls(config: Config):
    """List projects."""
    print_formatted(config, {'msg': 'Project listing goes here'})
