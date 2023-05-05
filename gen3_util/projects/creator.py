from gen3_util.config import Config
from gen3_util.util import print_formatted


def touch(config: Config):
    """Create projects."""
    print_formatted(config, {'msg': 'Project creation goes here'})
