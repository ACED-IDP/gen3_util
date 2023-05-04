from gen3_util.config import Config
from gen3_util.util import print_formatted


def cp(config: Config):
    """Copy meta from bucket"""
    print_formatted(config, {'msg': 'meta download progress goes here'})
