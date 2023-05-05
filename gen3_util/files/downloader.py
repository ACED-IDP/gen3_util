from gen3_util.config import Config
from gen3_util.util import print_formatted


def cp(config: Config):
    """Copy files from bucket"""
    print_formatted(config, {'msg': 'file download progress goes here'})
