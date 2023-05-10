from gen3_util.common import print_formatted
from gen3_util.config import Config


def cp(config: Config):
    """Copy meta to bucket"""
    print_formatted(config, {'msg': 'meta upload progress goes here'})
