from gen3_util.util import print_formatted
from gen3_util.config import Config


def cp(config: Config):
    """Copy files to bucket"""
    print_formatted(config, {'msg': 'file upload progress goes here'})
