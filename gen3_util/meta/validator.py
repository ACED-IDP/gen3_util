from gen3_util.config import Config
from gen3_util.util import print_formatted


def validate(config: Config):
    """Validate metadata."""
    print_formatted(config, {'msg': 'validate goes here'})
