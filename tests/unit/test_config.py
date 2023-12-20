from gen3_util import Config
from gen3_util.config import default, custom


def _assert_config_ok(config: Config):
    assert config, "must have config"
    assert config.log, "must have config.log"
    assert config.log.format, "must have config.log.format"


def test_config():
    """Ensure we can read default config bundled with package."""
    _assert_config_ok(default())


def test_custom_config(custom_config_path):
    """Ensure we can read config from arbitrary path."""
    _assert_config_ok(custom(custom_config_path))
