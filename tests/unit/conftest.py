from typing import List

import pytest


@pytest.fixture
def python_source_directories() -> List[str]:
    """Directories to scan with flake8."""
    return ["gen3_util", "tests"]


@pytest.fixture
def custom_config_path() -> str:
    """User specified config path"""
    return 'tests/fixtures/custom_config/config.yaml'
