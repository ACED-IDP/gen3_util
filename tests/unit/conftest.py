from typing import List

import pytest
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission


@pytest.fixture
def python_source_directories() -> List[str]:
    """Directories to scan with flake8."""
    return ["gen3_util", "tests"]


@pytest.fixture
def custom_config_path() -> str:
    """User specified config path"""
    return 'tests/fixtures/custom_config/config.yaml'


@pytest.fixture
def submission_client() -> str:
    """Gen3Submission client"""
    return Gen3Submission(auth_provider=Gen3Auth())
