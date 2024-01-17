import os

import pytest
from gen3.submission import Gen3Submission

import gen3_util
from gen3_util import Config
from gen3_util.config import ensure_auth


@pytest.fixture
def dependency() -> str:
    """Include fixtures here."""
    return "TODO"


@pytest.fixture
def profile() -> str:
    """gen3-client profile to use for testing."""
    if 'GEN3_UTIL_PROFILE' in os.environ:
        return os.environ['GEN3_UTIL_PROFILE']
    print("GEN3_UTIL_PROFILE not set, using profile 'local'")
    return "local"


@pytest.fixture
def program() -> str:
    """program to use for testing projects i.e. program-projects"""
    return "test"


@pytest.fixture
def submission_client() -> str:
    """Gen3Submission client"""
    auth = ensure_auth(profile='development')
    return Gen3Submission(auth_provider=auth)


@pytest.fixture
def config(profile) -> Config:
    """A config"""
    _ = gen3_util.config.default()
    _.gen3.profile = profile
    return _
