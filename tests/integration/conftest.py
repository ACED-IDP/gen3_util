import pytest


@pytest.fixture
def dependency() -> str:
    """Include fixtures here."""
    return "TODO"


@pytest.fixture
def profile() -> str:
    """gen3-client profile to use for testing."""
    return "aced-training"


@pytest.fixture
def program() -> str:
    """program to use for testing projects i.e. program-projects"""
    return "test"
