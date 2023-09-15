import pytest


@pytest.fixture
def dependency() -> str:
    """Include fixtures here."""
    return "TODO"


@pytest.fixture
def data_bucket() -> str:
    """Expected data bucket, keep in sync with development.aced-idp.org."""
    return "aced-development-ohsu-data-bucket"  # "aced-ohsu-development"


@pytest.fixture
def program() -> str:
    """program to use for testing projects i.e. program-projects"""
    return "test"
