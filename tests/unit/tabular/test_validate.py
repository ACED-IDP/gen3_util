
from gen3_util.meta.tabular import validate


def test_validate_simple(specimen: dict):
    """Should validate."""
    result = validate(specimen)
    assert result.exception is None


def test_validate_no_resource_type(specimen: dict):
    """Should not validate."""
    del specimen['resourceType']
    result = validate(specimen)
    assert result.exception is not None
    assert 'resourceType' in str(result.exception)


def test_validate_no_bad_date_time(specimen: dict):
    """Should not validate."""
    specimen['receivedTime'] = '1234ABCD'
    result = validate(specimen)
    assert result.exception is not None
    assert 'receivedTime' in str(result.exception)
    assert 'invalid datetime format' in str(result.exception)
