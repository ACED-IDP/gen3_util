"""Tests for the entities module."""

import pytest
from calypr_dataframer.entities import (
    SimplifiedResource,
    get_nested_value,
    normalize_coding,
    normalize_value,
    validate_and_transform_graphql_field_name
)


def test_get_nested_value():
    """Test nested value extraction."""
    data = {"a": {"b": {"c": "value"}}}
    assert get_nested_value(data, ["a", "b", "c"]) == "value"
    assert get_nested_value(data, ["a", "b", "d"]) is None
    assert get_nested_value(data, ["x"]) is None


def test_validate_and_transform_graphql_field_name():
    """Test GraphQL field name validation and transformation."""
    assert validate_and_transform_graphql_field_name("valid_field") == "valid_field"
    assert validate_and_transform_graphql_field_name("123invalid") == "_123invalid"
    assert validate_and_transform_graphql_field_name("field-with-hyphens") == "field_with_hyphens"
    assert validate_and_transform_graphql_field_name("") == "_"


def test_normalize_value():
    """Test value normalization from FHIR value[x] pattern."""
    # String value
    resource = {"valueString": "test value"}
    value, source = normalize_value(resource)
    assert value == "test value"
    assert source == "valueString"
    
    # No value
    resource = {"id": "test"}
    value, source = normalize_value(resource)
    assert value is None
    assert source == ""


def test_simplified_resource():
    """Test SimplifiedResource functionality."""
    resource = {
        "id": "test-id",
        "resourceType": "Patient",
        "identifier": [{"value": "12345"}],
        "name": [{"family": "Doe", "given": ["John"]}],
        "active": True
    }
    
    simplified = SimplifiedResource.build(resource)
    result = simplified.simplified
    
    assert result["id"] == "test-id"
    assert result["resourceType"] == "Patient"
    assert result["identifier"] == "12345"
    assert result["active"] is True