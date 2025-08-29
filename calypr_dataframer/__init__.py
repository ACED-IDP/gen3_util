"""Calypr Dataframer - FHIR metadata dataframe generation utilities."""

__version__ = "0.1.0"
__author__ = "Calypr Team"
__description__ = "A tool for generating dataframes from FHIR metadata"

# Core namespace UUID for deterministic ID generation
import uuid
CALYPR_NAMESPACE = uuid.uuid3(uuid.NAMESPACE_DNS, b'calypr.com')