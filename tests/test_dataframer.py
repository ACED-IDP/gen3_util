"""Tests for the dataframer module."""

import json
import tempfile
import pytest
from pathlib import Path

from calypr_dataframer.dataframer import LocalFHIRDatabase, create_dataframe


@pytest.fixture
def sample_patient():
    """Sample patient resource."""
    return {
        "id": "patient-1",
        "resourceType": "Patient",
        "identifier": [{"value": "P001"}],
        "name": [{"family": "Doe", "given": ["John"]}],
        "active": True
    }


@pytest.fixture
def sample_document_reference():
    """Sample document reference resource."""
    return {
        "id": "doc-1",
        "resourceType": "DocumentReference",
        "identifier": [{"value": "DOC001"}],
        "status": "current",
        "subject": {"reference": "Patient/patient-1"},
        "content": [{
            "attachment": {
                "contentType": "application/pdf",
                "title": "Test Document"
            }
        }]
    }


@pytest.fixture
def temp_meta_dir(sample_patient, sample_document_reference):
    """Create a temporary META directory with test data."""
    with tempfile.TemporaryDirectory() as temp_dir:
        meta_path = Path(temp_dir) / "META"
        meta_path.mkdir()
        
        # Write patient data
        with open(meta_path / "Patient.ndjson", "w") as f:
            f.write(json.dumps(sample_patient) + "\n")
        
        # Write document reference data  
        with open(meta_path / "DocumentReference.ndjson", "w") as f:
            f.write(json.dumps(sample_document_reference) + "\n")
        
        yield str(meta_path)


def test_local_fhir_database_creation():
    """Test LocalFHIRDatabase creation and table setup."""
    with tempfile.NamedTemporaryFile(suffix=".db") as temp_db:
        db = LocalFHIRDatabase(temp_db.name)
        db.create_table()
        assert "resources" in db.table_created
        db.disconnect()


def test_data_insertion(sample_patient):
    """Test inserting data into the database."""
    with tempfile.NamedTemporaryFile(suffix=".db") as temp_db:
        db = LocalFHIRDatabase(temp_db.name)
        db.insert_data_from_dict(sample_patient)
        
        # Verify data was inserted
        cursor = db.connect()
        cursor.execute("SELECT COUNT(*) FROM resources")
        count = cursor.fetchone()[0]
        assert count == 1
        
        db.disconnect()


def test_create_dataframe(temp_meta_dir):
    """Test dataframe creation from FHIR data."""
    with tempfile.TemporaryDirectory() as work_dir:
        df = create_dataframe(temp_meta_dir, work_dir, "DocumentReference")
        
        assert not df.empty
        assert "id" in df.columns
        assert "resourceType" in df.columns
        assert len(df) == 1