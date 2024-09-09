import json
import os
import pytest
import tempfile


from gen3_tracker.common import read_ndjson_file
from gen3_tracker.meta.dataframer import LocalFHIRDatabase
from gen3_tracker.meta.entities import SimplifiedResource
from pathlib import Path

############
# FIXTURES #
############


@pytest.fixture()
def document_reference_key():
    return "DocumentReference/9ae7e542-767f-4b03-a854-7ceed17152cb"


@pytest.fixture()
def specimen_key():
    return "Specimen/60c67a06-ea2d-4d24-9249-418dc77a16a9"


@pytest.fixture()
def patient_key():
    return "Patient/bc4e1aa6-cb52-40e9-8f20-594d9c84f920"


@pytest.fixture()
def research_subject_key():
    return "ResearchSubject/2fc448d6-a23b-4b94-974b-c66110164851"


@pytest.fixture()
def simplified_resources(
    document_reference_key, specimen_key, patient_key, research_subject_key
):
    return {
        document_reference_key: {
            "identifier": "9ae7e542-767f-4b03-a854-7ceed17152cb",
            "resourceType": "DocumentReference",
            "id": "9ae7e542-767f-4b03-a854-7ceed17152cb",
            "status": "current",
            "docStatus": "final",
            "date": "2024-08-21T10:53:00+00:00",
            "md5": "227f0a5379362d42eaa1814cfc0101b8",
            "source_path": "file:///home/LabA/specimen_1234_labA.fq.gz",
            "contentType": "text/fastq",
            "size": 5595609484,
            "url": "file:///home/LabA/specimen_1234_labA.fq.gz",
            "title": "specimen_1234_labA.fq.gz",
            "creation": "2024-08-21T10:53:00+00:00",
        },
        specimen_key: {
            "identifier": "specimen_1234_labA",
            "resourceType": "Specimen",
            "id": "60c67a06-ea2d-4d24-9249-418dc77a16a9",
            "collection": "Breast",
            "processing": "Double-Spun",
        },
        "Observation/cec32723-9ede-5f24-ba63-63cb8c6a02cf": {
            "identifier": "patientX_1234-9ae7e542-767f-4b03-a854-7ceed17152cb-sequencer",
            "resourceType": "Observation",
            "id": "cec32723-9ede-5f24-ba63-63cb8c6a02cf",
            "status": "final",
            "category": "Laboratory",
            "sequencer": "Illumina Seq 1000",
            "index": "100bp Single index",
            "type": "Exome",
            "project_id": "labA_projectXYZ",
            "read_length": "100",
            "instrument_run_id": "234_ABC_1_8899",
            "capture_bait_set": "Human Exom 2X",
            "end_type": "Paired-End",
            "capture": "emitter XT",
            "sequencing_site": "AdvancedGeneExom",
            "construction": "library_construction",
        },
        "Observation/4e3c6b59-b1fd-5c26-a611-da4cde9fd061": {
            "identifier": "patientX_1234-specimen_1234_labA-sample_type",
            "resourceType": "Observation",
            "id": "4e3c6b59-b1fd-5c26-a611-da4cde9fd061",
            "status": "final",
            "category": "Laboratory",
            "sample_type": "Primary Solid Tumor",
            "library_id": "12345",
            "tissue_type": "Tumor",
            "treatments": "Trastuzumab",
            "allocated_for_site": "TEST Clinical Research",
            "indexed_collection_date": "365",
            "biopsy_specimens": "specimenA, specimenB, specimenC",
            "biopsy_procedure_type": "Biopsy - Core",
            "biopsy_anatomical_location": "top axillary lymph node",
            "percent_tumor": "30",
        },
        "Observation/21f3411d-89a4-4bcc-9ce7-b76edb1c745f": {
            "identifier": "patientX_1234-9ae7e542-767f-4b03-a854-7ceed17152cb-Gene",
            "resourceType": "Observation",
            "id": "21f3411d-89a4-4bcc-9ce7-b76edb1c745f",
            "status": "final",
            "category": "Laboratory",
            "Gene": "TP53",
            "Chromosome": "chr17",
            "result": "gain of function (GOF)",
        },
        "ResearchStudy/7dacd4d0-3c8e-470b-bf61-103891627d45": {
            "identifier": "labA",
            "resourceType": "ResearchStudy",
            "id": "7dacd4d0-3c8e-470b-bf61-103891627d45",
            "name": "LabA",
            "status": "active",
            "description": "LabA Clinical Trial Study: FHIR Schema Chorot Integration",
        },
        research_subject_key: {
            "identifier": "subjectX_1234",
            "resourceType": "ResearchSubject",
            "id": "2fc448d6-a23b-4b94-974b-c66110164851",
            "status": "active",
        },
        "Organization/89c8dc4c-2d9c-48c7-8862-241a49a78f14": {
            "identifier": "LabA_ORGANIZATION",
            "resourceType": "Organization",
            "id": "89c8dc4c-2d9c-48c7-8862-241a49a78f14",
            "type": "Educational Institute",
        },
        patient_key: {
            "identifier": "patientX_1234",
            "resourceType": "Patient",
            "id": "bc4e1aa6-cb52-40e9-8f20-594d9c84f920",
            "active": True,
        },
    }


@pytest.fixture()
def fixture_path(data_path: Path) -> Path:
    return data_path / "fhir-compbio-examples/META"


@pytest.fixture()
def local_db(fixture_path: Path) -> LocalFHIRDatabase:
    """Load a local db with smmart data fixture."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # print(f"Temporary directory created at: {temp_dir}")
        db = LocalFHIRDatabase(db_name=os.path.join(temp_dir, "local.db"))

        assert (
            fixture_path.exists()
        ), f"Fixture path {fixture_path.absolute()} does not exist."
        for file in fixture_path.glob("*.ndjson"):
            # print(f"Loading {file}")
            for resource in read_ndjson_file(str(file)):
                db.insert_data_from_dict(resource)
        return db


@pytest.fixture()
def expected_keys(simplified_resources):
    return sorted(list(simplified_resources.keys()))


@pytest.fixture()
def resources(local_db):
    cursor = local_db.connection.cursor()
    cursor.execute("SELECT * FROM resources")
    resources = cursor.fetchall()
    _resources = []
    for row in resources:
        _, _, resource = row
        resource = json.loads(resource)
        _resources.append(resource)
    return _resources


##############
# DATAFRAMES #
##############


@pytest.fixture()
def docref_row(simplified_resources, document_reference_key):
    """Based on metadata files, create expected DocumentReference row, populated with any Observations that focus on it"""
    return {
        **simplified_resources[document_reference_key],
        "sequencer": "Illumina Seq 1000",
        "index": "100bp Single index",
        "type": "Exome",
        "project_id": "labA_projectXYZ",
        "read_length": "100",
        "instrument_run_id": "234_ABC_1_8899",
        "capture_bait_set": "Human Exom 2X",
        "end_type": "Paired-End",
        "capture": "emitter XT",
        "sequencing_site": "AdvancedGeneExom",
        "construction": "library_construction",
        "Gene": "TP53",
        "Chromosome": "chr17",
        "result": "gain of function (GOF)",
        "specimen_collection": "Breast",
        "specimen_id": "60c67a06-ea2d-4d24-9249-418dc77a16a9",
        "specimen_identifier": "specimen_1234_labA",
        "specimen_processing": "Double-Spun",
    }


@pytest.fixture()
def research_subject_row(simplified_resources, research_subject_key):
    """Based on metadata files, create an expected Observations dataframe"""
    return {
        **simplified_resources[research_subject_key],
        "study": "7dacd4d0-3c8e-470b-bf61-103891627d45",
        "subject_id": "bc4e1aa6-cb52-40e9-8f20-594d9c84f920",
        "subject_type": "Patient",
    }


@pytest.fixture()
def specimen_row(simplified_resources, specimen_key):
    return {
        **simplified_resources[specimen_key],
        "sample_type": "Primary Solid Tumor",
        "library_id": "12345",
        "tissue_type": "Tumor",
        "treatments": "Trastuzumab",
        "allocated_for_site": "TEST Clinical Research",
        "indexed_collection_date": "365",
        "biopsy_specimens": "specimenA, specimenB, specimenC",
        "biopsy_procedure_type": "Biopsy - Core",
        "biopsy_anatomical_location": "top axillary lymph node",
        "percent_tumor": "30",
        "patient_identifier": "patientX_1234",
        "patient_id": "bc4e1aa6-cb52-40e9-8f20-594d9c84f920",
        "patient_active": True,
    }


#########
# TESTS #
#########


def test_db(local_db, expected_keys):
    """Simple test to verify db load."""
    assert local_db
    cursor = local_db.connection.cursor()
    cursor.execute("SELECT * FROM resources")
    resources = cursor.fetchall()
    actual_keys = []
    for resource in resources:
        key, _, _ = resource
        actual_keys.append(key)
    actual_keys = sorted(actual_keys)
    print(actual_keys)
    assert actual_keys == expected_keys


def test_simplified(resources, simplified_resources):
    """Simple test to verify resources are simplified (no joins)."""
    actual = {}
    for resource in resources:
        assert isinstance(
            resource, dict
        ), f"Expected dict, got {type(resource)} {resource}"
        simplified = SimplifiedResource.build(resource=resource).simplified
        actual[f"{simplified['resourceType']}/{simplified['id']}"] = simplified

    assert actual == simplified_resources


def test_flattened_document_references(local_db, docref_row):
    """Test the dataframer using a local database with a SMMART bundle,
    this test ensures the  DocumentReference is populated with fields from any Observation with a focus on this DocumentReference
    """

    # get the singular test document reference
    doc_refs = [d for d in local_db.flattened_document_references()]
    doc_ref = doc_refs[0]

    assert doc_ref == docref_row


def test_flattened_specimens(local_db, specimen_row):
    """Test that the Specimen metadata is populated with the correct Observation codes through Observation.focus"""

    # get the singular test specimen
    specimens = [s for s in local_db.flattened_specimens()]
    specimen = specimens[0]

    assert specimen == specimen_row


def test_flattened_research_subjects(local_db, research_subject_row):
    """Test that the Research Subject metadata is populated with the correct Patient information"""

    local_db.connect()

    # get the singular test research subject
    research_subjects = [rs for rs in local_db.flattened_research_subjects()]
    research_subject = research_subjects[0]

    assert research_subject == research_subject_row
