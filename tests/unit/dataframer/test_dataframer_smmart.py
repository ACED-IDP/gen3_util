import inflection
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
def simplified_smmart_resources():
    return {
        "DocumentReference/9ae7e542-767f-4b03-a854-7ceed17152cb": {
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
        "Specimen/60c67a06-ea2d-4d24-9249-418dc77a16a9": {
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
        "ResearchSubject/2fc448d6-a23b-4b94-974b-c66110164851": {
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
        "Patient/bc4e1aa6-cb52-40e9-8f20-594d9c84f920": {
            "identifier": "patientX_1234",
            "resourceType": "Patient",
            "id": "bc4e1aa6-cb52-40e9-8f20-594d9c84f920",
            "active": True,
        },
    }


@pytest.fixture()
def smmart_fixture_path(data_path: Path) -> Path:
    return data_path / "fhir-compbio-examples/META"


@pytest.fixture()
def smmart_local_db(smmart_fixture_path: Path) -> LocalFHIRDatabase:
    """Load a local db with smmart data fixture."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # print(f"Temporary directory created at: {temp_dir}")
        db = LocalFHIRDatabase(db_name=os.path.join(temp_dir, "local.db"))

        assert (
            smmart_fixture_path.exists()
        ), f"Fixture path {smmart_fixture_path.absolute()} does not exist."
        for file in smmart_fixture_path.glob("*.ndjson"):
            # print(f"Loading {file}")
            for resource in read_ndjson_file(str(file)):
                db.insert_data_from_dict(resource)
        yield db


@pytest.fixture()
def smmart_docref_dataframe():
    """Create user-defined dataframe/row with Observations (and user-defined nomenclature) that have focus/association to this DocumentReference instance"""
    return {
        "identifier": "9ae7e542-767f-4b03-a854-7ceed17152cb",
        "resourceType": "DocumentReference",
        "id": "9ae7e542-767f-4b03-a854-7ceed17152cb",
        "status": "current",
        "docStatus": "final",
        "date": "2024-08-21T10:53:00+00:00",
        "md5": "227f0a5379362d42eaa1814cfc0101b8",
        "source_path": "file:///home/LabA/specimen_1234_labA.fq.gz",
        "contentType": "text/fastq",
        "url": "file:///home/LabA/specimen_1234_labA.fq.gz",
        "size": 5595609484,
        "title": "specimen_1234_labA.fq.gz",
        "creation": "2024-08-21T10:53:00+00:00",
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
def smmart_observation_dataframes():
    """Create user-defined dataframe/row with Observations (and user-defined nomenclature) that have focus/association to this DocumentReference instance"""
    return [
        {
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
            "documentreference_identifier": "9ae7e542-767f-4b03-a854-7ceed17152cb",
            "documentreference_id": "9ae7e542-767f-4b03-a854-7ceed17152cb",
            "documentreference_status": "current",
            "documentreference_docStatus": "final",
            "documentreference_date": "2024-08-21T10:53:00+00:00",
            "documentreference_contentType": "text/fastq",
            "documentreference_url": "file:///home/LabA/specimen_1234_labA.fq.gz",
            "documentreference_size": 5595609484,
            "documentreference_title": "specimen_1234_labA.fq.gz",
            "documentreference_creation": "2024-08-21T10:53:00+00:00",
            "documentreference_md5": "227f0a5379362d42eaa1814cfc0101b8",
            "documentreference_source_path": "file:///home/LabA/specimen_1234_labA.fq.gz",
            "patient_identifier": "patientX_1234",
            "patient_id": "bc4e1aa6-cb52-40e9-8f20-594d9c84f920",
            "patient_active": True,
        },
        {
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
            "specimen_identifier": "specimen_1234_labA",
            "specimen_id": "60c67a06-ea2d-4d24-9249-418dc77a16a9",
            "specimen_collection": "Breast",
            "specimen_processing": "Double-Spun",
            "patient_identifier": "patientX_1234",
            "patient_id": "bc4e1aa6-cb52-40e9-8f20-594d9c84f920",
            "patient_active": True,
        },
        {
            "identifier": "patientX_1234-9ae7e542-767f-4b03-a854-7ceed17152cb-Gene",
            "resourceType": "Observation",
            "id": "21f3411d-89a4-4bcc-9ce7-b76edb1c745f",
            "status": "final",
            "category": "Laboratory",
            "Gene": "TP53",
            "Chromosome": "chr17",
            "result": "gain of function (GOF)",
            "documentreference_identifier": "9ae7e542-767f-4b03-a854-7ceed17152cb",
            "documentreference_id": "9ae7e542-767f-4b03-a854-7ceed17152cb",
            "documentreference_status": "current",
            "documentreference_docStatus": "final",
            "documentreference_date": "2024-08-21T10:53:00+00:00",
            "documentreference_contentType": "text/fastq",
            "documentreference_url": "file:///home/LabA/specimen_1234_labA.fq.gz",
            "documentreference_size": 5595609484,
            "documentreference_title": "specimen_1234_labA.fq.gz",
            "documentreference_creation": "2024-08-21T10:53:00+00:00",
            "documentreference_md5": "227f0a5379362d42eaa1814cfc0101b8",
            "documentreference_source_path": "file:///home/LabA/specimen_1234_labA.fq.gz",
            "patient_identifier": "patientX_1234",
            "patient_id": "bc4e1aa6-cb52-40e9-8f20-594d9c84f920",
            "patient_active": True,
        },
    ]


@pytest.fixture()
def expected_keys(simplified_smmart_resources):
    return sorted(list(simplified_smmart_resources.keys()))


@pytest.fixture()
def smmart_resources(smmart_local_db):
    cursor = smmart_local_db.connection.cursor()
    cursor.execute("SELECT * FROM resources")
    resources = cursor.fetchall()
    _resources = []
    for row in resources:
        _, _, resource = row
        resource = json.loads(resource)
        _resources.append(resource)
    return _resources


#########
# TESTS #
#########


def test_smmart_db(smmart_local_db, expected_keys):
    """Simple test to verify db load."""
    assert smmart_local_db
    cursor = smmart_local_db.connection.cursor()
    cursor.execute("SELECT * FROM resources")
    resources = cursor.fetchall()
    actual_keys = []
    for resource in resources:
        key, _, _ = resource
        actual_keys.append(key)
    actual_keys = sorted(actual_keys)
    print(actual_keys)
    assert actual_keys == expected_keys


def test_simplified(smmart_resources, simplified_smmart_resources):
    """Simple test to verify resources are simplified (no joins)."""
    actual = {}
    for resource in smmart_resources:
        assert isinstance(
            resource, dict
        ), f"Expected dict, got {type(resource)} {resource}"
        simplified = SimplifiedResource.build(resource=resource).simplified
        actual[f"{simplified['resourceType']}/{simplified['id']}"] = simplified
    print(actual)
    assert actual == simplified_smmart_resources


def test_docref_with_observation_focus(smmart_local_db, smmart_docref_dataframe):
    """Test the dataframer using a local database with a SMMART bundle, this test ensures document reference and all its Observations."""
    doc_ref_generator = smmart_local_db.flattened_document_references()
    doc_refs = [d for d in doc_ref_generator]
    doc_ref = doc_refs[0]

    print("type(doc_refs):", type(doc_refs))
    print("type(doc_ref):", type(doc_ref))

    assert "specimen_collection" in doc_ref, doc_ref
    assert doc_ref["specimen_identifier"] == "specimen_1234_labA", doc_ref
    assert doc_ref["specimen_collection"] == "Breast", doc_ref

    print("final dataframe:", doc_ref)
    assert doc_ref == smmart_docref_dataframe


def test_observations(
    smmart_fixture_path, smmart_local_db, smmart_observation_dataframes
):
    # check metadata length is the same as number of dataframes in test fixture
    with open(smmart_fixture_path / "Observation.ndjson") as file:
        num_observation = len([1 for line in file if line.strip()])
    assert (
        len(smmart_observation_dataframes) == num_observation
    ), "observation ndjson metadata and expected observation dataframes are not the same length, check that the fixture have the same number of rows as the metadata"

    # test contents of flattener
    actual_dataframes = smmart_local_db.flattened_observations()
    for expected, actual in zip(smmart_observation_dataframes, actual_dataframes):
        assert (
            expected == actual
        ), f"Observation differs than expected, use pytest -vv flag for diff"
