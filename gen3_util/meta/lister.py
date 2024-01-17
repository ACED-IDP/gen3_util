from gen3.index import Gen3Index
from gen3.submission import Gen3Submission

from gen3_util.config import Config, ensure_auth


def ls(config: Config, auth=None):
    """List meta."""
    if not auth:
        auth = ensure_auth(profile=config.gen3.profile)
    index_client = Gen3Index(auth_provider=auth)
    records = index_client.client.list_with_params(limit=1000, params={'metadata': {'is_metadata': 'true'}})
    return {'records': [_.to_json() for _ in records]}


def counts(config: Config, auth=None):
    """Count meta."""
    if not auth:
        auth = ensure_auth(profile=config.gen3.profile)

    submission_client = Gen3Submission(auth)
    project_id = config.gen3.project_id

    query = """
    {
        Task: _task_count(project_id: "PROJECT_ID")
        Patient: _patient_count(project_id: "PROJECT_ID")
        Specimen: _specimen_count(project_id: "PROJECT_ID")
        Substance: _substance_count(project_id: "PROJECT_ID")
        MedicationAdministration: _medication_administration_count(project_id: "PROJECT_ID")
        ResearchStudy: _research_study_count(project_id: "PROJECT_ID")
        ResearchSubject: _research_subject_count(project_id: "PROJECT_ID")
        DocumentReference: _document_reference_count(project_id: "PROJECT_ID")
        Observation: _observation_count(project_id: "PROJECT_ID")
        Condition: _condition_count(project_id: "PROJECT_ID")
        Medication: _medication_count(project_id: "PROJECT_ID")
        FamilyMemberHistory: _family_member_history_count(project_id: "PROJECT_ID")
    }
    """.replace('PROJECT_ID', project_id)

    records = submission_client.query(query)['data']
    return {'resource_counts': records}
