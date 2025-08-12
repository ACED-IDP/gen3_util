

def test_none():
    """Test None fields."""
    from fhir.resources.patient import Patient
    patient_dict = {"multipleBirthInteger": None, "name": None}
    patient = Patient.validate(patient_dict)
    assert patient
    patient_json = patient.dict()
    patient2 = Patient.validate(patient_json)
    assert patient2 == patient and patient2.json() == patient.json()
