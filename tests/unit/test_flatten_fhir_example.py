from datetime import datetime, date
from urllib.parse import urlparse

import pytest
from fhir.resources.coding import Coding
from fhir.resources.domainresource import DomainResource
from fhir.resources.identifier import Identifier
from fhir.resources.observation import Observation
from fhir.resources.patient import Patient
from fhir.resources.reference import Reference
from fhir.resources.specimen import Specimen


# Implement "flatten" by monkey patching the DomainResource class.
# Specialize the flatten method for Observation.

#            __,__
#   .--.  .-"     "-.  .--.
#  / .. \/  .-. .-.  \/ .. \
# | |  '|  /   Y   \  |'  | |
# | \   \  \ 0 | 0 /  /   / |
#  \ '- ,\.-"`` ``"-./, -' /
#   `'-' /_   ^ ^   _\ '-'`
#       |  \._   _./  |
#       \   \ `~` /   /
#        '._ '-=-' _.'
#           '~---~'

# test data ------------------------------------------------------------
# The following fixtures provide test data for the tests below.

@pytest.fixture
def patient_dict() -> dict:
    # TODO - read the patient example from a file
    patient_dict = {"resourceType": "Patient", "id": "3", "meta": {"lastUpdated": "2012-05-29T23:45:32Z"},
                    "text": {"status": "generated",
                             "div": "\u003cdiv xmlns\u003d\"http://www.w3.org/1999/xhtml\"\u003eKidd, Kari. SSN:\n            444555555\u003c/div\u003e"},
                    "identifier": [{"type": {
                        "coding": [{"system": "http://terminology.hl7.org/CodeSystem/v2-0203", "code": "SS"}]},
                                    "system": "http://hl7.org/fhir/sid/us-ssn", "value": "444555555"}], "active": True,
                    "name": [{"use": "official", "family": "Kidd", "given": ["Kari"]}],
                    "telecom": [{"system": "phone", "value": "555-555-2005", "use": "work"}], "gender": "female",
                    "address": [{"use": "home", "line": ["2222 Home Street"]}],
                    "managingOrganization": {"reference": "Organization/hl7"}}
    yield patient_dict


@pytest.fixture
def specimen_dict():
    return {"resourceType": "Specimen", "id": "denovo-3", "text": {"status": "generated",
                                                                   "div": "\u003cdiv xmlns\u003d\"http://www.w3.org/1999/xhtml\"\u003e\u003cp\u003e\u003cb\u003eGenerated Narrative\u003c/b\u003e\u003c/p\u003e\u003cdiv style\u003d\"display: inline-block; background-color: #d9e0e7; padding: 6px; margin: 4px; border: 1px solid #8da1b4; border-radius: 5px; line-height: 60%\"\u003e\u003cp style\u003d\"margin-bottom: 0px\"\u003eResource \u0026quot;denovo-3\u0026quot; \u003c/p\u003e\u003c/div\u003e\u003cp\u003e\u003cb\u003eidentifier\u003c/b\u003e: id: 3\u003c/p\u003e\u003cp\u003e\u003cb\u003estatus\u003c/b\u003e: available\u003c/p\u003e\u003cp\u003e\u003cb\u003etype\u003c/b\u003e: Venous blood specimen \u003cspan style\u003d\"background: LightGoldenRodYellow; margin: 4px; border: 1px solid khaki\"\u003e (\u003ca href\u003d\"https://browser.ihtsdotools.org/\"\u003eSNOMED CT\u003c/a\u003e#122555007)\u003c/span\u003e\u003c/p\u003e\u003cp\u003e\u003cb\u003esubject\u003c/b\u003e: \u003ca href\u003d\"Patient-denovoFather.html\"\u003ePatient/denovoFather: John Doe\u003c/a\u003e \u0026quot; DOE\u0026quot;\u003c/p\u003e\u003cp\u003e\u003cb\u003ereceivedTime\u003c/b\u003e: 2021-01-01 01:01:01+0000\u003c/p\u003e\u003cp\u003e\u003cb\u003erequest\u003c/b\u003e: \u003ca href\u003d\"ServiceRequest-genomicServiceRequest.html\"\u003eServiceRequest/genomicServiceRequest\u003c/a\u003e\u003c/p\u003e\u003ch3\u003eCollections\u003c/h3\u003e\u003ctable class\u003d\"grid\"\u003e\u003ctr\u003e\u003ctd\u003e-\u003c/td\u003e\u003ctd\u003e\u003cb\u003eCollector\u003c/b\u003e\u003c/td\u003e\u003ctd\u003e\u003cb\u003eCollected[x]\u003c/b\u003e\u003c/td\u003e\u003ctd\u003e\u003cb\u003eQuantity\u003c/b\u003e\u003c/td\u003e\u003ctd\u003e\u003cb\u003eMethod\u003c/b\u003e\u003c/td\u003e\u003c/tr\u003e\u003ctr\u003e\u003ctd\u003e*\u003c/td\u003e\u003ctd\u003e\u003ca href\u003d\"Practitioner-practitioner01.html\"\u003ePractitioner/practitioner01\u003c/a\u003e \u0026quot; DOEL\u0026quot;\u003c/td\u003e\u003ctd\u003e2021-01-01 01:01:00+0000\u003c/td\u003e\u003ctd\u003e1 mL\u003c/td\u003e\u003ctd\u003eLine, Venous \u003cspan style\u003d\"background: LightGoldenRodYellow; margin: 4px; border: 1px solid khaki\"\u003e (\u003ca href\u003d\"http://terminology.hl7.org/3.1.0/CodeSystem-v2-0488.html\"\u003especimenCollectionMethod\u003c/a\u003e#LNV)\u003c/span\u003e\u003c/td\u003e\u003c/tr\u003e\u003c/table\u003e\u003c/div\u003e"},
            "identifier": [{"system": "http://www.somesystemabc.net/identifiers/specimens", "value": "3"}],
            "status": "available", "type": {
            "coding": [{"system": "http://snomed.info/sct", "code": "122555007", "display": "Venous blood specimen"}]},
            "subject": {"reference": "Patient/denovoFather", "display": "John Doe"},
            "receivedTime": "2021-01-01T01:01:01Z", "request": [{"reference": "ServiceRequest/genomicServiceRequest"}],
            "collection": {"collector": {"reference": "Practitioner/practitioner01"},
                           "collectedDateTime": "2021-01-01T01:01:00Z", "quantity": {"value": 1, "unit": "mL"},
                           "method": {
                               "coding": [{"system": "http://terminology.hl7.org/CodeSystem/v2-0488", "code": "LNV"}]}}}


@pytest.fixture
def observation_eye_color_dict():
    return {"resourceType": "Observation", "id": "eye-color", "text": {"status": "generated",
                                                                       "div": "\u003cdiv xmlns\u003d\"http://www.w3.org/1999/xhtml\"\u003e\u003cp\u003e\u003cb\u003eGenerated Narrative: Observation\u003c/b\u003e\u003ca name\u003d\"eye-color\"\u003e \u003c/a\u003e\u003ca name\u003d\"hceye-color\"\u003e \u003c/a\u003e\u003c/p\u003e\u003cdiv style\u003d\"display: inline-block; background-color: #d9e0e7; padding: 6px; margin: 4px; border: 1px solid #8da1b4; border-radius: 5px; line-height: 60%\"\u003e\u003cp style\u003d\"margin-bottom: 0px\"\u003eResource Observation \u0026quot;eye-color\u0026quot; \u003c/p\u003e\u003c/div\u003e\u003cp\u003e\u003cb\u003estatus\u003c/b\u003e: final\u003c/p\u003e\u003cp\u003e\u003cb\u003ecode\u003c/b\u003e: eye color \u003cspan style\u003d\"background: LightGoldenRodYellow; margin: 4px; border: 1px solid khaki\"\u003e ()\u003c/span\u003e\u003c/p\u003e\u003cp\u003e\u003cb\u003esubject\u003c/b\u003e: \u003ca href\u003d\"patient-example.html\"\u003ePatient/example\u003c/a\u003e \u0026quot;Peter CHALMERS\u0026quot;\u003c/p\u003e\u003cp\u003e\u003cb\u003eeffective\u003c/b\u003e: 2016-05-18\u003c/p\u003e\u003cp\u003e\u003cb\u003evalue\u003c/b\u003e: blue\u003c/p\u003e\u003c/div\u003e"},
            "status": "final", "code": {"text": "eye color"}, "subject": {"reference": "Patient/example"},
            "effectiveDateTime": "2016-05-18", "valueString": "blue"}


@pytest.fixture
def observation_bmi_dict():
    return {"resourceType": "Observation", "id": "bmi-using-related", "text": {"status": "generated",
                                                                               "div": "\u003cdiv xmlns\u003d\"http://www.w3.org/1999/xhtml\"\u003e\u003cp\u003e\u003cb\u003eGenerated Narrative: Observation\u003c/b\u003e\u003ca name\u003d\"bmi-using-related\"\u003e \u003c/a\u003e\u003ca name\u003d\"hcbmi-using-related\"\u003e \u003c/a\u003e\u003c/p\u003e\u003cdiv style\u003d\"display: inline-block; background-color: #d9e0e7; padding: 6px; margin: 4px; border: 1px solid #8da1b4; border-radius: 5px; line-height: 60%\"\u003e\u003cp style\u003d\"margin-bottom: 0px\"\u003eResource Observation \u0026quot;bmi-using-related\u0026quot; \u003c/p\u003e\u003c/div\u003e\u003cp\u003e\u003cb\u003estatus\u003c/b\u003e: \u003cspan title\u003d\"  \u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d need to fix vitals to removed fixed value \u0027has-member\u0027 \u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\u003d\n\t\u0026lt;meta\u0026gt;\n\t\t\u0026lt;profile value\u003d\u0026quot;http://hl7.org/fhir/StructureDefinition/vitalsigns\u0026quot;/\u0026gt;\n\t\u0026lt;/meta\u0026gt;\n     \"\u003efinal\u003c/span\u003e\u003c/p\u003e\u003cp\u003e\u003cb\u003ecategory\u003c/b\u003e: Vital Signs \u003cspan style\u003d\"background: LightGoldenRodYellow; margin: 4px; border: 1px solid khaki\"\u003e (\u003ca href\u003d\"http://terminology.hl7.org/5.5.0/CodeSystem-observation-category.html\"\u003eObservation Category Codes\u003c/a\u003e#vital-signs)\u003c/span\u003e\u003c/p\u003e\u003cp\u003e\u003cb\u003ecode\u003c/b\u003e: BMI \u003cspan style\u003d\"background: LightGoldenRodYellow; margin: 4px; border: 1px solid khaki\"\u003e (\u003ca href\u003d\"https://loinc.org/\"\u003eLOINC\u003c/a\u003e#39156-5 \u0026quot;Body mass index (BMI) [Ratio]\u0026quot;)\u003c/span\u003e\u003c/p\u003e\u003cp\u003e\u003cb\u003esubject\u003c/b\u003e: \u003ca href\u003d\"patient-example.html\"\u003ePatient/example\u003c/a\u003e \u0026quot;Peter CHALMERS\u0026quot;\u003c/p\u003e\u003cp\u003e\u003cb\u003eeffective\u003c/b\u003e: 1999-07-02\u003c/p\u003e\u003cp\u003e\u003cb\u003evalue\u003c/b\u003e: 16.2 kg/m2\u003cspan style\u003d\"background: LightGoldenRodYellow\"\u003e (Details: UCUM code kg/m2 \u003d \u0027kg/m2\u0027)\u003c/span\u003e\u003c/p\u003e\u003cp\u003e\u003cb\u003ederivedFrom\u003c/b\u003e: \u003c/p\u003e\u003cul\u003e\u003cli\u003e\u003ca href\u003d\"broken-link.html\"\u003eObservation/bodyheight: Body Height\u003c/a\u003e\u003c/li\u003e\u003cli\u003e\u003ca href\u003d\"observation-example.html\"\u003eObservation/example: Body Weight\u003c/a\u003e\u003c/li\u003e\u003c/ul\u003e\u003c/div\u003e"},
            "status": "final", "category": [{"coding": [
            {"system": "http://terminology.hl7.org/CodeSystem/observation-category", "code": "vital-signs",
             "display": "Vital Signs"}], "text": "Vital Signs"}], "code": {
            "coding": [{"system": "http://loinc.org", "code": "39156-5", "display": "Body mass index (BMI) [Ratio]"}],
            "text": "BMI"}, "subject": {"reference": "Patient/example"}, "effectiveDateTime": "1999-07-02",
            "valueQuantity": {"value": 16.2, "unit": "kg/m2", "system": "http://unitsofmeasure.org", "code": "kg/m2"},
            "derivedFrom": [{"reference": "Observation/bodyheight", "display": "Body Height"},
                            {"reference": "Observation/example", "display": "Body Weight"}]}


# flatteners ------------------------------------------------------------
# The following functions are used to flatten the FHIR resources.

def flatten_simple(self: DomainResource):
    """Convert the DomainResource instance to just an id."""
    return self.id


def _isodate(v):
    """If a value is a datetime object, return it as an ISO 8601 string."""
    if isinstance(v, datetime) or isinstance(v, date):
        return v.isoformat()
    return v


def flatten_scalars(self: DomainResource) -> dict:
    """Convert the DomainResource instance to a dictionary."""
    _ = {k: _isodate(v) for k, v in self.dict().items() if not isinstance(v, (list, dict))}
    return _


def flatten_references(self: DomainResource) -> dict:
    """Convert the DomainResource instance to a dictionary."""
    fields = [_ for _ in self.__fields__.keys() if not _.endswith('__ext')]
    _ = {}
    # if any top level field in this resource is a Reference, use the Reference.reference https://build.fhir.org/references-definitions.html#Reference.reference
    for k in fields:
        v = getattr(self, k)
        if isinstance(v, Reference):
            v: Reference = v
            _[k] = v.reference
    return _


def flatten_identifier(self: Identifier) -> dict:
    """Convert the Identifier instance to a key value, use a simplified system as key."""
    parsed_url = urlparse(self.system)
    path_parts = parsed_url.path.split('/')  # e.g. "http://hl7.org/fhir/sid/us-ssn" -> us-ssn
    key = path_parts[-1] if path_parts else 'identifier'
    return {key: self.value}


def flatten_coding(self: Coding) -> dict:
    """Convert the DomainResource instance to a dictionary."""
    return {'display': self.display}


def flatten_scalars_and_references(self: DomainResource) -> dict:
    """Convert the DomainResource instance to a dictionary."""
    _ = flatten_scalars(self)
    _.update(flatten_references(self))
    return _


def flatten_scalars_references_identifiers(self: DomainResource) -> dict:
    """Convert the DomainResource instance to a dictionary."""
    _ = flatten_scalars_and_references(self)
    # for now, just flatten all identifiers
    if self.identifier:
        for identifier in self.identifier:
            _.update(flatten_identifier(identifier))
    return _


def flatten_observation(self: Observation) -> dict:
    """Convert the DomainResource instance to a dictionary."""
    _ = flatten_scalars_references_identifiers(self)
    # normalize all the valueXXXXX to 'value'
    if self.valueQuantity:
        _['value'] = f"{self.valueQuantity.value} {self.valueQuantity.unit}"
    elif self.valueString:
        _['value'] = self.valueString
        del _['valueString']
    elif self.valueCodeableConcept:
        _['value'] = self.valueCodeableConcept.text
    # there are many other value types, but we'll ignore them for now
    # see https://build.fhir.org/observation-definitions.html#Observation.value_x_
    # 	Quantity|CodeableConcept|string|boolean|integer|Range|Ratio|SampledData|time|dateTime|Period|Attachment|Reference(MolecularSequence)
    # [for _ in self.__fields__.keys() if _.startswith('value')]
    return _


# patchers ------------------------------------------------------------
# The following fixtures are used to patch the DomainResource class to add the desired method.

@pytest.fixture
def patched_domain_resource_simple() -> bool:
    """Patch the DomainResource class to add a flatten method."""
    # Add the flatten method to the DomainResource class
    DomainResource.flatten = flatten_simple
    yield True
    # Remove the flatten method from the DomainResource class
    del DomainResource.flatten


@pytest.fixture
def patched_scalars() -> bool:
    """Patch the DomainResource class to add a flatten method."""
    # Add the flatten method to the DomainResource class
    DomainResource.flatten = flatten_scalars
    yield True
    # Remove the flatten method from the DomainResource class
    del DomainResource.flatten


@pytest.fixture
def patched_scalars_and_references() -> bool:
    """Patch the DomainResource class to add a flatten method."""
    # Add the flatten method to the DomainResource class
    DomainResource.flatten = flatten_scalars_and_references
    yield True
    # Remove the flatten method from the DomainResource class
    del DomainResource.flatten


@pytest.fixture
def patched_scalars_references_identifiers() -> bool:
    """Patch the DomainResource class to add a flatten method."""
    # Add the flatten method to the DomainResource class
    DomainResource.flatten = flatten_scalars_references_identifiers
    yield True
    # Remove the flatten method from the DomainResource class
    del DomainResource.flatten


@pytest.fixture
def patched_scalars_references_identifiers_observation() -> bool:
    """Patch the DomainResource class to add a flatten method."""
    # Add the flatten method to the DomainResource class e.g. all resources get flattened this way
    DomainResource.flatten = flatten_scalars_references_identifiers
    # except for Observation, which has a different flatten method
    Observation.flatten = flatten_observation
    yield True
    # Remove the flatten method from the DomainResource class
    del DomainResource.flatten
    del Observation.flatten


# tests ------------------------------------------------------------

def test_patient_without_flatten(patient_dict: dict):
    """This patient object should NOT have a 'flatten' method."""
    # without path dependency, just have a plain patient object with no flatten method
    patient = Patient.parse_obj(patient_dict)
    assert not hasattr(patient, 'flatten'), "Patient object should not have a 'flatten' method"


def test_patient_with_simple(patched_domain_resource_simple: bool, patient_dict: dict):
    """This patient object should have a 'flatten' method."""
    patient = Patient.parse_obj(patient_dict)
    assert hasattr(patient, 'flatten'), "Patient object does not have a 'flatten' method"
    assert patient.flatten() == patient.id, f"Patient.flatten() should return {patient.id}"


def test_patient_with_scalars(patched_scalars: bool, patient_dict: dict):
    """This patient object should have a 'flatten' method that returns a dict of scalar values."""
    patient = Patient.parse_obj(patient_dict)
    assert hasattr(patient, 'flatten'), "Patient object does not have a 'flatten' method"
    assert patient.flatten() == {'active': True, 'gender': 'female', 'id': '3', 'resourceType': 'Patient'}, "Patient.flatten() should return a dict of all scalar values"


def test_patient_with_scalars_and_references(patched_scalars_and_references: bool, patient_dict: dict):
    """This patient object should have a 'flatten' method that returns a dict of scalar values and references."""
    patient = Patient.parse_obj(patient_dict)
    assert hasattr(patient, 'flatten'), "Patient object does not have a 'flatten' method"
    assert patient.flatten() == {'active': True, 'gender': 'female', 'id': '3', 'managingOrganization': 'Organization/hl7', 'resourceType': 'Patient'}, "Patient.flatten() should return a dict of all scalar values and references"


def test_patient_with_scalars_references_identifiers(patched_scalars_references_identifiers: bool, patient_dict: dict):
    """This patient object should have a 'flatten' method that returns a dict of scalar values and references."""
    patient = Patient.parse_obj(patient_dict)
    assert hasattr(patient, 'flatten'), "Patient object does not have a 'flatten' method"
    assert patient.flatten() == {'active': True, 'gender': 'female', 'id': '3', 'managingOrganization': 'Organization/hl7', 'resourceType': 'Patient', 'us-ssn': '444555555'}, "Patient.flatten() should return a dict of all scalar values and references"


def test_specimen_with_scalars_references_identifiers(patched_scalars_references_identifiers: bool, specimen_dict: dict):
    """This patient object should have a 'flatten' method that returns a dict of scalar values and references."""
    specimen = Specimen.parse_obj(specimen_dict)
    assert hasattr(specimen, 'flatten'), "Specimen object does not have a 'flatten' method"
    assert specimen.flatten() == {'resourceType': 'Specimen', 'id': 'denovo-3', 'status': 'available',
                                  'receivedTime': '2021-01-01T01:01:01+00:00',
                                  'subject': 'Patient/denovoFather', 'specimens': '3'}


def test_eye_color_observation(patched_scalars_references_identifiers_observation: bool, observation_eye_color_dict: dict):
    """This patient object should have a 'flatten' method that returns a dict of scalar values and references."""
    observation = Observation.parse_obj(observation_eye_color_dict)
    assert hasattr(observation, 'flatten'), "Observation object does not have a 'flatten' method"
    assert observation.flatten() == {'resourceType': 'Observation', 'id': 'eye-color', 'status': 'final',
                                     'effectiveDateTime': '2016-05-18', 'value': 'blue',
                                     'subject': 'Patient/example'}


def test_bmi_observation(patched_scalars_references_identifiers_observation: bool, observation_bmi_dict: dict):
    """This patient object should have a 'flatten' method that returns a dict of scalar values and references."""
    observation = Observation.parse_obj(observation_bmi_dict)
    assert hasattr(observation, 'flatten'), "Observation object does not have a 'flatten' method"
    assert observation.flatten() == {'effectiveDateTime': '1999-07-02',
                                     'id': 'bmi-using-related',
                                     'resourceType': 'Observation',
                                     'status': 'final',
                                     'subject': 'Patient/example',
                                     'value': '16.2 kg/m2'}
