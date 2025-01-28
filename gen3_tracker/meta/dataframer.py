###########################
# LOCAL FHIR DATABASE ###
###########################

import inflection
import json
import ndjson
import numpy as np
import pandas as pd
import pathlib
import sqlite3

from collections import defaultdict
from deepmerge import always_merger
from functools import lru_cache
from typing import Dict, Generator, List

from gen3_tracker.meta.entities import (
    SimplifiedGroup,
    SimplifiedResource,
    get_nested_value,
    normalize_coding,
    normalize_value,
    traverse,
)


class LocalFHIRDatabase:
    def __init__(self, db_name):  # , db_name=pathlib.Path('.g3t') / 'local.db'):
        self.db_name = db_name
        self.connection = None
        self.cursor = None
        self.table_created = {}  # Flag to track if the table has been created

    def connect(self) -> sqlite3.Cursor:
        """establish database connection if not established, return cursor"""
        if self.connection is None:
            self.connection = sqlite3.connect(self.db_name)
        if self.cursor is None:
            self.cursor = self.connection.cursor()
        else:
            return self.cursor

    def disconnect(self) -> None:
        """clean up database connection"""
        if self.connection:
            self.connection.commit()
            self.connection.close()

    def create_table(
        self,
        name="resources",
        ddl="""
                    CREATE TABLE __NAME__ (
                        key TEXT PRIMARY KEY,
                        resource_type TEXT,
                        resource JSON
                    )
                """,
    ):
        self.connect()
        # Check if the table exists before creating it
        self.cursor.execute(
            f"SELECT name FROM sqlite_master WHERE type='table' AND name='{name}'"
        )
        table_exists = self.cursor.fetchone()

        if not table_exists:
            ddl = ddl.replace("__NAME__", name)
            self.cursor.execute(ddl)
            self.table_created[name] = True

    def count(self, table_name="resources"):
        self.connect()
        self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = self.cursor.fetchone()[0]
        return count

    def insert_data(self, id_, resource_type, resource, table_name="resources"):
        """Insert data into the database."""
        if table_name not in self.table_created:
            self.create_table(
                table_name
            )  # Lazily create the table if not already created

        composite_key = f"{resource_type}/{id_}"

        # see if the resource already exists
        self.cursor.execute(
            """
            SELECT resource FROM resources WHERE key = ?
        """,
            (composite_key,),
        )
        row = self.cursor.fetchone()

        # Initialize an empty dictionary to hold the resource
        existing_resource = {}

        # Check if the row is not None
        if row is not None:
            # The first element of the row contains the resource as a JSON string
            resource_json = row[0]

            # Convert the JSON string into a Python dictionary
            existing_resource = json.loads(resource_json)

        # Merge the existing resource with the new resource
        resource = always_merger.merge(existing_resource, resource)

        self.cursor.execute(
            f"""
            INSERT INTO {table_name} (key, resource_type, resource)
            VALUES (?, ?, ?)
        """,
            (composite_key, resource_type, json.dumps(resource)),
        )
        # print(f"Inserted {composite_key} into the database")

    def insert_data_from_dict(self, resource, table_name="resources"):
        """Insert data into the database from a dictionary."""
        if "id" not in resource or (
            "resource_type" not in resource and "resourceType" not in resource
        ):
            raise ValueError(
                f"Resource dictionary must contain 'id' and 'resource_type' keys {resource}"
            )
        self.insert_data(
            resource["id"],
            resource.get("resource_type", resource.get("resourceType")),
            resource,
            table_name,
        )

    def bulk_insert_data(self, resources, table_name="resources") -> int:
        """Bulk insert data into the database."""

        if table_name not in self.table_created:
            self.create_table(
                table_name
            )  # Lazily create the table if not already created

        def _prepare(resource):
            resource_type = resource.get("resource_type", resource.get("resourceType"))
            id_ = resource["id"]
            composite_key = f"{resource_type}/{id_}"
            return (composite_key, resource_type, json.dumps(resource))

        def _iterate(_resources):
            for _ in _resources:
                yield _prepare(_)

        try:
            self.connect()
            sql = f"""
                INSERT INTO {table_name} (key, resource_type, resource)
                VALUES (?, ?, ?)
            """
            new_cursor = self.cursor.executemany(sql, _iterate(_resources=resources))

        except sqlite3.IntegrityError as e:
            for resource in resources:
                prepared_resource = _prepare(resource)
                try:
                    self.cursor.execute(sql, prepared_resource)
                except sqlite3.IntegrityError:
                    print(f"Error inserting resource: {prepared_resource}")
                    print(f"Exception: {e}")
            raise

        finally:
            self.connection.commit()
            # self.disconnect()

        return new_cursor.rowcount

    def load_from_ndjson_file(self, file_path, table_name="resources"):
        """Load the NDJSON file into the database."""

        if table_name not in self.table_created:
            self.create_table(
                table_name
            )  # Lazily create the table if not already created

        with open(file_path, "r") as file:
            reader = ndjson.reader(file)
            self.bulk_insert_data(reader)

    def load_ndjson_from_dir(self, path: str = "META", pattern: str = "*.ndjson"):
        """Load all the NDJSON files in the directory into the database."""
        for file_path in pathlib.Path(path).glob(pattern):
            self.load_from_ndjson_file(file_path)

    @lru_cache(maxsize=None)
    def patient_everything(self, patient_id) -> Generator[dict, None, None]:
        """Return all the resources for a patient."""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT * FROM resources WHERE key = ?", (f"Patient/{patient_id}",)
        )

        for _ in cursor.fetchall():
            key, resource_type, resource = _
            yield json.loads(resource)

    # @lru_cache(maxsize=None) this cache was giving updated lookup results when I di
    def patient(self, patient_id) -> dict:
        """Return the patient resource."""
        cursor = self.cursor
        self.cursor.execute(
            "SELECT * FROM resources WHERE key = ?", (f"Patient/{patient_id}",)
        )
        _ = cursor.fetchone()
        if _ is None:
            print(f"Patient {patient_id} not found")
            return None
        key, resource_type, resource = _
        resource = json.loads(resource)

        resource = self.simplify_extensions(resource)

        return resource

    @lru_cache(maxsize=None)
    def condition_everything(self) -> List[Dict]:
        """Return all the resources for a Condition."""
        cursor = self.connection.cursor()
        cursor.execute(
            " SELECT * FROM resources where resource_type = ?", ("Condition",)
        )

        resources = []
        for row in cursor.fetchall():
            key, resource_type, resource = row
            resources.append(json.loads(resource))

        return resources

    @lru_cache(maxsize=None)
    def resource(self, resourceType, id) -> dict:
        """Return any resource with id and type specified"""
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT * FROM resources WHERE key = ?", (f"{resourceType}/{id}",)
        )
        _ = cursor.fetchone()
        if _ is None:
            print(f"{resourceType} {id} not found")
            return None
        key, resource_type, resource = _
        resource = json.loads(resource)

        resource = self.simplify_extensions(resource)

    @staticmethod
    def simplify_extensions(resource: dict) -> dict:
        """Extract extension values, derive key from extension url"""
        for _ in resource.get("extension", []):
            value_normalized, value_source = normalize_value(_)
            extension_key = _["url"].split("/")[-1]
            extension_key = (
                inflection.underscore(extension_key)
                .removesuffix(".json")
                .removeprefix("structure_definition_")
            )
            resource[extension_key] = value_normalized
            assert value_normalized, f"extension: {extension_key} = {value_normalized}"
        if "extension" in resource:
            del resource["extension"]
        return resource

    # @lru_cache(maxsize=None)
    def flattened_procedure(self, procedure_key) -> dict:
        """Return the procedure with everything resolved."""
        cursor = self.cursor
        cursor.execute("SELECT * FROM resources WHERE key = ?", (procedure_key,))
        key, resource_type, resource = cursor.fetchone()
        procedure = json.loads(resource)

        # simplify the identifier
        procedure["identifier"] = procedure["identifier"][0]["value"]
        # simplify the code
        procedure["code"] = procedure["code"]["coding"][0]["display"]
        # simplify the reason
        procedure["reason"] = procedure["reason"][0]["reference"]["reference"]
        # simplify the occurrenceAge
        procedure["occurrenceAge"] = procedure["occurrenceAge"]["value"]
        # simplify the subject
        subject = procedure["subject"]["reference"]
        procedure["subject"] = subject

        return procedure

    @lru_cache(maxsize=None)
    def flattened_condition(self, condition_key) -> dict:
        """Return the procedure with everything resolved."""
        #  TODO - implement with gen3_tracker.meta.entites.SimplifiedFHIR
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM resources WHERE key = ?", (condition_key,))
        key, resource_type, resource = cursor.fetchone()
        condition = json.loads(resource)

        # simplify the identifier
        condition["identifier"] = get_nested_value(
            condition, ["identifier", 0, "value"]
        )

        # simplify the code
        condition["code"] = self.select_coding(condition)
        condition["category"] = self.select_category(condition)

        for coding_normalized, coding_source in normalize_coding(condition):
            condition[coding_source] = coding_normalized
        # simplify the onsetAge
        condition["onsetAge"] = get_nested_value(condition, ["onsetAge", "value"])

        return condition

    def flattened_procedures(self) -> Generator[dict, None, None]:
        #  TODO - implement with gen3_tracker.meta.entites.SimplifiedFHIR
        """Return all the procedures with everything resolved"""
        loaded_db = self
        connection = sqlite3.connect(loaded_db.db_name)
        cursor = connection.cursor()
        cursor.execute(
            "SELECT * FROM resources where resource_type = ?", ("Procedure",)
        )

        for _ in cursor.fetchall():
            key, resource_type, procedure = _
            procedure = json.loads(procedure)
            # simplify the identifier
            procedure["identifier"] = procedure["identifier"][0]["value"]
            # simplify the code
            procedure["code"] = procedure["code"]["coding"][0]["display"]
            # simplify the reason
            procedure["reason"] = procedure["reason"][0]["reference"]["reference"]
            # simplify the occurrenceAge
            procedure["occurrenceAge"] = procedure["occurrenceAge"]["value"]
            # simplify the subject
            subject = procedure["subject"]["reference"]
            procedure["subject"] = subject

            if subject.startswith("Patient/"):
                _, patient_id = subject.split("/")
                resources = [_ for _ in loaded_db.patient_everything(patient_id)]
                resources.append(loaded_db.patient(patient_id))
                for resource in resources:

                    if resource["resourceType"] == "Patient":
                        procedure["patient"] = resource["identifier"][0]["value"]
                        continue

                    if (
                        resource["resourceType"] == "Condition"
                        and f"Condition/{resource['id']}" == procedure["reason"]
                    ):
                        procedure["reason"] = resource["code"]["text"]
                        continue

                    if resource["resourceType"] == "Observation":
                        # must be focus
                        if f"Procedure/{procedure['id']}" not in [
                            _["reference"] for _ in resource["focus"]
                        ]:
                            continue

                        # TODO - pick first coding, h2 allow user to specify preferred coding
                        code = resource["code"]["coding"][0]["code"]

                        if "valueQuantity" in resource:
                            value = resource["valueQuantity"]["value"]
                        elif "valueCodeableConcept" in resource:
                            value = resource["valueCodeableConcept"]["text"]
                        elif "valueInteger" in resource:
                            value = resource["valueInteger"]
                        elif "valueString" in resource:
                            value = resource["valueString"]
                        else:
                            value = None

                        assert value is not None, f"no value for {resource['id']}"
                        procedure[code] = value

                        continue

                    # skip these
                    if resource["resourceType"] in [
                        "Specimen",
                        "Procedure",
                        "ResearchSubject",
                    ]:
                        continue

                    # default, add entire resource as an item of the list
                    resource_type = inflection.underscore(resource["resourceType"])
                    if resource_type not in procedure:
                        procedure[resource_type] = []
                    procedure[resource_type].append(resource)

            yield procedure

    def handle_units(self, value_normalized: str):
        """This function is designed to attempt to remove units string suffixes
        and attempt to store values as float. The issue arises when elastic sees
        string and float data in the same column and gives errors because it is expecting
        only one data type per column"""

        if value_normalized is not None:
            value_normalized_split = value_normalized.split(" ")
            if isinstance(value_normalized_split, list):
                value_numeric = value_normalized_split[0]
                if is_number(value_numeric):
                    value_normalized = float(value_numeric)
            return value_normalized
        return None

    def select_category(self, resource):
        if get_nested_value(resource, ["category", 0, "coding", 0]):
            selected_coding = None

            # Loop through each coding entry
            for coding in resource["category"][0]["coding"]:
                # Check if coding system is SNOMED CT
                if coding.get("system") == "http://snomed.info/sct":
                    selected_coding = coding["display"]
                    break  # Found SNOMED code, exit loop

                # If SNOMED code not found, select the first coding
                if selected_coding is None:
                    selected_coding = coding["display"]

            # Now selected_coding contains the desired coding entry
            # Proceed with further actions or return selected_coding
            return selected_coding
        else:
            # Handle case where no coding entries exist
            return None

    def select_coding(self, resource):
        """
        Selects and returns a coding entry from a FHIR Condition resource based on the following priority:
        1. Selects a coding entry with system 'http://snomed.info/sct' if present.
        2. If no SNOMED coding is found, selects the first coding entry.

        Args:
            condition (dict): The FHIR Condition resource containing 'code' and 'code.coding' fields.

        Returns:
            dict or None: The selected coding entry dictionary if found, or None if no coding entries exist.
        """
        if get_nested_value(resource, ["code", "coding", 0]):
            selected_coding = None

            # Loop through each coding entry
            for coding in resource["code"]["coding"]:
                # Check if coding system is SNOMED CT
                if coding.get("system") == "http://snomed.info/sct":
                    selected_coding = coding["display"]
                    break  # Found SNOMED code, exit loop

                # If SNOMED code not found, select the first coding
                if selected_coding is None:
                    selected_coding = coding["display"]

            # Now selected_coding contains the desired coding entry
            # Proceed with further actions or return selected_coding
            return selected_coding
        else:
            # Handle case where no coding entries exist
            return None

    def flattened_research_subjects(self) -> Generator[dict, None, None]:

        # get all observations with a Observation.subject=Patient, mapped from patient ID to observation
        resource_type = "ResearchSubject"
        conditions_by_patient_id = get_conditions_by_subject(self, "Patient")

        # get all ResearchSubjects
        cursor = self.connect()
        cursor.execute(
            "SELECT * FROM resources where resource_type = ?", (resource_type,)
        )

        # get research subject and associated .subject patient
        for _, _, raw_research_subject in cursor.fetchall():
            research_subject = json.loads(raw_research_subject)
            flat_research_subject = SimplifiedResource.build(
                resource=research_subject
            ).simplified

            # return with .subject (ie Patient) fields
            patient = get_subject(self, research_subject)
            flat_research_subject.update(patient)

            # get condition code, eg enrollment diagnosis
            if patient["patient_id"] in conditions_by_patient_id:
                conditions = conditions_by_patient_id[patient["patient_id"]]

                # TODO: assumes there are no duplicate column names in each condition
                for condition in conditions:
                    for k, v in traverse(condition).items():
                        if k not in set(["condition_id", "condition_identifier"]):
                            flat_research_subject[k] = v

            yield flat_research_subject

    def flattened_medication_administrations(self) -> Generator[dict, None, None]:

        # get all MedicationAdministrations
        cursor = self.connect()
        cursor.execute(
            "SELECT * FROM resources where resource_type = ?",
            ("MedicationAdministration",),
        )

        # get research subject and associated .subject patient
        for _, _, raw_medication_administration in cursor.fetchall():
            medication_administration = json.loads(raw_medication_administration)
            flat_medication_administration = SimplifiedResource.build(
                resource=medication_administration
            ).simplified

            patient = get_subject(self, medication_administration)
            flat_medication_administration.update(patient)

            yield flat_medication_administration

    def flattened_document_references(self) -> Generator[dict, None, None]:
        """generator that yields document references populated
        with DocumentReference.subject fields and Observation codes through Observation.focus
        """

        cursor = self.connect()
        resource_type = "DocumentReference"

        # get a dict mapping focus ID to its associated observations
        observation_by_focus_id = get_observations_by_focus(self, resource_type)

        # flatten each document reference
        cursor.execute(
            "SELECT * FROM resources where resource_type = ?", (resource_type,)
        )
        for _, _, resource in cursor.fetchall():
            document_reference = json.loads(resource)
            yield self.flattened_document_reference(
                document_reference, observation_by_focus_id
            )

    def flattened_document_reference(
        self, doc_ref: dict, observation_by_focus_id: dict
    ) -> dict:
        # simplify document reference
        flat_doc_ref = SimplifiedResource.build(resource=doc_ref).simplified

        # extract the corresponding .subject and append its fields
        flat_doc_ref.update(get_subject(self, doc_ref))

        # populate observation data associated with the document reference document
        if doc_ref["id"] in observation_by_focus_id:
            associated_observations = observation_by_focus_id[doc_ref["id"]]

            # TODO: assumes there are no duplicate column names in each observation
            for observation in associated_observations:
                flat_observation = SimplifiedResource.build(resource=observation).values
                flat_doc_ref.update(flat_observation)

        # TODO: test this based on fhir-gdc
        if "basedOn" in doc_ref:
            for i, dict_ in enumerate(doc_ref["basedOn"]):
                doc_ref[f"basedOn_{i}"] = dict_["reference"]
            del doc_ref["basedOn"]

        return flat_doc_ref

    @lru_cache(maxsize=None)
    def flattened_specimens(self) -> Generator[dict, None, None]:
        """generator that yields specimens populated with Specimen.subject fields
        and Observation codes through Observation.focus"""

        resource_type = "Specimen"
        cursor = self.connect()

        # get a dict mapping focus (specimen) ID to its associated observations
        observations_by_focus_id = get_observations_by_focus(self, resource_type)

        # flatten each specimen
        cursor.execute(
            "SELECT * FROM resources where resource_type = ?", (resource_type,)
        )
        for _, _, resource in cursor.fetchall():
            specimen = json.loads(resource)
            yield self.flattened_specimen(specimen, observations_by_focus_id)

    def flattened_specimen(self, specimen: dict, observation_by_id: dict) -> dict:
        """Return the specimen with everything resolved."""

        # create simple specimen dict
        flat_specimen = SimplifiedResource.build(resource=specimen).simplified

        # extract its .subject and append its fields (including id)
        flat_specimen.update(get_subject(self, specimen))

        # populate observation codes for each associated observation
        if specimen["id"] in observation_by_id:
            observations = observation_by_id[specimen["id"]]

            # TODO: assumes there are no duplicate column names in each observation
            for observation in observations:
                flat_observation = SimplifiedResource.build(resource=observation).values
                flat_specimen.update(flat_observation)

        return flat_specimen

    def flattened_group_members(self) -> Generator[dict, None, None]:
        """generator that yields fhir group entities, flattening out members.entities.reference"""

        resource_type = "Group"
        cursor = self.connect()

        # get all groups
        cursor.execute(
            "SELECT * FROM resources where resource_type = ?", (resource_type,)
        )

        # flatten groups into group members
        for _, _, resource in cursor.fetchall():
            group = json.loads(resource)

            # flatten group, typed for code readability
            group_resource: SimplifiedGroup = SimplifiedResource.build(resource=group)
            simplified_group = group_resource.simplified

            # for each member in a group, yield a group member dict
            for member_id in group_resource.members:
                # unique primary key from group and member ids
                group_member_id = (
                    simplified_group["id"] + "-" + member_id
                )

                # group member dict composed of a simple group dict, unique primary key, and unique member_id
                yield {
                    **simplified_group,
                    "id": group_member_id,
                    "member_id": member_id,
                }


def create_dataframe(
    directory_path: str, work_path: str, data_type: str
) -> pd.DataFrame:
    """Create a dataframe from the FHIR data in the directory."""
    assert pathlib.Path(work_path).exists(), f"Directory {work_path} does not exist."
    work_path = pathlib.Path(work_path)
    db_path = work_path / "local_fhir.db"
    db_path.unlink(missing_ok=True)

    db = LocalFHIRDatabase(db_name=db_path)
    db.load_ndjson_from_dir(path=directory_path)

    data_type_to_flatten_fn = {
        "DocumentReference": db.flattened_document_references,
        "ResearchSubject": db.flattened_research_subjects,
        "MedicationAdministration": db.flattened_medication_administrations,
        "Specimen": db.flattened_specimen,
        "GroupMember": db.flattened_group_members,
    }

    if data_type in data_type_to_flatten_fn:
        flattener = data_type_to_flatten_fn[data_type]
        df = pd.DataFrame(flattener())
    else:
        data_types_str = ", ".join(data_type_to_flatten_fn)
        raise ValueError(
            f"{data_type} not supported yet. Supported data types are {data_types_str}"
        )

    if df.empty:
        raise ValueError(
            "Dataframe is empty, are there any DocumentReference resources?"
        )

    front_column_names = ["resourceType", "identifier"]
    if "patient" in df.columns:
        front_column_names = front_column_names + ["patient"]

    remaining_columns = [col for col in df.columns if col not in front_column_names]
    rear_column_names = [
        "id"
    ]  # removed status for the purpose of not needing it for the demo
    if "subject" in df.columns:
        rear_column_names = rear_column_names + ["subject"]
    for c in df.columns:
        if c.endswith("_identifier"):
            rear_column_names.append(c)
    remaining_columns = [
        col for col in remaining_columns if col not in rear_column_names
    ]

    reordered_columns = front_column_names + remaining_columns + rear_column_names
    df = df[reordered_columns]
    df = df.replace({np.nan: ""})
    return df


def is_number(s):
    """Returns True if string is a number."""
    try:
        int(s)
        return True
    except ValueError:
        return False


def get_subject(db: LocalFHIRDatabase, resource: dict) -> dict:
    """get the resource's subject field if it exists"""

    # ensure resource has subject field
    subject_key = get_nested_value(resource, ["subject", "reference"])
    if subject_key is None:
        return {}

    # traverse the resource of the subject and return its values
    cursor = db.connect()
    cursor.execute("SELECT * FROM resources WHERE key = ?", (subject_key,))
    row = cursor.fetchone()
    assert row, f"{subject_key} not found in database"
    _, _, raw_subject = row
    subject = json.loads(raw_subject)
    return traverse(subject)


def get_resources_by_reference(
    db: LocalFHIRDatabase, resource_type: str, reference_field: str, reference_type: str
) -> dict[str, list]:
    """given a set of rescode ources of type resource_type, map each unique reference in reference field of type reference_type to its associated resources
    ex: use all Observations with a Specimen focus, map Specimen IDs to its list of associated Observations and return the map
    """

    # ensure reference field is allowed
    allowed_fields = ["focus", "subject"]
    assert (
        reference_field in allowed_fields
    ), f"Field not implemented, choose between {allowed_fields}"

    cursor = db.connect()
    cursor.execute(
        """
        SELECT *
        FROM resources
        WHERE resource_type = ?
    """,
        (resource_type,),
    )

    resource_by_reference_id = defaultdict(list)

    for _, _, raw_resource in cursor.fetchall():
        resource = json.loads(raw_resource)

        # determine which how to process the field
        if reference_field == "focus":
            # add the resource (eg observation) for each focus reference to the dict
            for i in range(len(resource["focus"])):
                reference_key = get_nested_value(
                    resource, [reference_field, i, "reference"]
                )
                if reference_key is not None and reference_type in reference_key:
                    reference_id = reference_key.split("/")[-1]
                    resource_by_reference_id[reference_id].append(resource)

        elif reference_field == "subject":
            # add the resource (eg observation) to the dict
            reference_key = get_nested_value(resource, [reference_field, "reference"])
            if reference_key is not None and reference_type in reference_key:
                reference_id = reference_key.split("/")[-1]
                resource_by_reference_id[reference_id].append(resource)

    return resource_by_reference_id


def get_observations_by_focus(
    db: LocalFHIRDatabase, focus_type: str
) -> dict[str, list]:
    """get all Observations that have a focus of resource type focus_type"""
    return get_resources_by_reference(db, "Observation", "focus", focus_type)


def get_conditions_by_subject(
    db: LocalFHIRDatabase, subject_type: str
) -> dict[str, list]:
    """get all Conditions that have a subject of resource type subject_type"""
    return get_resources_by_reference(db, "Condition", "subject", subject_type)
