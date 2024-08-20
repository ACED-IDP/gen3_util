import json
import pathlib
import sqlite3
from functools import lru_cache
from typing import Generator, Optional, List, Tuple, Dict

import inflection
import ndjson
import numpy as np
import pandas as pd
import uuid
from collections import defaultdict


class LocalFHIRDatabase:
    def __init__(self, db_name):  # , db_name=pathlib.Path('.g3t') / 'local.db'):
        self.db_name = db_name
        self.connection = None
        self.cursor = None
        self.table_created = {}  # Flag to track if the table has been created

    def connect(self) -> sqlite3.Cursor:
        self.connection = sqlite3.connect(self.db_name)
        self.cursor = self.connection.cursor()

    def disconnect(self):
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
    def patient_everything(self, patient_id: str) -> Generator[dict, None, None]:
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
        return self.resource("Patient", patient_id)

    @lru_cache(maxsize=None)
    def condition_everything(self) -> List[Dict]:
        """Return all the resources for a Condition."""
        cursor = self.connection.cursor()
        cursor.execute(" SELECT * FROM resources where resource_type = ?", ("Condition",))

        resources = []
        for row in cursor.fetchall():
            key, resource_type, resource = row
            resources.append(json.loads(resource))

        return resources
    
    def add_observation_codes(self, resource: dict, observation: dict) -> None:
        """Adds the codes from the provided observation to the provided resource"""
        for component in observation.get("component", []):
            for coding_normalized, coding_source in normalize_coding(component):
                if coding_source == "code":
                    value_normalized, _ = normalize_value(component)
                            # value_normalized = self.handle_units(value_normalized)
                    formatted_coding = coding_normalized[0].translate(
                               self.normalize_table()
                            )
                    if value_normalized is not None:
                        resource[formatted_coding] = value_normalized
                    break

    @lru_cache(maxsize=None)
    def resource(self, resource_type, id) -> dict:
        cursor = self.cursor
        self.cursor.execute(
            "SELECT * FROM resources WHERE key = ?", (f"{resource_type}/{id}",)
        )
        _ = cursor.fetchone()
        if _ is None:
            print(f"{resource_type} {id} not found")
            return None
        key, resource_type, resource = _
        resource = json.loads(resource)

        resource = self.simplify_extensions(resource)

        return resource

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
    
    def normalize_table(self):
        return  str.maketrans(
            {
                ".": "",
                " ": "_",
                "[": "",
                "]": "",
                "'": "",
                ")": "",
                "(": "",
                ",": "",
                "/": "_per_",
                "-": "to",
                "#": "number",
                "+": "_plus_",
                "%": "percent",
                "&": "_and_",
            }
        )

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
    def flattened_specimen(self, specimen_key) -> dict:
        """Return the procedure with everything resolved."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM resources WHERE key = ?", (specimen_key,))
        key, resource_type, resource = cursor.fetchone()
        specimen = json.loads(resource)

        # simplify the identifier

        specimen['identifier'] = specimen['identifier'][0]['value']
        # simplify parent
        if 'parent' in specimen.keys():
            specimen['parent'] = specimen['parent'][0]

        if "collection" in specimen:
            for coding_normalized, coding_source in normalize_coding(
                specimen["collection"]
            ):
                specimen[f"collection_{coding_source}"] = coding_normalized[0]
            del specimen["collection"]
        if "processing" in specimen:
            for processing in specimen.get("processing", []):
                for coding_normalized, coding_source in normalize_coding(processing):
                    # if coding_normalized is a list how to map multiple normalized codings to one coding source?
                    # Getting errors in guppy when coding normalized is a list of > 1 elements
                    specimen[f"processing_{coding_source}"] = coding_normalized[0]
                break  # TODO - only first one
            del specimen["processing"]

        return specimen

    @lru_cache(maxsize=None)
    def flattened_condition(self, condition_key) -> dict:
        """Return the procedure with everything resolved."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM resources WHERE key = ?", (condition_key,))
        key, resource_type, resource = cursor.fetchone()
        condition = json.loads(resource)

        # simplify the identifier
        condition["identifier"] = self.get_nested_value(
            condition, ["identifier", 0, "value"]
        )

        # simplify the code
        condition["code"] = self.select_coding(condition)
        condition["category"] = self.select_category(condition)

        for coding_normalized, coding_source in normalize_coding(condition):
            condition[coding_source] = coding_normalized
        # simplify the onsetAge
        condition["onsetAge"] = self.get_nested_value(condition, ["onsetAge", "value"])

        return condition

    def get_nested_value(self, d: dict, keys: list):
        for key in keys:
            try:
                d = d[key]
            except (KeyError, IndexError, TypeError):
                return None
        return d

    def flattened_procedures(self) -> Generator[dict, None, None]:
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

        connection.close()

    def handle_units(self, value_normalized: str):
        """This function is designed to attempt to remove units string suffixes
        and attempt to store values as float. The issue arises when elastic sees
        string and float data in the same column and gives errors because it is expecting
        only one data type per column"""

        print("UNITS: ", value_normalized)
        if value_normalized is not None:
            value_normalized_split = value_normalized.split(" ")
            if isinstance(value_normalized_split, list):
                value_numeric = value_normalized_split[0]
                if is_number(value_numeric):
                    #print("VALUE NUMERIC: ", float(value_numeric))
                    value_normalized = float(value_numeric)
            return value_normalized
        return None

    def select_category(self, resource):
        if self.get_nested_value(
                resource, ["category", 0, "coding", 0]
        ):
            selected_coding = None

            # Loop through each coding entry
            for coding in resource['category'][0]['coding']:
                # Check if coding system is SNOMED CT
                if coding.get('system') == 'http://snomed.info/sct':
                    selected_coding = coding['display']
                    break  # Found SNOMED code, exit loop

                # If SNOMED code not found, select the first coding
                if selected_coding is None:
                    selected_coding = coding['display']

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
        if self.get_nested_value(
                resource, ["code", "coding", 0]
        ):
            selected_coding = None

            # Loop through each coding entry
            for coding in resource['code']['coding']:
                # Check if coding system is SNOMED CT
                if coding.get('system') == 'http://snomed.info/sct':
                    selected_coding = coding['display']
                    break  # Found SNOMED code, exit loop

                # If SNOMED code not found, select the first coding
                if selected_coding is None:
                    selected_coding = coding['display']

            # Now selected_coding contains the desired coding entry
            # Proceed with further actions or return selected_coding
            return selected_coding
        else:
            # Handle case where no coding entries exist
            return None

    def flattened_observations(self) -> Generator[dict, None, None]:

        loaded_db = self
        connection = sqlite3.connect(loaded_db.db_name)
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT
                json_extract(resource, '$.subject') AS subject,
                focus.value AS focus,
                json(resource) as observation
            FROM resources,
                json_each(json_extract(resource, '$.focus')) AS focus
            WHERE resource_type = ?
            ORDER BY subject, focus
        """,
            ("Observation",),
        )

        def get_patient_and_id(observation):
            patient_id = str(observation["subject"]["reference"]).removeprefix(
                "Patient/"
            )
            patient = loaded_db.patient(patient_id)
            if "identifier" in patient and isinstance(patient["identifier"], list):
                patient["identifier"] = self.get_nested_value(
                    patient, ["identifier", 0, "value"]
                )

            return patient, patient_id

        observations_by_focus = defaultdict(list)

        for row in cursor:
            observation = json.loads(row[2])
            selected_focus = json.dumps(observation["focus"][0]["reference"])
            observations_by_focus[selected_focus].append(observation)

        # since observations are grouped by patient this works
        for focus, observations in observations_by_focus.items():
            patient, _ = get_patient_and_id(observations[0])
            # keep patient id as an artifact, but the id field needs to be unique
            patient["patient_id"] = patient["id"]
            # Better way to mint 'patient' ids for ease of display in elastic
            patient["id"] = uuid.uuid5(uuid.uuid3(uuid.NAMESPACE_DNS, 'aced-idp.org'), str(focus))
            for observation in observations:
                value_normalized, _ = normalize_value(observation)
                # value_normalized = self.handle_units(value_normalized)
                #print("NORMALIZED VALUE: ", value_normalized, "ID: " , observation["id"])

                for coding_normalized, _ in normalize_coding(observation):
                    formatted_coding = coding_normalized[0].translate(self.normalize_table())
                    if value_normalized is not None:
                        patient[formatted_coding] = value_normalized
                        #print("FORMATTED CODING: ", formatted_coding)
                #print("BREAK ___________________________________________________________")

                observation_category = self.get_nested_value(
                    observation, ["category", 0, "coding", 0, "code"]
                )
                if observation_category is not None:
                    patient["category"] = observation_category

                self.add_observation_codes(patient, observation)

                for f in observation.get("focus", []):
                    if f["reference"].startswith("Procedure/"):
                        patient["procedure"] = f["reference"]
                        for k, v in self.flattened_procedure(f["reference"]).items():
                            if k not in ["id", "subject", "resourceType"]:
                                patient[f"procedure_{k}"] = v

                    elif f["reference"].startswith("Specimen/"):
                        for k, v in self.flattened_specimen(f["reference"]).items():
                            if k not in ["id", "subject", "resourceType"]:
                                if k == "parent":
                                    patient[f"specimen_{k}"] = self.get_nested_value(v, ["parent", 0, "reference"])
                                elif k == "type":
                                    specimen_type = self.get_nested_value(
                                        v, ["coding", 0, "display"]
                                    )
                                    patient[f"specimen_{k}"] = specimen_type
                                else:
                                    patient[f"specimen_{k}"] = v

                    elif f["reference"].startswith("Condition/"):
                        for k, v in self.flattened_condition(f["reference"]).items():
                            if k not in ["id", "subject", "resourceType", "encounter"]:
                                if k == "stage":
                                    for j, stage in enumerate(v):
                                        stage_coding = self.get_nested_value(
                                            stage, ["type", "coding", 0, "display"]
                                        )
                                        patient[f"stage_summary_{j}"] = stage_coding
                                else:
                                    patient[f"condition_{k}"] = v

            yield patient

    def flattened_research_subjects(self) -> Generator[dict, None, None]:
        loaded_db = self
        connection = sqlite3.connect(loaded_db.db_name)
        cursor = connection.cursor()
        cursor.execute(
            "SELECT * FROM resources where resource_type = ?", ("ResearchSubject",)
        )

        # for each research subject row
        for _, _, raw_research_subject in cursor.fetchall():
            research_subject = json.loads(raw_research_subject)

            # flatten subject and study (eg Patient)
            subject = self.get_nested_value(
                research_subject, ["subject", "reference"]
            )
            research_subject["subject_type"], research_subject["subject_id"] = subject.split("/")
            del research_subject["subject"]

            study = self.get_nested_value(
                research_subject, ["study", "reference"]
            )
            research_subject["study"] = study.split("/")[1]

            # flatten identifier
            research_subject["identifier"] = self.get_nested_value(
                research_subject, ["identifier", 0, "value"]
            )

            yield research_subject

        connection.close()

    def flattened_document_references(self) -> Generator[dict, None, None]:
        loaded_db = self
        connection = sqlite3.connect(loaded_db.db_name)
        cursor = connection.cursor()
        cursor.execute("""
            SELECT
                json(focus.value) AS focus,
                json(resource) as observation
            FROM resources,
                 json_each(json_extract(resource, '$.focus')) AS focus
            WHERE resource_type = ?
        """, ("Observation",))

        # create a dict mapping from focus (DocRef) ID to observation
        associated_observations_map = {}
        for focus_str, obs in cursor.fetchall():
            focus = json.loads(focus_str)["reference"]
            if "DocumentReference" in focus:
                associated_observations_map[focus] = json.loads(obs)

        # process document references
        cursor.execute(
            "SELECT * FROM resources where resource_type = ?", ("DocumentReference",)
        )

        for _ in cursor.fetchall():
            key, resource_type, procedure = _
            document_reference = json.loads(procedure)

            # simplify the subject

            subject = document_reference.get("subject", {"reference": None})[
                "reference"
            ]

            if subject is not None:
                subject_type, subject_id = subject.split("/")
                # Look for the patient reference in specimen if it is not in docref
                if subject_type != "Patient":
                    document_reference[f"{inflection.underscore(subject_type)}_subject"] = subject_id
                if subject_type == "Specimen":
                    for k, v in self.flattened_specimen(subject).items():
                        if k == "subject":
                            subject_type, subject_id = v["reference"].split("/")
                            if subject_type == "Patient":
                                document_reference["subject"] = subject_id

            docref_category = self.get_nested_value(
                document_reference, ["category", 0, "coding", 0, "code"]
            )
            if docref_category is not None:
                document_reference["category"] = docref_category

            # In some places like TCGA-LUAD there is more than one identifier that could be displayed
            document_reference["identifier"] = document_reference.get(
                "identifier", [{"value": None}]
            )[0]["value"]

            for elem in normalize_coding(document_reference):
                document_reference[elem[1]] = elem[0][0]

            # simplify the extensions
            if (
                self.get_nested_value(document_reference, ["content", 0, "attachment"])
                is not None
            ):
                if "extension" in document_reference["content"][0]["attachment"]:
                    for _ in document_reference["content"][0]["attachment"][
                        "extension"
                    ]:
                        value_normalized, value_source = normalize_value(_)
                        document_reference[(_["url"].split("/")[-1])] = value_normalized

                content_url = self.get_nested_value(
                    document_reference, ["content", 0, "attachment", "url"]
                )
                if content_url is not None:
                    document_reference["source_url"] = content_url

            if "content" in document_reference:
                for k, v in document_reference["content"][0]["attachment"].items():
                    if k in ["extension"]:
                        continue
                    if k == "size":
                        document_reference[k] = str(v)
                        continue
                    document_reference[k] = v

            if "basedOn" in document_reference:
                for i, dict_ in enumerate(document_reference["basedOn"]):
                    document_reference[f"basedOn_{i}"] = dict_["reference"]
                del document_reference["basedOn"]

            if subject is not None and subject.startswith("Patient/"):
                _, patient_id = subject.split("/")
                resources = [_ for _ in loaded_db.patient_everything(patient_id)]
                resources.append(loaded_db.patient(patient_id))
                for resource in resources:

                    if resource["resourceType"] == "Patient":
                        identifier = self.get_nested_value(
                            resource, ["identifier", 0, "value"]
                        )
                        if identifier is not None:
                            document_reference["patient"] = identifier
                        # document_reference['patient'] = resource['identifier'][0]['value']
                        continue

                    if (
                        resource["resourceType"] == "Condition"
                        and f"Condition/{resource['id']}" == procedure["reason"]
                    ):
                        document_reference["reason"] = resource["code"]["text"]
                        continue

                    if resource["resourceType"] == "Observation":
                        # must be focus
                        if f"Procedure/{procedure['id']}" not in [
                            _["reference"] for _ in resource["focus"]
                        ]:
                            continue

                        # TODO - pick first coding, h2 allow user to specify preferred coding
                        code = resource["code"]["coding"][0]["code"]

                        value = normalize_value(resource)

                        assert value is not None, f"no value for {resource['id']}"
                        document_reference[code] = value

                        continue

                    # skip these
                    if resource["resourceType"] in [
                        "Specimen",
                        "Procedure",
                        "ResearchSubject",
                        "DocumentReference",
                    ]:
                        continue

                    # default, add entire resource as an item of the list
                    resource_type = inflection.underscore(resource["resourceType"])
                    if resource_type not in procedure:
                        document_reference[resource_type] = []
                    document_reference[resource_type].append(resource)

            del document_reference["content"]
            
            # populate DocRef with codes if there's an associated Observation
            doc_ref_id = f"DocumentReference/{document_reference['id']}"
            if doc_ref_id in associated_observations_map:
                observation = associated_observations_map[doc_ref_id]
                self.add_observation_codes(document_reference, observation)
            
            yield document_reference

        connection.close()   

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

    if data_type == "DocumentReference":
        df = pd.DataFrame(db.flattened_document_references())
    elif data_type == "Observation":
        df = pd.DataFrame(db.flattened_observations())
    elif data_type == "ResearchSubject":
        df = pd.DataFrame(db.flattened_research_subjects())
    else:
        raise ValueError(
            f"{data_type} not supported yet. Supported data types are DocumentReference and Observation"
        )
    assert (
        not df.empty
    ), "Dataframe is empty, are there any DocumentReference resources?"

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


def normalize_value(resource_dict: dict) -> tuple[Optional[str], Optional[str]]:
    """return a tuple containing the normalized value and the name of the field it was derived from"""
    value_normalized = None
    value_source = None

    if set(resource_dict.keys()) == set(["url", "extension"]):
        assert (
            len(resource_dict["extension"]) > 0
        ), f"Expected at least on extension, in {resource_dict}"
        return normalize_value(resource_dict["extension"][0])
    if "valueQuantity" in resource_dict:
        value = resource_dict["valueQuantity"]
        value_normalized = f"{value['value']} {value.get('unit', '')}"
        value_source = "valueQuantity"
    elif "valueCodeableConcept" in resource_dict:
        value = resource_dict["valueCodeableConcept"]
        # For the labkey dataset, the values from the "display" keys had system specific metadata, so first check for
        # text key populated before going for that sub optimal display value
        if "text" in value:
            value_normalized = value["text"]
        else:
            value_normalized = " ".join(
                [coding["display"] for coding in value.get("coding", [])]
            )
        value_source = "valueCodeableConcept"
    elif "valueCoding" in resource_dict:
        value = resource_dict["valueCoding"]
        value_normalized = value["display"]
        value_source = "valueCoding"
    elif "valueString" in resource_dict:
        value_normalized = resource_dict["valueString"]
        value_source = "valueString"
    elif "valueCode" in resource_dict:
        value_normalized = resource_dict["valueCode"]
        value_source = "valueCode"
    elif "valueBoolean" in resource_dict:
        value_normalized = str(resource_dict["valueBoolean"])
        value_source = "valueBoolean"
    elif "valueInteger" in resource_dict:
        value_normalized = str(resource_dict["valueInteger"])
        value_source = "valueInteger"
    elif "valueRange" in resource_dict:
        value = resource_dict["valueRange"]
        low = value["low"]
        high = value["high"]
        value_normalized = f"{low['value']} - {high['value']} {low.get('unit', '')}"
        value_source = "valueRange"
    elif "valueRatio" in resource_dict:
        value = resource_dict["valueRatio"]
        numerator = value["numerator"]
        denominator = value["denominator"]
        value_normalized = f"{numerator['value']} {numerator.get('unit', '')}/{denominator['value']} {denominator.get('unit', '')}"
        value_source = "valueRatio"
    elif "valueSampledData" in resource_dict:
        value = resource_dict["valueSampledData"]
        value_normalized = value["data"]
        value_source = "valueSampledData"
    elif "valueTime" in resource_dict:
        value_normalized = resource_dict["valueTime"]
        value_source = "valueTime"
    elif "valueDateTime" in resource_dict:
        value_normalized = resource_dict["valueDateTime"]
        value_source = "valueDateTime"
    elif "valuePeriod" in resource_dict:
        value = resource_dict["valuePeriod"]
        value_normalized = f"{value['start']} to {value['end']}"
        value_source = "valuePeriod"
    # for debugging...
    # else:
    #     raise ValueError(f"value[x] not found in {resource_dict}")

    return value_normalized, value_source


def normalize_coding(resource_dict: dict) -> List[Tuple[str, str]]:
    def extract_coding(coding_list):
        # return a concatenated string
        # return ','.join([coding.get('display', '') for coding in coding_list if 'display' in coding])
        # or alternatively return an array
        return [coding.get("display", coding.get("code", "")) for coding in coding_list]

    def find_codings_in_dict(d: dict, parent_key: str = "") -> List[Tuple[str, str]]:
        codings = []
        for key, value in d.items():
            if "valueCodeableConcept" in key:
                continue
            if isinstance(value, list):
                # categories are values not codings in the pivot.
                if "category" in key:
                    continue
                for item in value:
                    if isinstance(item, dict):
                        # valudeCodeableConcept is a value in the pivot not a coding
                        # Check if the dict contains a 'coding' list
                        if "coding" in item and isinstance(item["coding"], list):
                            coding_string = extract_coding(item["coding"])
                            codings.append((coding_string, key))
                        # Recursively search in the dict
                        codings.extend(find_codings_in_dict(item, key))
            elif isinstance(value, dict):
                # Check if the dict contains a 'coding' list
                if "coding" in value and isinstance(value["coding"], list):
                    coding_string = extract_coding(value["coding"])
                    codings.append((coding_string, key))
                # Recursively search in the dict
                codings.extend(find_codings_in_dict(value, key))
        return codings

    return find_codings_in_dict(resource_dict)


def is_number(s):
    """Returns True if string is a number."""
    try:
        int(s)
        return True
    except ValueError:
        return False
