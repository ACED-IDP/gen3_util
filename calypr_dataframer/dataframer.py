"""FHIR metadata dataframe generation utilities."""

import uuid
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

from calypr_dataframer import CALYPR_NAMESPACE
from calypr_dataframer.entities import (
    SimplifiedGroup,
    SimplifiedResource,
    get_nested_value,
    normalize_coding,
    normalize_value,
    traverse,
    validate_and_transform_graphql_field_name,
)


class LocalFHIRDatabase:
    """SQLite-based local FHIR database for processing metadata."""
    
    def __init__(self, db_name):
        self.db_name = db_name
        self.connection = None
        self.cursor = None
        self.table_created = {}  # Flag to track if the table has been created

    def connect(self) -> sqlite3.Cursor:
        """Establish database connection if not established, return cursor."""
        if self.connection is None:
            self.connection = sqlite3.connect(self.db_name)
        if self.cursor is None:
            self.cursor = self.connection.cursor()
        return self.cursor

    def disconnect(self) -> None:
        """Clean up database connection."""
        if self.connection:
            self.connection.commit()
            self.connection.close()

    def create_table(self, table_name="resources"):
        """Create the resources table."""
        self.connect()
        self.cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                key TEXT PRIMARY KEY,
                resource_type TEXT,
                resource JSON
            )
        """
        )
        self.table_created[table_name] = True
        self.connection.commit()

    def insert_data(self, id_, resource_type, resource, table_name="resources"):
        """Insert data into the database with upsert behavior."""
        if table_name not in self.table_created:
            self.create_table(table_name)

        self.connect()
        composite_key = f"{resource_type}/{id_}"

        # Check if the key already exists
        self.cursor.execute(
            f"SELECT resource FROM {table_name} WHERE key = ?", (composite_key,)
        )
        row = self.cursor.fetchone()

        existing_resource = {}
        if row is not None:
            resource_json = row[0]
            existing_resource = json.loads(resource_json)

        # Merge the existing resource with the new resource
        resource = always_merger.merge(existing_resource, resource)

        self.cursor.execute(
            f"""
            INSERT OR REPLACE INTO {table_name} (key, resource_type, resource)
            VALUES (?, ?, ?)
        """,
            (composite_key, resource_type, json.dumps(resource)),
        )

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
            self.create_table(table_name)

        def _prepare(resource):
            resource_type = resource.get("resource_type", resource.get("resourceType"))
            id_ = resource["id"]
            composite_key = f"{resource_type}/{id_}"
            return (composite_key, resource_type, json.dumps(resource))

        def _iterate(_resources):
            for _ in _resources:
                yield _prepare(_)

        self.connect()
        sql = f"""
            INSERT OR REPLACE INTO {table_name} (key, resource_type, resource)
            VALUES (?, ?, ?)
        """
        
        try:
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

        return new_cursor.rowcount

    def load_from_ndjson_file(self, file_path, table_name="resources"):
        """Load the NDJSON file into the database."""
        if table_name not in self.table_created:
            self.create_table(table_name)

        with open(file_path, "r") as file:
            reader = ndjson.reader(file)
            self.bulk_insert_data(reader)

    def load_ndjson_from_dir(self, path: str = "META", pattern: str = "*.ndjson"):
        """Load all the NDJSON files in the directory into the database."""
        for file_path in pathlib.Path(path).glob(pattern):
            self.load_from_ndjson_file(file_path)

    @lru_cache(maxsize=None)
    def resource(self, resourceType, id) -> dict:
        """Return any resource with id and type specified."""
        cursor = self.connect()
        cursor.execute(
            "SELECT * FROM resources WHERE resource_type = ? AND key = ?",
            (resourceType, f"{resourceType}/{id}"),
        )
        _, _, resource = cursor.fetchone()
        resource = json.loads(resource)
        return self.simplify_extensions(resource)

    @staticmethod
    def simplify_extensions(resource: dict) -> dict:
        """Hoist extensions to top-level fields."""
        for extension in resource.get("extension", []):
            if "url" in extension:
                # Derive a key from the extension URL
                key = extension["url"].split("/")[-1]
                key = inflection.underscore(key).removeprefix("structure_definition_")
                
                # Extract the value from the extension
                value, _ = normalize_value(extension)
                if value is not None:
                    resource[key] = value
        return resource

    def flattened_document_references(self) -> Generator[dict, None, None]:
        """Generate flattened document references with associated observations."""
        # Get all observations by their focus document reference
        observation_by_focus_id = self._get_observations_by_focus()

        cursor = self.connect()
        cursor.execute(
            "SELECT * FROM resources WHERE resource_type = ?", ("DocumentReference",)
        )

        for _, _, raw_doc_ref in cursor.fetchall():
            doc_ref = json.loads(raw_doc_ref)
            yield self._flatten_document_reference(doc_ref, observation_by_focus_id)

    def _get_observations_by_focus(self) -> dict:
        """Get observations organized by their focus document reference ID."""
        observations_by_focus = defaultdict(list)
        cursor = self.connect()
        cursor.execute(
            "SELECT * FROM resources WHERE resource_type = ?", ("Observation",)
        )

        for _, _, raw_observation in cursor.fetchall():
            observation = json.loads(raw_observation)
            focus_refs = observation.get("focus", [])
            for focus in focus_refs:
                if "reference" in focus:
                    doc_ref_id = focus["reference"].split("/")[-1]
                    observations_by_focus[doc_ref_id].append(observation)

        return observations_by_focus

    def _flatten_document_reference(self, doc_ref: dict, observation_by_focus_id: dict) -> dict:
        """Flatten a single document reference with associated data."""
        # Simplify document reference
        flat_doc_ref = SimplifiedResource.build(resource=doc_ref).simplified

        # Extract the corresponding subject and append its fields
        flat_doc_ref.update(self._get_subject(doc_ref))

        # Populate observation data associated with the document reference
        if doc_ref["id"] in observation_by_focus_id:
            associated_observations = observation_by_focus_id[doc_ref["id"]]
            for observation in associated_observations:
                flat_observation = SimplifiedResource.build(resource=observation).simplified
                flat_doc_ref.update(flat_observation)

        # Handle basedOn references
        if "basedOn" in doc_ref:
            for i, based_on in enumerate(doc_ref["basedOn"]):
                flat_doc_ref[f"basedOn_{i}"] = based_on.get("reference", "")

        return flat_doc_ref

    def flattened_research_subjects(self) -> Generator[dict, None, None]:
        """Generate flattened research subjects with patient data."""
        cursor = self.connect()
        cursor.execute(
            "SELECT * FROM resources WHERE resource_type = ?", ("ResearchSubject",)
        )

        for _, _, raw_research_subject in cursor.fetchall():
            research_subject = json.loads(raw_research_subject)
            flat_research_subject = SimplifiedResource.build(resource=research_subject).simplified

            # Return with subject (Patient) fields
            patient = self._get_subject(research_subject)
            flat_research_subject.update(patient)

            yield flat_research_subject

    def flattened_medication_administrations(self) -> Generator[dict, None, None]:
        """Generate flattened medication administrations."""
        cursor = self.connect()
        cursor.execute(
            "SELECT * FROM resources WHERE resource_type = ?", ("MedicationAdministration",)
        )

        for _, _, raw_medication_administration in cursor.fetchall():
            medication_administration = json.loads(raw_medication_administration)
            flat_medication_administration = SimplifiedResource.build(
                resource=medication_administration
            ).simplified

            patient = self._get_subject(medication_administration)
            flat_medication_administration.update(patient)

            yield flat_medication_administration

    def flattened_specimens(self) -> Generator[dict, None, None]:
        """Generate flattened specimens."""
        cursor = self.connect()
        cursor.execute(
            "SELECT * FROM resources WHERE resource_type = ?", ("Specimen",)
        )

        for _, _, raw_specimen in cursor.fetchall():
            specimen = json.loads(raw_specimen)
            flat_specimen = SimplifiedResource.build(resource=specimen).simplified

            patient = self._get_subject(specimen)
            flat_specimen.update(patient)

            yield flat_specimen

    def flattened_group_members(self) -> Generator[dict, None, None]:
        """Generate flattened group members."""
        cursor = self.connect()
        cursor.execute(
            "SELECT * FROM resources WHERE resource_type = ?", ("Group",)
        )

        for _, _, raw_group in cursor.fetchall():
            group = json.loads(raw_group)
            simplified_group = SimplifiedGroup(resource=group)
            
            for member in simplified_group.members:
                # Extract entity reference details
                entity_ref = member.get("entity_reference", "")
                if "/" in entity_ref:
                    entity_type, entity_id = entity_ref.split("/", 1)
                    member_data = {
                        "group_id": group.get("id"),
                        "group_identifier": group.get("identifier", [{}])[0].get("value") if group.get("identifier") else None,
                        "entity_type": entity_type,
                        "entity_id": entity_id,
                        "entity_reference": entity_ref,
                        "inactive": member.get("inactive", False)
                    }
                    yield member_data

    def _get_subject(self, resource: dict) -> dict:
        """Get the resource's subject field if it exists."""
        subject_data = {}
        
        if "subject" in resource:
            subject_ref = resource["subject"]
            if isinstance(subject_ref, dict) and "reference" in subject_ref:
                reference = subject_ref["reference"]
                if "/" in reference:
                    resource_type, resource_id = reference.split("/", 1)
                    subject_data["subject"] = reference
                    
                    # Try to get the actual patient resource
                    try:
                        if resource_type == "Patient":
                            patient = self.resource("Patient", resource_id)
                            # Add patient fields with patient_ prefix
                            for key, value in patient.items():
                                if key not in ["id", "resourceType"]:
                                    subject_data[f"patient_{key}"] = value
                            subject_data["patient_id"] = resource_id
                    except:
                        # If patient not found, just use the reference
                        subject_data["patient_id"] = resource_id
        
        return subject_data


def create_dataframe(directory_path: str, work_path: str, data_type: str) -> pd.DataFrame:
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
        "Specimen": db.flattened_specimens,
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
            f"Dataframe is empty, are there any {data_type} resources?"
        )

    # Reorder columns for better presentation
    front_column_names = []
    if "identifier" in df.columns:
        front_column_names += ["identifier"]
    if "resourceType" in df.columns:
        front_column_names += ["resourceType"]
    if "patient" in df.columns:
        front_column_names = front_column_names + ["patient"]

    remaining_columns = [col for col in df.columns if col not in front_column_names]
    rear_column_names = ["id"]
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