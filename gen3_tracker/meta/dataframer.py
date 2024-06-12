import json
import pathlib
import sqlite3
import uuid
from functools import lru_cache
from typing import Generator, Optional, List, Tuple

import inflection
import ndjson
import numpy as np
import pandas as pd
from tqdm import tqdm
from copy import deepcopy
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
            name='resources',
            ddl='''
                    CREATE TABLE __NAME__ (
                        key TEXT PRIMARY KEY,
                        resource_type TEXT,
                        resource JSON
                    )
                '''):
        self.connect()
        # Check if the table exists before creating it
        self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{name}'")
        table_exists = self.cursor.fetchone()

        if not table_exists:
            ddl = ddl.replace('__NAME__', name)
            self.cursor.execute(ddl)
            self.table_created[name] = True

    def count(self, table_name='resources'):
        self.connect()
        self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = self.cursor.fetchone()[0]
        return count

    def insert_data(self, id_, resource_type, resource, table_name='resources'):
        """Insert data into the database."""
        if table_name not in self.table_created:
            self.create_table(table_name)  # Lazily create the table if not already created

        composite_key = f"{resource_type}/{id_}"
        self.cursor.execute(f'''
            INSERT INTO {table_name} (key, resource_type, resource)
            VALUES (?, ?, ?)
        ''', (composite_key, resource_type, json.dumps(resource)))
        # print(f"Inserted {composite_key} into the database")

    def insert_data_from_dict(self, resource, table_name='resources'):
        """Insert data into the database from a dictionary."""
        if 'id' not in resource or ('resource_type' not in resource and 'resourceType' not in resource):
            raise ValueError(f"Resource dictionary must contain 'id' and 'resource_type' keys {resource}")
        self.insert_data(
            resource['id'],
            resource.get('resource_type', resource.get('resourceType')),
            resource,
            table_name
        )

    def bulk_insert_data(self, resources, table_name='resources') -> int:
        """Bulk insert data into the database."""

        if table_name not in self.table_created:
            self.create_table(table_name)  # Lazily create the table if not already created

        def _prepare(resource):
            resource_type = resource.get('resource_type', resource.get('resourceType'))
            id_ = resource['id']
            composite_key = f"{resource_type}/{id_}"
            return (
                composite_key,
                resource_type,
                json.dumps(resource)
            )

        def _iterate(_resources):
            for _ in _resources:
                yield _prepare(_)

        try:
            self.connect()
            sql = f'''
                INSERT INTO {table_name} (key, resource_type, resource)
                VALUES (?, ?, ?)
            '''
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

    def load_from_ndjson_file(self, file_path, table_name='resources'):
        """Load the NDJSON file into the database."""

        if table_name not in self.table_created:
            self.create_table(table_name)  # Lazily create the table if not already created

        with open(file_path, 'r') as file:
            reader = ndjson.reader(file)
            self.bulk_insert_data(reader)

    def load_ndjson_from_dir(self, path: str = 'META', pattern: str = '*.ndjson'):
        """Load all the NDJSON files in the directory into the database."""
        for file_path in pathlib.Path(path).glob(pattern):
            self.load_from_ndjson_file(file_path)

    @lru_cache(maxsize=None)
    def patient_everything(self, patient_id) -> Generator[dict, None, None]:
        """Return all the resources for a patient."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM resources WHERE key = ?",
                       (f"Patient/{patient_id}",))

        for _ in cursor.fetchall():
            key, resource_type, resource = _
            yield json.loads(resource)

    @lru_cache(maxsize=None)
    def patient(self, patient_id) -> dict:
        """Return the patient resource."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM resources WHERE key = ?", (f"Patient/{patient_id}",))
        _ = cursor.fetchone()
        if _ is None:
            print(f"Patient {patient_id} not found")
            return None
        key, resource_type, resource = _
        resource = json.loads(resource)

        resource = self.simplify_extensions(resource)

        return resource

    @staticmethod
    def simplify_extensions(resource: dict) -> dict:
        """Extract extension values, derive key from extension url"""
        for _ in resource.get('extension', []):
            value_normalized, value_source = normalize_value(_)
            extension_key = _['url'].split('/')[-1]
            extension_key = inflection.underscore(extension_key).removesuffix(".json").removeprefix("structure_definition_")
            resource[extension_key] = value_normalized
            assert value_normalized, f"extension: {extension_key} = {value_normalized}"
        if 'extension' in resource:
            del resource['extension']
        return resource

    @staticmethod
    def attach_extension(resource: dict, target_dict: dict) -> dict:
        """Extract extension values, derive key from extension url"""
        print("RESOURCE: ", resource)
        for _ in resource.get('extension', []):
            value_normalized, value_source = normalize_value(_)
            extension_key = _['url'].split('/')[-1]
            extension_key = inflection.underscore(extension_key).removesuffix(".json").removeprefix("structure_definition_")
            target_dict[extension_key] = value_normalized
            assert value_normalized, f"extension: {extension_key} = {value_normalized}"
        if 'extension' in resource:
            del resource['extension']
        return target_dict

    @lru_cache(maxsize=None)
    def flattened_procedure(self, procedure_key) -> dict:
        """Return the procedure with everything resolved."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM resources WHERE key = ?", (procedure_key,))
        key, resource_type, resource = cursor.fetchone()
        procedure = json.loads(resource)

        # simplify the identifier
        procedure['identifier'] = procedure['identifier'][0]['value']
        # simplify the code
        procedure['code'] = procedure['code']['coding'][0]['display']
        # simplify the reason
        procedure['reason'] = procedure['reason'][0]['reference']['reference']
        # simplify the occurrenceAge
        procedure['occurrenceAge'] = procedure['occurrenceAge']['value']
        # simplify the subject
        subject = procedure['subject']['reference']
        procedure['subject'] = subject
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

        if 'collection' in specimen:
            for coding_normalized, coding_source in normalize_coding(specimen['collection']):
                specimen[f"collection_{coding_source}"] = coding_normalized
            del specimen['collection']
        if 'processing' in specimen:
            for processing in specimen.get('processing', []):
                for coding_normalized, coding_source in normalize_coding(processing):
                    specimen[f"processing_{coding_source}"] = coding_normalized
                break # TODO - only first one
            del specimen['processing']

        return specimen

    @lru_cache(maxsize=None)
    def flattened_condition(self, condition_key) -> dict:
        """Return the procedure with everything resolved."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM resources WHERE key = ?", (condition_key,))
        key, resource_type, resource = cursor.fetchone()
        condition = json.loads(resource)

        # simplify the identifier
        condition['identifier'] = condition['identifier'][0]['value']
        # simplify the code
        condition['code'] = condition['code']['coding'][0]['display']
        for coding_normalized, coding_source in normalize_coding(condition):
            condition[coding_source] = coding_normalized
        # simplify the onsetAge
        condition['onsetAge'] = condition['onsetAge']['value']
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
        cursor.execute("SELECT * FROM resources where resource_type = ?", ("Procedure",))

        for _ in cursor.fetchall():
            key, resource_type, procedure = _
            procedure = json.loads(procedure)
            # simplify the identifier
            procedure['identifier'] = procedure['identifier'][0]['value']
            # simplify the code
            procedure['code'] = procedure['code']['coding'][0]['display']
            # simplify the reason
            procedure['reason'] = procedure['reason'][0]['reference']['reference']
            # simplify the occurrenceAge
            procedure['occurrenceAge'] = procedure['occurrenceAge']['value']
            # simplify the subject
            subject = procedure['subject']['reference']
            procedure['subject'] = subject

            if subject.startswith('Patient/'):
                _, patient_id = subject.split('/')
                resources = [_ for _ in loaded_db.patient_everything(patient_id)]
                resources.append(loaded_db.patient(patient_id))
                for resource in resources:

                    if resource['resourceType'] == 'Patient':
                        procedure['patient'] = resource['identifier'][0]['value']
                        continue

                    if resource['resourceType'] == 'Condition' and f"Condition/{resource['id']}" == procedure['reason']:
                        procedure['reason'] = resource['code']['text']
                        continue

                    if resource['resourceType'] == 'Observation':
                        # must be focus
                        if f"Procedure/{procedure['id']}" not in [_['reference'] for _ in resource['focus']]:
                            continue

                        # TODO - pick first coding, h2 allow user to specify preferred coding
                        code = resource['code']['coding'][0]['code']

                        if 'valueQuantity' in resource:
                            value = resource['valueQuantity']['value']
                        elif 'valueCodeableConcept' in resource:
                            value = resource['valueCodeableConcept']['text']
                        elif 'valueInteger' in resource:
                            value = resource['valueInteger']
                        elif 'valueString' in resource:
                            value = resource['valueString']
                        else:
                            value = None

                        assert value is not None, f"no value for {resource['id']}"
                        procedure[code] = value

                        continue

                    # skip these
                    if resource['resourceType'] in ['Specimen', 'Procedure', 'ResearchSubject']:
                        continue

                    # default, add entire resource as an item of the list
                    resource_type = inflection.underscore(resource['resourceType'])
                    if resource_type not in procedure:
                        procedure[resource_type] = []
                    procedure[resource_type].append(resource)

            yield procedure

        connection.close()

    def flattened_observations(self) -> Generator[dict, None, None]:
        loaded_db = self
        connection = sqlite3.connect(loaded_db.db_name)
        cursor = connection.cursor()
        cursor.execute('''
            SELECT
                json_extract(resource, '$.subject') as subject,
                json(resource) as observation
            FROM resources
            WHERE resource_type = ?
            ORDER BY subject
        ''', ("Observation",))

        # Initialize an empty list to store the dictionaries
        # Fetch rows one by one and process them

        previous_observation, patient = {}, {}
        for i, row in enumerate(cursor):
            observation = json.loads(row[1])
            if i == 0:
                patient_id = str(observation["subject"]["reference"]).removeprefix("Patient/")
                patient = loaded_db.patient(patient_id)
                patient['identifier'] = self.get_nested_value(patient, ['identifier', 0, 'value'])

            if i > 0 and observation["subject"] != previous_observation["subject"]:
                yield patient
                patient_id = str(observation["subject"]["reference"]).removeprefix("Patient/")
                patient = loaded_db.patient(patient_id)
                patient['identifier'] = self.get_nested_value(patient, ['identifier', 0, 'value'])

            value_normalized, _ = normalize_value(observation)
            for coding_normalized, coding_source in normalize_coding(observation):
                patient[coding_normalized[0]] = value_normalized

            # renormalize the value in components
            if observation.get('component', []):
                for component in observation.get('component', []):
                    codings = normalize_coding(component)
                    for _ in codings:
                        coding_normalized, coding_source = _
                        if coding_source == 'code':
                            value_normalized, value_source = normalize_value(component)
                            if isinstance(coding_normalized, list) and len(coding_normalized) > 1:
                                print("CODING NORMALIZED: ", coding_normalized)
                            patient[coding_normalized[0]] = value_normalized
                            break

            previous_observation = observation

        connection.close()

    def flattened_document_references(self) -> Generator[dict, None, None]:
        loaded_db = self
        connection = sqlite3.connect(loaded_db.db_name)
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM resources where resource_type = ?", ("DocumentReference",))

        for _ in cursor.fetchall():
            key, resource_type, procedure = _
            document_reference = json.loads(procedure)

            # simplify the subject

            subject = document_reference.get('subject', {'reference': None})['reference']
            document_reference['subject'] = subject

            #In some places like TCGA-LUAD there is more than one identifier that could be displayed
            document_reference['identifier'] = document_reference.get('identifier', [{'value': None}])[0]['value']

            for elem in normalize_coding(document_reference):
                document_reference[elem[1]] = elem[0][0]

            # simplify the extensions
            if self.get_nested_value(document_reference, ['content', 0, 'attachment']) is not None:
                if "extension" in document_reference['content'][0]['attachment']:
                    for _ in document_reference['content'][0]['attachment']['extension']:
                        value_normalized, value_source = normalize_value(_)
                        document_reference[(_['url'].split('/')[-1])] = value_normalized

                content_url = self.get_nested_value(document_reference, ['content', 0, 'attachment', 'url'])
                if content_url is not None:
                    document_reference['source_url'] = content_url

            if "content" in document_reference:
                for k, v in document_reference['content'][0]['attachment'].items():
                    if k in ['extension']:
                        continue
                    document_reference[k] = v

            if "basedOn" in document_reference:
                for i, dict_ in enumerate(document_reference['basedOn']):
                    document_reference['basedOn'][i] = dict_["reference"]

            if subject is not None and subject.startswith('Patient/'):
                _, patient_id = subject.split('/')
                resources = [_ for _ in loaded_db.patient_everything(patient_id)]
                resources.append(loaded_db.patient(patient_id))
                for resource in resources:

                    if resource['resourceType'] == 'Patient':
                        document_reference['patient'] = resource['identifier'][0]['value']
                        continue

                    if resource['resourceType'] == 'Condition' and f"Condition/{resource['id']}" == procedure['reason']:
                        document_reference['reason'] = resource['code']['text']
                        continue

                    if resource['resourceType'] == 'Observation':
                        # must be focus
                        if f"Procedure/{procedure['id']}" not in [_['reference'] for _ in resource['focus']]:
                            continue

                        # TODO - pick first coding, h2 allow user to specify preferred coding
                        code = resource['code']['coding'][0]['code']

                        value = normalize_value(resource)

                        assert value is not None, f"no value for {resource['id']}"
                        document_reference[code] = value

                        continue

                    # skip these
                    if resource['resourceType'] in ['Specimen', 'Procedure', 'ResearchSubject', 'DocumentReference']:
                        continue

                    # default, add entire resource as an item of the list
                    resource_type = inflection.underscore(resource['resourceType'])
                    if resource_type not in procedure:
                        document_reference[resource_type] = []
                    document_reference[resource_type].append(resource)

            del document_reference['content']
            yield document_reference

        connection.close()


def create_dataframe(directory_path: str, work_path: str, data_type: str) -> pd.DataFrame:
    """Create a dataframe from the FHIR data in the directory."""
    assert pathlib.Path(work_path).exists(), f"Directory {work_path} does not exist."
    work_path = pathlib.Path(work_path)
    db_path = (work_path / "local_fhir.db")
    db_path.unlink(missing_ok=True)

    db = LocalFHIRDatabase(db_name=db_path)
    db.load_ndjson_from_dir(path=directory_path)

    if data_type == "DocumentReference":
        df = pd.DataFrame(db.flattened_document_references())
    elif data_type == "Observation":
        df = pd.DataFrame(db.flattened_observations())
    else:
        raise ValueError(f"{data_type} not supported yet. Supported data types are DocumentReference and Observation")
    assert not df.empty, "Dataframe is empty, are there any DocumentReference resources?"

    front_column_names = ["resourceType", "identifier"]
    if "patient" in df.columns:
        front_column_names = front_column_names + ["patient"]

    remaining_columns = [col for col in df.columns if col not in front_column_names]
    rear_column_names = ["id"]  # removed status for the purpose of not needing it for the demo
    if "subject" in df.columns:
        rear_column_names = rear_column_names + ["subject"]
    for c in df.columns:
        if c.endswith("_identifier"):
            rear_column_names.append(c)
    remaining_columns = [col for col in remaining_columns if col not in rear_column_names]

    reordered_columns = front_column_names + remaining_columns + rear_column_names
    df = df[reordered_columns]
    df = df.replace({np.nan: ''})
    return df


def normalize_value(resource_dict: dict) -> tuple[Optional[str], Optional[str]]:
    """return a tuple containing the normalized value and the name of the field it was derived from"""
    value_normalized = None
    value_source = None

    if set(resource_dict.keys()) == set(['url', 'extension']):
        assert len(resource_dict['extension']) > 0, f"Expected at least on extension, in {resource_dict}"
        return normalize_value(resource_dict['extension'][0])
    if 'valueQuantity' in resource_dict:
        value = resource_dict['valueQuantity']
        value_normalized = f"{value['value']} {value.get('unit', '')}"
        value_source = 'valueQuantity'
    elif 'valueCodeableConcept' in resource_dict:
        value = resource_dict['valueCodeableConcept']
        value_normalized = ' '.join([coding['display'] for coding in value.get('coding', [])])
        value_source = 'valueCodeableConcept'
    elif 'valueCoding' in resource_dict:
        value = resource_dict['valueCoding']
        value_normalized = value['display']
        value_source = 'valueCoding'
    elif 'valueString' in resource_dict:
        value_normalized = resource_dict['valueString']
        value_source = 'valueString'
    elif 'valueCode' in resource_dict:
        value_normalized = resource_dict['valueCode']
        value_source = 'valueCode'
    elif 'valueBoolean' in resource_dict:
        value_normalized = str(resource_dict['valueBoolean'])
        value_source = 'valueBoolean'
    elif 'valueInteger' in resource_dict:
        value_normalized = str(resource_dict['valueInteger'])
        value_source = 'valueInteger'
    elif 'valueRange' in resource_dict:
        value = resource_dict['valueRange']
        low = value['low']
        high = value['high']
        value_normalized = f"{low['value']} - {high['value']} {low.get('unit', '')}"
        value_source = 'valueRange'
    elif 'valueRatio' in resource_dict:
        value = resource_dict['valueRatio']
        numerator = value['numerator']
        denominator = value['denominator']
        value_normalized = f"{numerator['value']} {numerator.get('unit', '')}/{denominator['value']} {denominator.get('unit', '')}"
        value_source = 'valueRatio'
    elif 'valueSampledData' in resource_dict:
        value = resource_dict['valueSampledData']
        value_normalized = value['data']
        value_source = 'valueSampledData'
    elif 'valueTime' in resource_dict:
        value_normalized = resource_dict['valueTime']
        value_source = 'valueTime'
    elif 'valueDateTime' in resource_dict:
        value_normalized = resource_dict['valueDateTime']
        value_source = 'valueDateTime'
    elif 'valuePeriod' in resource_dict:
        value = resource_dict['valuePeriod']
        value_normalized = f"{value['start']} to {value['end']}"
        value_source = 'valuePeriod'
    # for debugging...
    # else:
    #     raise ValueError(f"value[x] not found in {resource_dict}")

    return value_normalized, value_source


def normalize_coding(resource_dict: dict) -> List[Tuple[str, str]]:
    def extract_coding(coding_list):
        # return a concatenated string
        # return ','.join([coding.get('display', '') for coding in coding_list if 'display' in coding])
        # or alternatively return an array
        return [coding.get('display', coding.get('code', '')) for coding in coding_list]

    def find_codings_in_dict(d: dict, parent_key: str = '') -> List[Tuple[str, str]]:
        codings = []
        for key, value in d.items():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        # Check if the dict contains a 'coding' list
                        if 'coding' in item and isinstance(item['coding'], list):
                            coding_string = extract_coding(item['coding'])
                            codings.append((coding_string, key))
                        # Recursively search in the dict
                        codings.extend(find_codings_in_dict(item, key))
            elif isinstance(value, dict):
                # Check if the dict contains a 'coding' list
                if 'coding' in value and isinstance(value['coding'], list):
                    coding_string = extract_coding(value['coding'])
                    codings.append((coding_string, key))
                # Recursively search in the dict
                codings.extend(find_codings_in_dict(value, key))
        return codings

    return find_codings_in_dict(resource_dict)


def is_number(s):
    """ Returns True if string is a number. """
    try:
        complex(s)
        return True
    except ValueError:
        return False
