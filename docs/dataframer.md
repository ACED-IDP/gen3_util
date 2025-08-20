# What `dataframer.py` is for

A lightweight local FHIR “warehouse” built on SQLite that can ingest NDJSON FHIR bundles, merge/upsert resources, and expose convenient accessors to:

- fetch specific resources (`resource(type, id)`), a patient (`patient(id)`), or “everything” tied to a patient (`patient_everything`)
- flatten common resources (e.g., `flattened_procedure`, `flattened_condition`, `flattened_procedures`) into analysis-friendly dictionaries
- normalize extensions and common FHIR value/coding structures for tabular use (e.g., pandas/Elasticsearch).

---

## Key components & methods

### Storage

- `LocalFHIRDatabase(db_name)`: wraps a SQLite DB; lazily creates tables.
- `create_table(name="resources")`: creates table with schema `(key TEXT PRIMARY KEY, resource_type TEXT, resource JSON)`.
- `insert_data(id_, resource_type, resource)`: merges with existing (via `deepmerge.always_merger`) before insert (an upsert).
- Bulk/file loaders: `bulk_insert_data(resources)`, `load_from_ndjson_file(path)`, `load_ndjson_from_dir(path="META", pattern="*.ndjson")`.

### Reading / lookup

- `resource(resourceType, id)`: returns JSON (extensions simplified).
- `patient(patient_id)`: returns simplified Patient.
- `patient_everything(patient_id)`: generator over all rows with `key = Patient/{id}`.
- `condition_everything()`: returns all Condition resources.
- `simplify_extensions(resource)`: hoists each `extension` into top-level fields with normalized keys.

### Flatteners / denormalizers

- `flattened_procedure(procedure_key)`: normalizes common fields (identifier, code, reason, occurrenceAge, subject).
- `flattened_condition(condition_key)`: normalizes identifiers, category/code (SNOMED preferred), and collects additional code strings.
- `flattened_procedures()`: iterates Procedures and enriches each row with related Patient/Condition/Observation values; Observation codes become columns; other linked resources are attached with underscored type names.

### Helpers

- `handle_units(value_normalized)`: strips unit suffixes and coerces numeric strings to floats.
- `select_category`, `select_coding`: prefer SNOMED (`http://snomed.info/sct`) when present.

---

## Internal dependencies

- **`gen3_tracker.ACED_NAMESPACE`**
  A UUIDv3 namespace for `aced-idp.org`; imported for use in generating deterministic IDs.

- **`gen3_tracker.meta.entities`**
  Provides the utility functions and simplifier classes used by `dataframer.py`. See below for detail.

---

## gen3_tracker.meta.entities (deep dive)

### Core utilities

- **`get_nested_value(d, keys)`** – Safe traversal over dict/list structures.
- **`normalize_value(resource_dict)`** – Extracts a single usable value from `value[x]`, with `(value, source_key)`.
- **`normalize_coding(x)`** – Consolidates codings across codeable concepts; SNOMED-first; returns primary display/code/system + all codings.
- **`traverse(resource, prefix="")`** – Flattens JSON into audit-friendly key/value pairs with dotted paths.

### Simplifier classes

All subclasses of `SimplifiedFHIR` expose a uniform surface:
- `.simplified: dict` → curated, stable keys (id, type, subject, codes, categories, etc.)
- `.values: dict[str, Any]` → atomic/numeric values keyed by code (Observations, components)
- `.codings: dict` → normalized coding bundle
- `.identifiers: list` → structured identifiers

**Specializations:**

- `SimplifiedObservation` – handles panels, components, values, specimen refs.
- `SimplifiedCondition` – handles status, onset, recordedDate, coding.
- `SimplifiedDocumentReference` – handles attachments, context, relatesTo.
- `SimplifiedMedicationAdministration` – handles medication, dose, timing.
- `SimplifiedGroup` – handles members, type, count.
- `SimplifiedSpecimen` – handles hierarchy (parent/derivedFrom), type, container, treatments.

**Factory:**
`SimplifiedResource.build(resource)` picks the right simplifier by `resourceType`.

---

## Notable behaviors / gotchas

- **Upsert merge:** existing JSON is merged with incoming resource before insert—be mindful of deepmerge semantics.
- **Extension hoisting:** extensions become top-level fields keyed by a normalized suffix of the URL.
- **SNOMED preference:** `select_coding` and `normalize_coding` choose SNOMED displays first.
- **Observation flattening:** `flattened_procedures()` uses Observation codes (or component codes) as column names—missing values assert.
- **Unit normalization:** numeric strings like `"12 mg"` coerced to `12.0`, units preserved separately.

---


## Appendix: Field Map (Aligned to `simplified_resources`)

This appendix maps FHIR JSON paths to the **simplified keys** asserted in the
unit-test fixture `simplified_resources`.

| Resource | FHIR JSON path(s) | Simplified key (fixture) | Notes on transformation |
|---|---|---|---|
| **Patient** | `id` | `id` | Resource id |
|  | `resourceType` | `resourceType` | Always `"Patient"` |
|  | `identifier[0].value` (or best available) | `identifier` | Collapsed to single string |
|  | `active` | `active` | Boolean carried through |
| **Specimen** | `id` | `id` |  |
|  | `resourceType` | `resourceType` | `"Specimen"` |
|  | `identifier[0].value` | `identifier` | Collapsed string |
|  | `collection.bodySite.coding[0].display` (or `.text`) | `collection` | Display text (e.g., “Breast”) |
|  | `processing[0].procedure.coding[0].display` (or `processing[0].description`) | `processing` | Display/description (e.g., “Double‑Spun”) |
| **DocumentReference** | `id` | `id` |  |
|  | `resourceType` | `resourceType` | `"DocumentReference"` |
|  | `identifier[0].value` | `identifier` | Collapsed string (UUID in fixture) |
|  | `status` | `status` | e.g., `"current"` |
|  | `docStatus` | `docStatus` | Document status enum |
|  | `date` | `date` | Top‑level date |
|  | `content[0].attachment.hash` | `md5` | MD5 digest exposed as `md5` |
|  | `content[0].attachment.url` | `source_path` | Path/URL duplicated as `source_path` |
|  | `content[0].attachment.contentType` | `contentType` | MIME type |
|  | `content[0].attachment.size` | `size` | Bytes (int) |
|  | `content[0].attachment.url` | `url` | URL (same as `source_path` in fixture) |
|  | `content[0].attachment.title` | `title` | Filename/label |
|  | `content[0].attachment.creation` | `creation` | Attachment creation timestamp |
| **Observation** | `id` | `id` |  |
|  | `resourceType` | `resourceType` | `"Observation"` |
|  | `identifier[0].value` (or composed id string) | `identifier` | Collapsed string |
|  | `status` | `status` | `"final"` in fixture |
|  | `category[0].coding[0].display` | `category` | `"Laboratory"` in fixture |
|  | `effectiveDateTime` | `effectiveDateTime` | Kept under original key per fixture |
|  | `valueCodeableConcept.text` (or preferred display) | `valueCodeableConcept` | Collapsed to a single string |
|  | `component[*]`/extensions mapped by label | `sequencer`, `index`, `type`, `project_id`, `read_length`, `instrument_run_id`, `capture_bait_set`, `end_type`, `capture`, `sequencing_site`, `construction` | Labels become top‑level scalar keys |
|  | Sample metadata attributes | `sample_type`, `library_id`, `observation_code`, `tissue_type`, `treatments`, `allocated_for_site`, `indexed_collection_date`, `biopsy_specimens`, `biopsy_procedure_type`, `biopsy_anatomical_location`, `percent_tumor` | Promoted to top‑level keys |
|  | Gene panel attributes | `Gene`, `Chromosome`, `result` | Promoted from component codes/values |

**Notes**
- `identifier` is a **single string** for each resource in the fixture.
- Document attachments are **flattened directly** on `DocumentReference`.
- Observation keeps some original FHIR element names (e.g., `effectiveDateTime`) and
promotes many coded values/labels to top-level scalar keys.
