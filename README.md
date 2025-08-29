# Calypr Dataframer

A specialized tool for generating dataframes from FHIR metadata. This package extracts the dataframe functionality from the gen3_util repository and provides a focused, lightweight solution for FHIR metadata processing.

## Features

- Generate structured dataframes from FHIR metadata
- Support for multiple FHIR resource types:
  - DocumentReference
  - ResearchSubject  
  - MedicationAdministration
  - Specimen
  - GroupMember
- Interactive data exploration with dtale (optional)
- CSV export functionality
- Built-in FHIR resource flattening and normalization

## Installation

```bash
pip install calypr-dataframer
```

### Optional: Interactive Data Exploration

For interactive data exploration capabilities:

```bash
pip install calypr-dataframer[dtale]
```

## Quick Start

### Basic Usage

```bash
# Generate a CSV dataframe from DocumentReference resources
calypr-dataframer dataframe DocumentReference ./META

# Generate a CSV with custom output path
calypr-dataframer dataframe Specimen ./META my_specimens.csv

# Interactive exploration with dtale
calypr-dataframer dataframe ResearchSubject ./META --dtale
```

### Directory Structure

The tool expects FHIR metadata files in NDJSON format:

```
META/
├── DocumentReference.ndjson
├── ResearchSubject.ndjson
├── Specimen.ndjson
├── Patient.ndjson
└── ...
```

### Command Line Interface

```bash
calypr-dataframer dataframe --help
```

**Arguments:**
- `DATA_TYPE`: The type of FHIR resource to process (required)
  - Options: Specimen, DocumentReference, ResearchSubject, MedicationAdministration, GroupMember
- `DIRECTORY_PATH`: Path to metadata directory (default: ./META)
- `OUTPUT_PATH`: Output CSV file path (default: {DATA_TYPE}.csv)

**Options:**
- `--dtale`: Launch interactive data exploration in browser
- `--debug`: Enable debug mode for troubleshooting

## Python API

```python
from calypr_dataframer.dataframer import create_dataframe
import tempfile

# Create dataframe from FHIR metadata
with tempfile.TemporaryDirectory() as temp_dir:
    df = create_dataframe(
        directory_path="./META",
        work_path=temp_dir,
        data_type="DocumentReference"
    )
    
    print(df.head())
    df.to_csv("output.csv", index=False)
```

## Supported FHIR Resources

### DocumentReference
- Flattens document metadata
- Includes associated Observation resources
- Links to subject Patient data

### ResearchSubject  
- Research study participant information
- Linked Patient demographics
- Enrollment details

### MedicationAdministration
- Medication administration events
- Patient linkage
- Dosage and timing information

### Specimen
- Biological specimen metadata
- Patient source information
- Collection and processing details

### GroupMember
- Group membership relationships
- Entity references
- Active/inactive status

## Data Processing Features

- **Resource Flattening**: Converts nested FHIR structures to flat tabular format
- **Reference Resolution**: Automatically resolves Patient references
- **Extension Handling**: Extracts and normalizes FHIR extensions
- **Coding Normalization**: Standardizes coded values and displays
- **Column Reordering**: Optimizes column order for better readability

## Requirements

- Python 3.8+
- pandas
- numpy  
- click
- pydantic
- ndjson
- inflection
- deepmerge

## Development

### Setup Development Environment

```bash
git clone https://github.com/calypr/dataframer
cd dataframer
pip install -r requirements.txt
pip install -e .
```

### Running Tests

```bash
pytest tests/
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Origins

This package extracts and focuses the dataframe functionality from the [gen3_util](https://github.com/ACED-IDP/gen3_util) repository, providing a lightweight, specialized tool for FHIR metadata processing.