# Calypr Dataframer Repository

This directory contains the new `@calypr/dataframer` package extracted from the gen3_util repository. The package focuses exclusively on the metadata dataframe generation functionality.

## Repository Structure

```
calypr_dataframer/
├── __init__.py           # Package initialization
├── cli.py               # Command line interface  
├── dataframer.py        # Core dataframe generation logic
└── entities.py          # FHIR resource simplification utilities

tests/
├── __init__.py
├── test_dataframer.py   # Tests for core functionality
└── test_entities.py     # Tests for entity utilities

setup.py                 # Package setup configuration
pyproject.toml           # Modern Python packaging configuration
requirements.txt         # Core dependencies
README.md                # Package documentation
LICENSE                  # MIT license
.gitignore              # Git ignore patterns
```

## Key Features Extracted

1. **LocalFHIRDatabase**: SQLite-based local FHIR data processing
2. **create_dataframe()**: Main function for generating dataframes from FHIR metadata
3. **SimplifiedResource**: FHIR resource flattening and normalization
4. **CLI Interface**: Command-line tool with dataframe generation command
5. **Multiple Resource Support**: DocumentReference, ResearchSubject, MedicationAdministration, Specimen, GroupMember

## What Was Removed

- All gen3-specific functionality (projects, collaborators, git operations)
- Gen3 client dependencies
- Complex configuration management
- Non-dataframe related CLI commands
- Gen3-specific authentication and profile management

## Dependencies Simplified

The new package has minimal dependencies:
- pandas, numpy (data processing)
- click (CLI)
- pydantic (data validation)
- ndjson, inflection, deepmerge (data processing utilities)

## Usage

```bash
# Install the package
pip install -e .

# Generate dataframe
calypr-dataframer dataframe DocumentReference ./META

# Interactive exploration
calypr-dataframer dataframe --dtale Specimen ./META
```

This creates a focused, lightweight tool specifically for FHIR metadata dataframe generation.