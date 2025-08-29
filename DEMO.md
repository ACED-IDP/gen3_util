# Calypr Dataframer Demo

This demo shows how the calypr_dataframer package would be used once dependencies are installed.

## Package Installation

```bash
# Install the package in development mode
pip install -e .

# Or install with all dependencies
pip install -r requirements.txt
```

## Sample Usage

### Command Line Interface

```bash
# Generate DocumentReference dataframe
calypr-dataframer dataframe DocumentReference ./META

# Generate with custom output
calypr-dataframer dataframe Specimen ./META specimens.csv

# Interactive exploration  
calypr-dataframer dataframe ResearchSubject ./META --dtale

# Show help
calypr-dataframer --help
calypr-dataframer dataframe --help
```

### Python API

```python
import tempfile
from calypr_dataframer.dataframer import create_dataframe

# Create dataframe from FHIR metadata
with tempfile.TemporaryDirectory() as work_dir:
    df = create_dataframe(
        directory_path="./META",
        work_path=work_dir,
        data_type="DocumentReference"  
    )
    
    print(f"Generated dataframe with {len(df)} rows and {len(df.columns)} columns")
    print(f"Columns: {list(df.columns)}")
    
    # Save to CSV
    df.to_csv("output.csv", index=False)
```

### Expected Directory Structure

```
./META/
├── DocumentReference.ndjson
├── ResearchSubject.ndjson  
├── Specimen.ndjson
├── Patient.ndjson
├── MedicationAdministration.ndjson
└── Group.ndjson
```

### Supported Data Types

- `DocumentReference` - Document metadata with linked observations
- `ResearchSubject` - Research participants with patient data
- `MedicationAdministration` - Medication events with patient context  
- `Specimen` - Biological specimens with patient source
- `GroupMember` - Group membership relationships

## Key Features Demonstrated

1. **Resource Flattening**: Converts nested FHIR to flat tables
2. **Reference Resolution**: Links Patient data to other resources
3. **Extension Processing**: Extracts FHIR extensions as columns
4. **Coding Normalization**: Standardizes coded values
5. **Column Optimization**: Reorders columns for readability

## Example Output

A DocumentReference dataframe might include columns like:
- `identifier`, `resourceType`, `patient_id`
- `status`, `type`, `category`  
- `patient_name`, `patient_birthDate`
- `subject`, `id`

The package focuses exclusively on dataframe generation, making it lightweight and purpose-built for FHIR metadata analysis.