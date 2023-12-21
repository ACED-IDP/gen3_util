#### configuration


* Each resource in scope has a list of `editable columns`,  the structure of those names is defined by [flatten()](https://github.com/amirziai/flatten?tab=readme-ov-file#usage)

* Resources not on the list will be ignored, not part of the tabular workflow
  * If no key is found for {program-project}: resourceType, look in {program}: resourceType else look in default: resourceType otherwise resource not supported.

* Resources that are not editable are:
    * DocumentReference: is based on the outcome of file upload and is immutable
    * Program and Project: part of gen3 authorization are immutable


```yaml
configuration:
  default:
    Patient: [editable column names]
    Specimen: [editable column names]
    Observation: [editable column names]
    ResearchStudy: [editable column names]
    ResearchSubject: [editable column names]
  programs:
    <program-name>:
      # defaults for a program go here
  projects:
    <program-project>:
    # defaults for a program-project go here

```

A Configuration class has a method `get_config` that returns a dictionary of editable columns for a given resourceType.

The method looks for the resourceType in the following order:

* {program-project}: resourceType
* {program}: resourceType
* default: resourceType



#### to_tabular

This naive pseudo-Python code defines a function `to_table` that takes an object as input
and converts its data into tabular columns represented as a dictionary.

see https://pypi.org/project/flatten-json/


```python
def to_tabular(resource: dict, config: Config, project_id: str) -> dict:
    """Convert a resource to a tabular representation.

    Args:
        resource (dict): A resource in JSON format.
        config (Config): A configuration object.
        project_id (str): The project ID.

    Returns:
        dict: A tabular representation of the resource.

    Pseudo Code:
        1. get the resourceType from resource, lookup config, raise exception if not found
        2. get the editable columns from config
        3. flatten the resource, remove keys not in editable columns
        4. return the flattened resource
    """
    pass
```


#### from_tabular

This naive pseudo-Python code defines a function `from_table` that takes a tabular object as input and merges it with the original resource.

see https://pypi.org/project/deepmerge/

```python
def from_tabular(tabular: dict, config: Config, project_id: str, resource: dict) -> dict:
    """Convert a tabular representation of a resource to a resource. (mock for testing)

    Args:
        tabular (dict): A tabular (edited) representation of a resource.
        config (Config): A configuration object.
        project_id (str): The project ID.
        resource (immutable dict): The original resource.

    Returns:
        dict: A merged resource

    Pseudo Code:
        1. get the resourceType from resource, lookup config, raise exception if not found
        2. get the editable columns from config, raise exception if tabular contains extraneous keys
        3. unflatten the resource,
        4. return merger of the original resource and the unflattened tabular

    """
    pass
```

### validation

The validation module contains functions that validate a resource against a schema.

```python
def validate(resource_dict: dict) -> ParseResult:
    """Validate a resource. (mock for testing)

    Args:
        resource_dict (dict): A resource in JSON format.

    Returns:
        ParseResult: The validated resource.

    Pseudo Code:
        1. raise exception if resource_dict does not contain a resourceType
        2. validate the resource against the schema for the resourceType
        3. return a parse result that contains the validated resource and any validation exception

    """
    pass

```


### workflow

The tabular methods above need to be incorporated into a command line workflow.
The documentation for these commands should be maintained in the [user facing documentation here](https://github.com/ACED-IDP/aced-idp.github.io/blob/0b9b33dc0542795e756219ef8305dd2e33bcc173/docs/workflows/metadata.md#L20-L19)


Pseudo Code:
1. Download [all | subset] existing metadata from the portal e.g. `gen3_util meta cp --profile=aced --project_id=aced-test <PATH-TO-FHIR>`
2. Convert the FHIR data to tabular form. e.g. `gen3_util meta to_tabular <PATH-TO-FHIR> <PATH-TO-TABULAR>`
3. Edit the tabular data
4. Convert the tabular data to FHIR and validate. e.g. `gen3_util meta from_tabular <PATH-TO-TABULAR> <PATH-TO-FHIR>`
5. Upload the metadata to the portal e.g. `gen3_util meta publish --profile=aced --project_id=aced-test <PATH-TO-FHIR>`

### testing

  `test/unit/tabular/` contains tests for the tabular module, please add tests for any new functions you add to the module.

### unresolved issues

  * How to define `a subset` of the project's metadata? Currently, for upload a `manifest` is used to define the subset of files to upload.
  * How to define a reasonable list of `editable columns` for each resource type? Currently, the list is defined in the configuration file.  However, the contents of each list needs to be defined.
