"""Simplified FHIR entities for dataframe generation."""

import inflection
import re
from pydantic import BaseModel, computed_field
from typing import Dict, List, Optional, Tuple


#######################
# FHIR HELPER METHODS #
#######################


def get_nested_value(d: dict, keys: list):
    """Safely navigate nested dictionary/list structures."""
    for key in keys:
        try:
            d = d[key]
        except (KeyError, IndexError, TypeError):
            return None
    return d


def normalize_coding(resource_dict: Dict) -> List[Tuple[str, str]]:
    """Extract normalized coding information from FHIR resource."""

    def extract_coding(coding_list):
        # return a concatenated string or alternatively return an array
        return [coding.get("display", coding.get("code", "")) for coding in coding_list]

    def find_codings_in_dict(d: dict, parent_key: str = "") -> list[tuple[str, str]]:
        codings = []
        for key, value in d.items():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        # Check if the dict contains a 'coding' list
                        if "coding" in item and isinstance(item["coding"], list):
                            coding_string = extract_coding(item["coding"])
                            codings.append((coding_string, key))
                        if "code" in item:
                            coding_string = item.get("display", item.get("code"))
                            codings.append((coding_string, key))

                        # Recursively search in the dict
                        codings.extend(find_codings_in_dict(item, key))
            elif isinstance(value, dict):
                # Check if the dict contains a 'coding' list
                if "coding" in value and isinstance(value["coding"], list):
                    coding_string = extract_coding(value["coding"])
                    codings.append((coding_string, key))
                if "code" in value:
                    coding_string = value.get("display", value.get("code"))
                    codings.append((coding_string, key))

                # Recursively search in the dict
                codings.extend(find_codings_in_dict(value, key))

        return codings

    return find_codings_in_dict(resource_dict)


def normalize_value(resource_dict: Dict) -> Tuple[Optional[str], str]:
    """Extract value from FHIR value[x] pattern."""
    value_keys = [k for k in resource_dict.keys() if k.startswith("value")]
    
    if not value_keys:
        return None, ""
    
    # Take the first value key found
    value_key = value_keys[0]
    value = resource_dict[value_key]
    
    # Handle different value types
    if isinstance(value, dict):
        if "value" in value:
            return str(value["value"]), value_key
        elif "display" in value:
            return str(value["display"]), value_key
        elif "code" in value:
            return str(value["code"]), value_key
    elif isinstance(value, (str, int, float, bool)):
        return str(value), value_key
    
    return str(value), value_key


def validate_and_transform_graphql_field_name(field_name: str) -> str:
    """Transform field names to be GraphQL/database compliant."""
    # GraphQL field name regex: starts with _ or letter, followed by _, letter, or number
    graphql_field_regex = r"^[_\w][\w]*$"

    # Replace invalid characters with underscores
    cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '_', field_name)
    
    # Replace multiple underscores with single underscore
    transformed_name = re.sub(r"[^\w]+", "_", cleaned_name)

    # Ensure the name doesn't start with a number
    if transformed_name and re.match(r"^[0-9]", transformed_name):
        transformed_name = "_" + transformed_name

    # Handle empty strings
    if not transformed_name:
        return "_"
        
    return transformed_name


def traverse(resource: dict, prefix: str = "") -> dict:
    """Flatten nested resource structure for audit/debugging."""
    result = {}
    
    for key, value in resource.items():
        new_key = f"{prefix}.{key}" if prefix else key
        
        if isinstance(value, dict):
            result.update(traverse(value, new_key))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    result.update(traverse(item, f"{new_key}[{i}]"))
                else:
                    result[f"{new_key}[{i}]"] = item
        else:
            result[new_key] = value
    
    return result


#######################
# SIMPLIFIED CLASSES  #
#######################


class SimplifiedResource(BaseModel):
    """Simplified FHIR resource for dataframe generation."""
    resource: dict

    @classmethod
    def build(cls, resource: dict) -> "SimplifiedResource":
        """Build a simplified resource from a FHIR resource dict."""
        return cls(resource=resource)

    @computed_field
    @property
    def simplified(self) -> dict:
        """Return simplified flat representation."""
        result = {}
        result.update(self.scalars)
        result.update(self.identifiers)
        result.update(self.codings)
        result.update(self.extensions)
        result.update(self.values)
        return result

    @computed_field
    @property
    def scalars(self) -> dict:
        """Return scalar values from the resource."""
        return {
            k: v
            for k, v in self.resource.items()
            if not isinstance(v, (list, dict))
        }

    @computed_field
    @property
    def identifiers(self) -> dict:
        """Extract identifier information."""
        identifiers = self.resource.get("identifier", [])
        
        if not identifiers:
            return {"identifier": None}
        elif len(identifiers) == 1:
            return {"identifier": identifiers[0].get("value")}
        else:
            # Return multiple identifiers
            result = {}
            for i, identifier in enumerate(identifiers):
                key = "identifier" if i == 0 else f"identifier_{i}"
                result[key] = identifier.get("value")
            return result

    @computed_field
    @property
    def codings(self) -> dict:
        """Extract coding information."""
        codings = {}
        for value, source in normalize_coding(self.resource):
            if isinstance(value, list):
                codings[source] = ", ".join(str(v) for v in value if v)
            else:
                codings[source] = str(value) if value else ""
        
        # Ensure field names are GraphQL compliant
        return {
            validate_and_transform_graphql_field_name(k): v 
            for k, v in codings.items()
        }

    @computed_field
    @property
    def extensions(self) -> dict:
        """Extract extension values."""
        extensions = {}
        
        for ext in self.resource.get("extension", []):
            if "url" in ext:
                # Derive key from extension url
                ext_key = ext["url"].split("/")[-1]
                ext_key = inflection.underscore(ext_key).removesuffix(".json")
                ext_key = validate_and_transform_graphql_field_name(ext_key)
                
                # Extract value
                value, _ = normalize_value(ext)
                if value is not None:
                    extensions[ext_key] = value
        
        return extensions

    @computed_field
    @property
    def values(self) -> dict:
        """Extract value[x] pattern values."""
        value, source = normalize_value(self.resource)
        if not value:
            return {}
        
        # Use code text if available for better field naming
        if self.resource.get("code", {}).get("text"):
            source = validate_and_transform_graphql_field_name(
                self.resource["code"]["text"]
            )
        
        return {source: value}


class SimplifiedGroup(BaseModel):
    """Simplified Group resource handling."""
    resource: dict

    @computed_field
    @property
    def members(self) -> List[dict]:
        """Extract group members."""
        members = []
        for member in self.resource.get("member", []):
            member_data = {
                "entity_reference": member.get("entity", {}).get("reference", ""),
                "inactive": member.get("inactive", False)
            }
            members.append(member_data)
        return members