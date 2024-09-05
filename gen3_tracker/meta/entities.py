import inflection

from pydantic import BaseModel, computed_field
from typing import Dict, List, Optional, Tuple

#######################
# FHIR HELPER METHODS #
#######################

def get_nested_value(d: dict, keys: list):
    for key in keys:
        try:
            d = d[key]
        except (KeyError, IndexError, TypeError):
            return None
    return d


def normalize_coding(resource_dict: Dict) -> List[Tuple[str, str]]:
    """normalize any nested coding"""

    def extract_coding(coding_list):
        # return a concatenated string
        # or alternatively return an array
        return [coding.get("display", coding.get("code", "")) for coding in coding_list]

    def find_codings_in_dict(
        d: dict, parent_key: str = ""
    ) -> list[tuple[str, str]]:  # TODO - parent_key not used?
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

                # Recursively search in the dict
                codings.extend(find_codings_in_dict(value, key))
        return codings

    return find_codings_in_dict(resource_dict)


def normalize_value(resource_dict: dict) -> tuple[Optional[str], Optional[str]]:
    """return a tuple containing the normalized value and the name of the field it was derived from"""

    if "valueQuantity" in resource_dict:
        value = resource_dict["valueQuantity"]
        value_normalized = f"{value['value']} {value.get('unit', '')}"
        value_source = "valueQuantity"
    elif "valueCodeableConcept" in resource_dict:
        value = resource_dict["valueCodeableConcept"]
        value_normalized = " ".join(
            [
                coding.get("display", coding.get("code", ""))
                for coding in value.get("coding", [])
            ]
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
    elif "valueUrl" in resource_dict:
        value_normalized = resource_dict["valueUrl"]
        value_source = "valueUrl"
    elif "valueDate" in resource_dict:
        value_normalized = resource_dict["valueDate"]
        value_source = "valueDate"
    elif "valueCount" in resource_dict:
        value_normalized = resource_dict["valueCount"]["value"]
        value_source = "valueCount"
    else:
        value_normalized, value_source = None, None
        # for debugging...
        # raise ValueError(f"value[x] not found in {resource_dict}")

    return value_normalized, value_source


def traverse(resource):
    """simplify a resource's fields, returned as a dict of values,
    where keys are prefixed with "resourceType_" """

    final_subject = {}
    simplified_subject = SimplifiedResource.build(resource=resource).simplified
    prefix = simplified_subject["resourceType"].lower()
    for k, v in simplified_subject.items():
        if k in ["resourceType"]:
            continue
        final_subject[f"{prefix}_{k}"] = v

    return final_subject


########################
# FHIR BUILDER OBJECTS #
########################


class SimplifiedFHIR(BaseModel):
    """All simplifiers should inherit from this class."""

    warnings: list[str] = []
    """A list of warnings generated during the simplification process."""
    resource: dict
    """The FHIR resource to be simplified."""

    @computed_field()
    @property
    def simplified(self) -> dict:
        _ = {"identifier": self.identifier.get("value", None)}
        _.update(self.scalars)
        _.update(self.codings)
        _.update(self.extensions)
        _.update(self.values)
        return _

    def simplify_extensions(
        self, resource: dict = None, _extensions: dict = None
    ) -> dict:
        """Extract extension values, derive key from extension url"""

        def _populate_simplified_extension(extension: dict):
            # simple extension
            value_normalized, extension_key = normalize_value(extension)
            extension_key = extension["url"].split("/")[-1]
            extension_key = (
                inflection.underscore(extension_key)
                .removesuffix(".json")
                .removeprefix("structure_definition_")
            )
            assert (
                value_normalized is not None
            ), f"extension: {extension_key} = {value_normalized} {extension}"
            _extensions[extension_key] = value_normalized

        if not _extensions:
            _extensions = {}

        if not resource:
            resource = self.resource

        for _ in resource.get("extension", [resource]):
            if "extension" not in _.keys():
                if "resourceType" not in _.keys():
                    _populate_simplified_extension(_)
                continue
            elif set(_.keys()) == {"url", "extension"}:
                for child_extension in _["extension"]:
                    self.simplify_extensions(
                        resource=child_extension, _extensions=_extensions
                    )

        return _extensions

    @computed_field
    @property
    def extensions(self) -> dict:
        return self.simplify_extensions()

    @computed_field
    @property
    def scalars(self) -> dict:
        """Return a dictionary of scalar values."""
        return {
            k: v
            for k, v in self.resource.items()
            if (not isinstance(v, list) and not isinstance(v, dict))
        }

    @computed_field
    @property
    def codings(self) -> dict:
        """Return a dictionary of scalar values."""
        _codings = {}
        for k, v in self.resource.items():
            # these are handled in separate methods
            if k in ["identifier", "extension", "component", "code"]:
                continue
            elif isinstance(v, list):
                for elem in v:
                    if isinstance(elem, dict):
                        for value, source in normalize_coding(elem):
                            _codings[k] = value
            elif isinstance(v, dict):
                for value, elem in normalize_coding(v):
                    _codings[k] = value
        return _codings

    @property
    def identifier(self) -> dict:
        """Return the official identifier, or first of a resource."""
        identifiers = self.resource.get("identifier", [])
        official_identifiers = [
            _ for _ in identifiers if _.get("use", "") == "official"
        ]
        if not official_identifiers and identifiers:
            return identifiers[0]
        elif official_identifiers:
            return official_identifiers[0]
        else:
            return {}

    @computed_field
    @property
    def values(self) -> dict:
        """Return a dictionary of source:value."""
        # FIXME: values that are scalars are processed twice: once in scalars once here in values (eg valueString)
        value, source = normalize_value(self.resource)
        if not value:
            return {}

        # update the key if code information is available
        if self.resource.get("code", {}).get("text", None):
            source = self.resource["code"]["text"]
        return {source: value}


class SimplifiedObservation(SimplifiedFHIR):
    @computed_field
    @property
    def codings(self) -> dict:
        """does everything but gets rid of code since that's dealt with in values"""
        _codings = super().codings
        if "code" in _codings:
            del _codings["code"]

        return _codings

    # TODO: remove after data fix
    @computed_field
    @property
    def scalars(self) -> dict:
        """Return a dictionary of scalar values."""
        return {
            k: v
            for k, v in self.resource.items()
            if (
                not isinstance(v, list)
                and not isinstance(v, dict)
                and not k == "valueString"
            )
        }

    @computed_field
    @property
    def values(self) -> dict:
        """Return a dictionary of 'value':value or <component>:value.
        https://build.fhir.org/observation-definitions.html#Observation.component
        """

        # get top-level value in dict if it exists
        _values = super().values

        if len(_values) == 0:
            assert (
                "component" in self.resource
            ), "no component nor top-level value found"

            # get component codes
            if "component" in self.resource:
                for component in self.resource["component"]:
                    value, source = normalize_value(component)
                    if component.get("code", {}).get("text", None):
                        source = component["code"]["text"]
                    if not value:
                        continue
                    _values[source] = value

        # knowing there's now at least 1 item in _values
        if "component" in self.resource:
            # ensure no top-level value is not duplicating a component code value
            # TODO: ensure this value_key corresponds to percent_tumor on some runs due to getting display
            value_key = [k for k in _values][0]
            assert (
                value_key not in self.resource["component"]
            ), """duplicate code value found, only specify the code value in the component, see Rule obs-7
                https://build.fhir.org/observation.html#invs"""

            # get component codes
            if "component" in self.resource:
                for component in self.resource["component"]:
                    value, source = normalize_value(component)
                    if component.get("code", {}).get("text", None):
                        source = component["code"]["text"]
                    if not value:
                        continue
                    _values[source] = value

        assert len(_values) > 0, f"no values found in Observation: {self.resource}"

        return _values


class SimplifiedDocumentReference(SimplifiedFHIR):
    @computed_field
    @property
    def values(self) -> dict:
        """Return a dictionary of 'value':value."""
        _values = super().values
        for content in self.resource.get("content", []):
            if "attachment" in content:
                for k, v in SimplifiedFHIR(
                    resource=content["attachment"]
                ).simplified.items():
                    if k in ["identifier", "extension"]:
                        continue
                    _values[k] = v
        return _values


class SimplifiedResource(object):
    """A simplified FHIR resource, a factory method."""

    @staticmethod
    def build(resource: dict) -> SimplifiedFHIR:
        """Return a simplified FHIR resource."""

        resource_type = resource.get("resourceType", None)
        if resource_type == "Observation":
            return SimplifiedObservation(resource=resource)
        if resource_type == "DocumentReference":
            return SimplifiedDocumentReference(resource=resource)
        return SimplifiedFHIR(resource=resource)
