import re
import typing

import fhir_core.types
from annotated_types import BaseMetadata, GroupedMetadata, Le, Ge


class Integer64(GroupedMetadata):
    """A signed integer in the range
    -9,223,372,036,854,775,808 to +9,223,372,036,854,775,807 (64-bit).
    This type is defined to allow for record/time counters that can get very large"""

    pattern = re.compile(r"^[0]|[-+]?[1-9][0-9]*$")

    min_length: int = -9223372036854775807
    max_length: int = 9223372036854775807
    __visit_name__ = "integer64"

    def __iter__(self) -> typing.Iterator[BaseMetadata]:
        """ """
        yield Le(self.max_length)

        yield Ge(self.min_length)


def apply_patches():
    """ """
    fhir_core.types.Integer64 = Integer64
    fhir_core.types.Integer64Type = typing.Annotated[int, Integer64()]
    fhir_core.types.FHIR_PRIMITIVES_MAPS[fhir_core.types.Integer64Type] = "integer64"
    fhir_core.types.FHIR_PRIMITIVES_MAPS[fhir_core.types.Integer64] = "integer64"
