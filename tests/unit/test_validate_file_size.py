import numpy as np
import pytest
from fhir_core.types import Integer64

MAX_SIZE = 9223372036854775807


def test_file_size_max():
    """Test file size."""
    from fhir.resources.attachment import Attachment
    from fhir.resources.documentreference import DocumentReference
    attachment: Attachment = Attachment()
    # https://build.fhir.org/datatypes-definitions.html#Attachment.size
    # is a
    # https://build.fhir.org/datatypes.html#integer64
    # Convert max_size from bytes to terabytes
    max_size_tb = MAX_SIZE / (1024 ** 4)

    if hasattr(attachment, 'model_dump'):
        # fhir.resources >= 8.0.0
        attachment.size = MAX_SIZE
        assert attachment.size == MAX_SIZE
        assert attachment.model_dump()['size'] == MAX_SIZE
        assert Attachment.model_validate(attachment)
    else:
        # fhir.resources >= 7.x
        attachment.size = MAX_SIZE
        assert attachment.size == MAX_SIZE
        assert attachment.dict()['size'] == MAX_SIZE
        assert Attachment.validate(attachment)

    print(f"Max size in terabytes: {max_size_tb}")
    assert max_size_tb == 8388608, "Max size in terabytes should be 8388608"

    document_reference_dict = {"status": "current",
                               "content": [
                                   {
                                       "attachment": {
                                           "size": MAX_SIZE
                                       }
                                   }
                               ]
                               }
    print(document_reference_dict)
    document_reference = DocumentReference(**document_reference_dict)
    assert DocumentReference.model_validate(document_reference)


def test_np_int64():

    max_size = np.int64(MAX_SIZE)
    assert max_size == MAX_SIZE
    with pytest.raises(OverflowError):
        np.int64(MAX_SIZE + 1)


def test_size_meta_info():
    """Test size meta info."""
    from fhir.resources.attachment import Attachment
    attachment: Attachment = Attachment(size=MAX_SIZE)
    assert attachment.model_validate(attachment)


def test_fhir_type_integer64():
    from pydantic import BaseModel
    from pydantic import Field
    from fhir.resources import fhirtypes

    class MyModel(BaseModel):
        size: fhirtypes.Integer64Type | None = Field(  # type: ignore
            None,
            alias="size",
            title="Number of bytes of content (if url provided)",
            description=(
                "The number of bytes of data that make up this attachment (before "
                "base64 encoding, if that is done)."
            ),
            json_schema_extra={
                "element_property": True,
            },
        )

    assert MyModel(size=f"{MAX_SIZE}"), "Should not raise an exception"

    _ = MyModel()
    _.size = 9223372036854775807
    assert _.size == 9223372036854775807


def test_integer64():
    from fhir.resources import fhirtypes

    integer_64: Integer64 = 9223372036854775807
    assert integer_64 == 9223372036854775807

    integer_64: fhirtypes.Integer64Type = 9223372036854775807
    assert integer_64 == 9223372036854775807
