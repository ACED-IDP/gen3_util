import pathlib
from typing import Annotated

import pydantic
from fhir.resources.attachment import Attachment
from pydantic import UrlConstraints, AnyUrl


def test_validate_any_url():

    class MyModel(pydantic.BaseModel):
        url: Annotated[AnyUrl, UrlConstraints(host_required=False)]

    _ = MyModel(url='file:///foo/bar')
    assert _, "file:///foo/bar is a valid file url"
    assert _.url.host is None, "file:///foo/bar has no host"

    _ = MyModel(url='xxx:///XXXX')
    assert _, "file:///foo/bar is a valid file url"
    assert _.url.host is None, "file:///foo/bar has no host"


def test_fhir_url():
    """Previously a monkey patch was used to enable file urls.  Any xs:anyURI is now allowed. See https://w3.org/TR/xmlschema-2/#anyURI
    From https://hl7.org/fhir/datatypes.html#url (This regex is very permissive, but URIs must be valid. Implementers are welcome to use more specific regex statements for a URI in specific contexts)"""
    attachment: Attachment = Attachment(url='file:///foo/bar')
    assert attachment.validate_after_model_construction()

    attachment: Attachment = Attachment.model_validate({'url': 'file:///foo/bar'})
    assert attachment

    attachment: Attachment = Attachment.model_validate({'url': 'xxx:///XXXX'})
    assert attachment

    attachment: Attachment = Attachment.model_validate({'url': 'FOO BAR'})
    assert attachment


def test_path_encoders():
    """Previously a monkey patch was used to enable correct serialization of path objects"""
    # eg
    # # default initializers for path
    # pydantic.v1.json.ENCODERS_BY_TYPE[pathlib.PosixPath] = str
    # pydantic.v1.json.ENCODERS_BY_TYPE[pathlib.WindowsPath] = str
    # pydantic.v1.json.ENCODERS_BY_TYPE[pathlib.Path] = str

    class MyModel(pydantic.BaseModel):
        path: pathlib.Path

    _ = MyModel(path=pathlib.Path('/foo/bar'))
    assert _, "/foo/bar is a valid path"
    _.model_dump()['path'] == '/foo/bar'
