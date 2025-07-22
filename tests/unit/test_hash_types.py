import pytest
from pydantic import ValidationError

from gen3_tracker.git import DVCItem
from gen3_tracker.gen3.indexd import create_hashes_metadata
from gen3_tracker.common import ACCEPTABLE_HASHES


VALID_HASHES = {
    "md5": "acbd18db4cc2f85cedef654fccc4a4d8",
    "sha1": "2ef7bde608ce5404e97d5f042f95f89f1c232871",
    "sha256": "5bf8aa57fc5a6bc547decf1cc6db63f10deb55a3c6c5df497d631fb3d95e1abf",
    "sha512": "3ba2942ed1d05551d4360a2a7bb6298c2359061dc07b368949bd3fb7feca3344221257672d772ce456075b7cfa50fd7ce41eaefe529d056bf23dd665de668b78",
    "crc": "3e25960a",
    "etag": "acbd18db4cc2f85cedef654fccc4a4d8-3",
}


def test_invalid_hash_values():
    """Test that invalid hash values raise a ValidationError."""
    for hash_type in ACCEPTABLE_HASHES.keys():
        _ = dict(hash=hash_type, modified="2013-07-01T16:10-04:00", path="dddd", size=1)
        _[hash_type] = "foo"
        print(_)
        with pytest.raises(ValidationError):
            item = DVCItem(**_)
            print(item)


def test_valid_hash_values():
    """Test that valid hash values do raise a ValidationError."""
    for hash_type in VALID_HASHES.keys():
        _ = dict(hash=hash_type, modified="2013-07-01T16:10-04:00", path="dddd", size=1)
        _[hash_type] = VALID_HASHES[hash_type]
        print(_)
        item = DVCItem(**_)
        print(item)


class DummyOut:
    """
    Dummy class to simulate an object with hash attributes.
    """

    def __init__(self, **kwargs) -> None:
        """
        Initialize DummyOut with arbitrary attributes.
        """
        for k, v in kwargs.items():
            setattr(self, k, v)


class DummyDVC:
    """
    Dummy class to simulate a DVC object with object_id, out, and meta.
    """

    def __init__(self, object_id: str, out: DummyOut, meta: "DummyMeta") -> None:
        """
        Initialize DummyDVC with object_id, out, and meta.
        """
        self.object_id = object_id
        self.out = out
        self.meta = meta


class DummyMeta:
    """
    Dummy class to simulate metadata for DVC objects.
    """

    specimen: str = "spec1"
    patient: str = "pat1"
    task: str = "task1"
    observation: str = "obs1"
    no_bucket: bool = False


def test_create_hashes_metadata_fills_hashes() -> None:
    """
    Test that create_hashes_metadata fills the hashes dict with all present and non-empty attributes
    from ACCEPTABLE_HASHES on dvc.out.
    """
    out = DummyOut(md5="abc", sha256="def", sha1=None)
    dvc = DummyDVC(object_id="guid1", out=out, meta=DummyMeta())
    hashes, metadata = create_hashes_metadata(dvc, "prog", "proj")
    for h in ACCEPTABLE_HASHES:
        if hasattr(out, h) and getattr(out, h):
            assert h in hashes
            assert hashes[h] == getattr(out, h)
        else:
            assert h not in hashes
