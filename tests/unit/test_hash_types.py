import pytest
from pydantic import ValidationError

from gen3_tracker.common import ACCEPTABLE_HASHES
from gen3_tracker.git import DVCItem

VALID_HASHES = {
    'md5': 'acbd18db4cc2f85cedef654fccc4a4d8',
    'sha1': '2ef7bde608ce5404e97d5f042f95f89f1c232871',
    'sha256': '5bf8aa57fc5a6bc547decf1cc6db63f10deb55a3c6c5df497d631fb3d95e1abf',
    'sha512': '3ba2942ed1d05551d4360a2a7bb6298c2359061dc07b368949bd3fb7feca3344221257672d772ce456075b7cfa50fd7ce41eaefe529d056bf23dd665de668b78',
    'crc': '3e25960a',
    'etag': 'acbd18db4cc2f85cedef654fccc4a4d8-3'
}


def test_invalid_hash_values():
    """Test that invalid hash values raise a ValidationError."""
    for hash_type in ACCEPTABLE_HASHES.keys():
        _ = dict(hash=hash_type, modified='2013-07-01T16:10-04:00', path='dddd', size=1)
        _[hash_type] = 'foo'
        print(_)
        with pytest.raises(ValidationError):
            item = DVCItem(**_)
            print(item)


def test_valid_hash_values():
    """Test that valid hash values do raise a ValidationError."""
    for hash_type in VALID_HASHES.keys():
        _ = dict(hash=hash_type, modified='2013-07-01T16:10-04:00', path='dddd', size=1)
        _[hash_type] = VALID_HASHES[hash_type]
        print(_)
        item = DVCItem(**_)
        print(item)
