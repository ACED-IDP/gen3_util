import pytest

from gen3_util.common import is_url
from gen3_util.files.downloader import _validate_parameters as downloader_validate_parameters
from gen3_util.files.uploader import _validate_parameters as uploader_validate_parameters


def test_is_upload():
    """Checks cp's 'to' parameter."""
    assert is_url("bucket://foo"), "Should be an upload"
    assert not is_url("xxx"), "Should be a download"


def test_downloader_validate_parameters():
    """Checks downloader from and to."""

    with pytest.raises(AssertionError) as exec_info:
        from_, to_ = downloader_validate_parameters("xxx", "bucket://foo")
    assert 'appears to be a url' in str(exec_info)

    with pytest.raises(AssertionError) as exec_info:
        from_, to_ = downloader_validate_parameters("bucket://foo", "xxx")
    assert 'xxx does not exist' in str(exec_info)

    from_, to_ = downloader_validate_parameters("bucket://foo", "tests/fixtures")
    assert from_ and to_, "Should have validated"


def test_uploader_validate_parameters():
    """Checks uploader from and to."""

    with pytest.raises(AssertionError) as exec_info:
        from_, to_ = uploader_validate_parameters("bucket://foo", "xxx")
    assert 'xxx does not appear to be a url' in str(exec_info)

    with pytest.raises(AssertionError) as exec_info:
        from_, to_ = uploader_validate_parameters("bucket://foo", "bucket://foo")
    assert 'url to url cp not supported' in str(exec_info)

    with pytest.raises(AssertionError) as exec_info:
        from_, to_ = uploader_validate_parameters("xxx", "bucket://foo")
    assert 'xxx does not exist' in str(exec_info)

    from_, to_ = uploader_validate_parameters("tests/fixtures", "bucket://foo")
    assert from_ and to_, "Should have validated"


def test_normalize_file_url():
    """"""
    from gen3_util.files.uploader import _normalize_file_url
    expected = [
        ("s3://foo/bar", "s3://foo/bar"),
        ("file:///foo/bar/", "foo/bar/"),
        ("./foo/bar/", "foo/bar/"),
        ("./foo/bar/", "foo/bar/"),
        ("foo/bar/", "foo/bar/"),
        ("foo/bar/.baz", "foo/bar/.baz"),
        ("foo/bar/./baz", "foo/bar/./baz"),
        ("foo/bar/file:///baz", "foo/bar/file:///baz"),
    ]
    for url, expected_url in expected:
        assert _normalize_file_url(url) == expected_url, "Should normalize {}".format(url)
