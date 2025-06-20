from gen3_tracker.git import get_mime_type


def test_added_mime_types():
    """Ensure we can get the mime type for a fastq, svs, etc files."""
    assert get_mime_type("tests/data/test.fastq") == "text/fastq"
    assert get_mime_type("tests/data/test.fastq.gz") == "text/fastq"
    assert get_mime_type("tests/data/test.svs") == "image/x-svs"

