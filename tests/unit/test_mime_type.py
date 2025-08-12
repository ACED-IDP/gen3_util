from gen3_tracker.git import get_mime_type


def test_fastq_mime_type():
    """Ensure we can get the mime type for a fastq file."""
    assert get_mime_type("tests/data/test.fastq") == "text/fastq"
    assert get_mime_type("tests/data/test.fastq.gz") == "text/fastq"
