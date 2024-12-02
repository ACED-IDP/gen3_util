import os

from gen3_tracker.git import Gen3ClientRemoteWriter


def test_num_parallel():
    """Test the default worker count for file uploads."""
    os.environ["G3T_NUM_PARALLEL"] = "9999"
    assert Gen3ClientRemoteWriter.default_worker_count() == 9999
    del os.environ["G3T_NUM_PARALLEL"]
    assert Gen3ClientRemoteWriter.default_worker_count() != 9999
    assert Gen3ClientRemoteWriter.default_worker_count() > 0
