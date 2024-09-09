"""Run flake8 tests"""

import subprocess
import os


def test_coding_conventions():
    """Check python conventions on key directories"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    directories = [os.path.join(script_dir, "../../gen3_tracker"), os.path.join(script_dir, "../../tests")]
    failures = []
    for directory in directories:
        cmd_str = f"flake8 {directory} --max-line-length 256 --exclude test_flatten_fhir_example.py"
        completed = subprocess.run(cmd_str, shell=True)
        if completed.returncode != 0:
            _ = f"FAILURE: Python formatting and style for directory {directory}/"
            failures.append(_)
            print(_)

    assert len(failures) == 0, failures
