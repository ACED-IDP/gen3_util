# the fhir import export job is a sower job, which is a kubernetes job
# it calls entrypoints in gen3_util, lets make sure they work
import pydantic

from gen3_util.config import Config
from gen3_util.meta.uploader import cp


def test_cp():
    """Should be callable"""
    assert callable(cp), "cp is not callable"


def test_config():
    """Should be a pydantic model"""
    assert isinstance(Config, pydantic.main.ModelMetaclass), type(Config)
