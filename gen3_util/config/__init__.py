from pydantic.dataclasses import dataclass


@dataclass
class LogConfig:
    format: str
    """https://docs.python.org/3/library/logging.html#logging.Formatter"""
    level: str
    """https://docs.python.org/3/library/logging.html#logging-levels"""


@dataclass
class OutputConfig:
    format: str = "text"
    """write to stdout with this format"""


@dataclass
class Config:
    log: LogConfig = LogConfig('%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s', 'INFO')
    """logging setup"""
    output: OutputConfig = OutputConfig('text')
    """output setup"""
