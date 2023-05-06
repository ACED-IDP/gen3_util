from typing import Mapping

import orjson
import yaml
from pydantic.json import pydantic_encoder

from gen3_util.config import Config


def print_formatted(config: Config, output: Mapping) -> None:
    """Print the obj, using configured output format"""

    if config.output.format == "yaml":
        print(yaml.dump(output))
    elif config.output.format == "json":
        print(orjson.dumps(output, default=pydantic_encoder, option=orjson.OPT_INDENT_2).decode())
    else:
        print(output)
