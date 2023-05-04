from typing import Any

import orjson
import yaml
from pydantic.json import pydantic_encoder

from gen3_util.config import Config


def print_formatted(config: Config, obj: Any) -> None:
    """Print the obj, using configured output format"""

    if config.output.format == "yaml":
        print(yaml.dump(orjson.loads(orjson.dumps(obj, default=pydantic_encoder))))
    elif config.output.format == "json":
        print(orjson.dumps(obj, default=pydantic_encoder, option=orjson.OPT_INDENT_2).decode())
    else:
        print(obj)
