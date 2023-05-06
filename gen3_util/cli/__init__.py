from typing import Mapping

from gen3_util.config import Config
from gen3_util.util import print_formatted


class CLIOutput:
    """Ensure output, exceptions and exit code are returned to user consistently."""
    def __init__(self, config: Config, output: Mapping):
        self.output = output
        self.config = config

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        rc = 0
        _ = {}
        _.update(self.output)
        if exc_type is not None:
            _['exception'] = str(exc_val)
            rc = 1
        if 'msg' not in _:
            if rc == 1:
                _['msg'] = 'FAIL'
            else:
                _['msg'] = 'OK'
        print_formatted(self.config, _)
        exit(rc)
