"""Contains yaml related utils which might get used in places."""


from pathlib import Path
from typing import Any, Dict

import yaml

from dbt_sugar.core.exceptions import YAMLFileEmptyError


def open_yaml(path: Path) -> Dict[str, Any]:
    """Opens a yaml file... Nothing too exciting there.

    Args:
        path (Path): Full filename path pointing to the yaml file we want to open.

    Returns:
        Dict[str, Any]: A python dict containing the content from the yaml file.
    """
    if path.is_file():
        with open(path, "r") as stream:
            yaml_dict = yaml.safe_load(stream)
            if yaml_dict:
                return yaml_dict
            raise YAMLFileEmptyError(f"The following file {path.resolve()} seems empty.")
    raise FileNotFoundError(f"File {path.resolve()} was not found.")
