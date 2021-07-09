"""Contains yaml related utils which might get used in places."""

from pathlib import Path
from typing import Any, Dict

import ruamel.yaml
import yaml
import yamlloader

from dbt_sugar.core.exceptions import YAMLFileEmptyError
from dbt_sugar.core.logger import GLOBAL_LOGGER as logger

# TODO: I probably need to also make sure that we load with the appropriate method
# let's always use ruamel.yaml but in one case we will load with `typ='safe'` and
# in the other we will load with `typ='rt'` the same will be applied to when we save.


def open_yaml(path: Path, preserve_yaml_order: bool = False) -> Dict[str, Any]:
    """Opens a yaml file... Nothing too exciting there.

    Args:
        path (Path): Full filename path pointing to the yaml file we want to open.

    Returns:
        Dict[str, Any]: A python dict containing the content from the yaml file.
    """
    if path.is_file():
        logger.debug(f"Opening: {path}")
        with open(path, "r") as stream:
            if preserve_yaml_order:
                ryaml = ruamel.yaml.YAML(typ="rt")
                yaml_dict = ryaml.load(stream)
            else:
                yaml_dict = yaml.load(stream, Loader=yamlloader.ordereddict.CSafeLoader)
            if yaml_dict:
                return yaml_dict
            raise YAMLFileEmptyError(f"The following file {path.resolve()} seems empty.")
    raise FileNotFoundError(f"File {path.resolve()} was not found.")


def save_yaml(path: Path, data: Dict[str, Any], preserve_yaml_order: bool = False) -> None:
    """Saves a YAML content.

    Args:
        path (Path): Full filename path pointing to the yaml file we want to save.
        data (dict[str, Any]): Data to save in the file.
    """
    # TODO: This is a bit of a hot fix. Ideally we should avoid processing those
    # non-model conatining files earlier in the flow.
    if data.get("models"):
        with open(path, "w") as outfile:
            if preserve_yaml_order:
                ryaml = ruamel.yaml.YAML(typ="rt")
                ryaml.width = 100
                ryaml.dump(data, outfile)
            else:
                yaml.dump(data, outfile, width=100, Dumper=yamlloader.ordereddict.CDumper)
