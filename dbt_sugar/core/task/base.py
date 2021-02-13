"""API definition for Task-like objects."""
import abc
import os
import re
from pathlib import Path
from typing import Any, Dict

from dbt_sugar.core.clients.yaml_helpers import open_yaml, save_yaml

COLUMN_NOT_DOCUMENTED = "No description for this column."
MODEL_NOT_DOCUMENTED = "No description for this model."
EXCLUDE_TARGET_FILES_PATTERN = r"\/target\/"


class BaseTask(abc.ABC):
    """Sets up basic API for task-like classes."""

    def __init__(self) -> None:
        self.repository_path = Path().absolute()
        self.dbt_definitions: Dict[str, str] = {}
        self.save_all_descriptions()

    def get_column_description_from_dbt_definitions(self, column_name: str) -> str:
        """Searches for the description of a column in all the descriptions in DBT.

        Args:
            column_name (str): column name to get the description from.

        Returns:
            str: with the description of the column.
        """
        return self.dbt_definitions.get(column_name, COLUMN_NOT_DOCUMENTED)

    def get_documented_columns(self, content: Dict[str, Any], model_name: str) -> Dict[str, str]:
        """Method to get the documented columns from a model in a schema.yml.

        Args:
            content (Dict[str, Any]): content of the schema.yml.
            model_name (str): model name to get the columns from.

        Returns:
            Dict[str, str]: with the columns names and descriptions documented.
        """
        documented_columns = {}
        for model in content.get("models", []):
            if model["name"] == model_name:
                for column in model.get("columns", []):
                    if column["description"] != COLUMN_NOT_DOCUMENTED:
                        documented_columns[column["name"]] = column["description"]
        return documented_columns

    def get_not_documented_columns(
        self, content: Dict[str, Any], model_name: str
    ) -> Dict[str, str]:
        """Method to get the undocumented columns from a model in a schema.yml.

        Args:
            content (Dict[str, Any]): content of the schema.yml.
            model_name (str): model name to get the columns from.

        Returns:
            Dict[str, str]: with the columns names and descriptions undocumented.
        """
        not_documented_columns = {}
        for model in content.get("models", []):
            if model["name"] == model_name:
                for column in model.get("columns", []):
                    if column["description"] == COLUMN_NOT_DOCUMENTED:
                        not_documented_columns[column["name"]] = COLUMN_NOT_DOCUMENTED
        return not_documented_columns

    def update_column_test_from_schema(
        self, path_file: Path, model_name: str, tests: Dict[str, Any]
    ):
        """
        Method to update a schema.yml with a Dict of columns names and tests.

        Args:
            path_file (Path): Path to the schema.yml file to update the columns descriptions from.
            model_name (str): with the name of the model.
            tests: Dict with the tests to update.
        """
        content = open_yaml(path_file)
        for model in content["models"]:
            if model["name"] == model_name:
                for column in model.get("columns", []):
                    column_name = column["name"]
                    if column_name in tests.keys():
                        column["tests"] = tests[column_name]
        save_yaml(path_file, content)

    def update_column_description_from_schema(
        self, path_file: Path, dict_column_description_to_update: Dict[str, Dict[str, Any]]
    ) -> None:
        """Method to update a schema.yml with a Dict of columns names and description.

        Args:
            path_file (Path): Path to the schema.yml file to update the columns descriptions from.
            dict_column_description_to_update (Dict[str, Dict[str, Any]]): Dict with the column name with
            the description to update.
        """
        content = open_yaml(path_file)
        for model in content["models"]:
            for column in model.get("columns", []):
                column_name = column["name"]
                if column_name in dict_column_description_to_update.keys():
                    new_desctiption = dict_column_description_to_update[column_name].get(
                        "description"
                    )
                    if new_desctiption:
                        column["description"] = new_desctiption
        save_yaml(path_file, content)

    def update_column_descriptions(
        self, dict_column_description_to_update: Dict[str, Dict[str, Any]]
    ) -> None:
        """Method to update all the schema.ymls from a dbt project with a Dict of columns names and description.

        Args:
            dict_column_description_to_update (Dict[str, Dict[str, Any]]): Dict with the column name with
            the description to update.
        """
        for root, _, files in os.walk(self.repository_path):
            files = [
                f
                for f in files
                if not re.match(EXCLUDE_TARGET_FILES_PATTERN, f) and f == "schema.yml"
            ]
            for file in files:
                path_file = Path(os.path.join(root, file))
                self.update_column_description_from_schema(
                    path_file, dict_column_description_to_update
                )

    def update_description_in_dbt_descriptions(
        self, column_name: str, column_description: str
    ) -> None:
        """Update a column description in the global description dictionary.

        Args:
            column_name (str): column name to update.
            column_description (str): column description to update.
        """
        if not column_description:
            column_description = COLUMN_NOT_DOCUMENTED
        self.dbt_definitions[column_name] = column_description

    def save_descriptions_from_schema(self, content: Dict[str, Any]) -> None:
        """Save the columns descriptions from a schema.yml into the global descriptions dictionary.

        Args:
            content (Dict[str, Any]): content of the schema.yaml.
        """
        if not content:
            return

        for model in content.get("models", []):
            for column in model.get("columns", []):
                column_description = column.get("description", None)
                self.update_description_in_dbt_descriptions(column["name"], column_description)

    def save_all_descriptions(self) -> None:
        """Save the columns descriptions from all the dbt project."""
        for root, _, files in os.walk(self.repository_path):
            files = [
                f
                for f in files
                if not re.match(EXCLUDE_TARGET_FILES_PATTERN, f) and f == "schema.yml"
            ]
            for file in files:
                path_file = Path(os.path.join(root, file))
                content = open_yaml(path_file)
                self.save_descriptions_from_schema(content)

    def is_model_in_schema_content(self, content, model_name) -> bool:
        """Method to check if a model exists in a schema.yaml content.

        Args:
            content (Dict[str, Any]): content of the schema.yaml.
            model_name (str): model name to search.

        Returns:
            boolean: is true if the model is present in the schema.yaml.
        """
        if not content:
            return False

        for model in content.get("models", []):
            if model["name"] == model_name:
                return True
        return False

    @abc.abstractmethod
    def run(self) -> int:
        """Orchestrator method that calls all the needed stuff to run a documentation task."""
        ...
