"""API definition for Task-like objects."""
import abc
import os
from pathlib import Path
from typing import Any, Dict

from dbt_sugar.core.clients.yaml_helpers import open_yaml

COLUMN_NOT_DOCUMENTED = "No description for this column."
MODEL_NOT_DOCUMENTED = "No description for this model."


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
        """Save the columns descriptions from all the DBT repository."""
        for root, _, files in os.walk(self.repository_path):
            for file in files:
                # TODO: Check how to avoid using target to discriminate compiled files in DBT.
                if file == "schema.yml" and "target" not in root:
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
    def run(self) -> None:
        """Orchestrator method that calls all the needed stuff to run a documentation task."""
        ...
