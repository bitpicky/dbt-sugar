"""API definition for Task-like objects."""
import abc
import os
from pathlib import Path
from typing import Any, Dict, List

from dbt_sugar.core.clients.yaml_helpers import open_yaml, save_yaml
from dbt_sugar.core.logger import GLOBAL_LOGGER as logger

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

    def update_model(
        self, content: Dict[str, Any], model_name: str, columns_sql: List[str]
    ) -> Dict[str, Any]:
        """Method to update the columns from a model in a schema.yaml content.

        Args:
            content (Dict[str, Any]): content of the schema.yaml.
            model_name (str): model name to update.
            columns_sql (List[str]): List of columns that the model have in the database.

        Returns:
            Dict[str, Any]: with the content of the schema.yml with the model updated.
        """
        logger.info("The model already exists, update the model.")
        for model in content.get("models", []):
            if model["name"] == model_name:
                for column in columns_sql:
                    columns_names = [column["name"] for column in model["columns"]]
                    if column not in columns_names:
                        description = self.get_column_description_from_dbt_definitions(column)
                        logger.info(f"Updating column with name {column}")
                        model["columns"].append({"name": column, "description": description})
        return content

    def create_new_model(
        self, content: Dict[str, Any], model_name: str, columns_sql: List[str]
    ) -> Dict[str, Any]:
        """Method to create a new model in a schema.yaml content.

        Args:
            content (Dict[str, Any]): content of the schema.yaml.
            model_name (str): model name to create.
            columns_sql (List[str]): List of columns that the model have in the database.

        Returns:
            Dict[str, Any]: with the content of the schema.yml with the model created.
        """
        logger.info("The model doesn't exists, creating a new model.")
        columns = []
        for column_sql in columns_sql:
            description = self.get_column_description_from_dbt_definitions(column_sql)
            columns.append({"name": column_sql, "description": description})
        model = {
            "name": model_name,
            "description": MODEL_NOT_DOCUMENTED,
            "columns": columns,
        }
        if not content:
            content = {"version": 2, "models": [model]}
        else:
            content["models"].append(model)
        return content

    def process_model(self, model_name: str, columns_sql: List[str]) -> None:
        """Method to update/create a model in the schema.yaml.

        Args:
            model_name (str): model name to create.
            columns_sql (List[str]): List of columns that the model have in the database.
        """
        for root, _, files in os.walk(self.repository_path):
            for file in files:
                # TODO: Check how to avoid using target to discriminate compiled files in DBT.
                if file == f"{model_name}.sql" and "target" not in root:
                    path_file = Path(os.path.join(root, "schema.yml"))
                    content = open_yaml(path_file)
                    if self.is_model_in_schema_content(content, model_name):
                        content = self.update_model(content, model_name, columns_sql)
                    else:
                        content = self.create_new_model(content, model_name, columns_sql)
                    save_yaml(path_file, content)

    @abc.abstractmethod
    def run(self) -> int:
        """Orchestrator method that calls all the needed stuff to run a documentation task."""
        ...
