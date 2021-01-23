"""Document Task module."""
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from dbt_sugar.core.clients.dbt import DbtProfile
from dbt_sugar.core.clients.yaml_helpers import open_yaml, save_yaml
from dbt_sugar.core.connectors.postgres_connector import PostgresConnector
from dbt_sugar.core.connectors.snowflake_connector import SnowflakeConnector
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.logger import GLOBAL_LOGGER as logger
from dbt_sugar.core.task.base import EXCLUDE_TARGET_FILES_PATTERN, MODEL_NOT_DOCUMENTED, BaseTask
from dbt_sugar.core.ui.cli_ui import UserInputCollector

NUMBER_COLUMNS_TO_PRINT_PER_ITERACTION = 5


class DocumentationTask(BaseTask):
    """Documentation Task object.

    Holds methods and attrs necessary to orchestrate a model documentation task.
    """

    def __init__(self, flags: FlagParser, dbt_profile: DbtProfile) -> None:
        super().__init__()
        self.columns_to_update: Dict[str, str] = {}
        self._flags = flags
        self._dbt_profile = dbt_profile

    def load_dbt_credentials(self) -> Dict[str, str]:
        """Method to load the DBT profile credentials."""
        self._dbt_profile.read_profile()
        dbt_credentials = self._dbt_profile.profile
        if not dbt_credentials:
            logger.info("Not able to locate DBT profile.")
            exit(1)
        return dbt_credentials

    def run(self) -> int:
        """Main script to run the command doc"""
        columns_sql = []

        model = self._flags.model
        schema = self._flags.schema

        dbt_credentials = self.load_dbt_credentials()
        type_of_connection = dbt_credentials.get("type", "")

        if type_of_connection == "postgres":
            columns_sql = PostgresConnector(
                user=dbt_credentials.get("user", ""),
                password=dbt_credentials.get("password", ""),
                host=dbt_credentials.get("host", "localhost"),
                database=dbt_credentials.get("database", "dwh"),
            ).get_columns_from_table(model, schema)
        elif type_of_connection == "snowflake":
            columns_sql = SnowflakeConnector(
                user=dbt_credentials.get("user", ""),
                password=dbt_credentials.get("password", ""),
                account=dbt_credentials.get("account", ""),
                database=dbt_credentials.get("database", "dwh"),
            ).get_columns_from_table(model, schema)
        if columns_sql:
            return self.orchestrate_model_documentation(model, columns_sql)
        return 1

    def change_model_description(self, content: Dict[str, Any], model_name: str) -> Dict[str, Any]:
        """Updates the model description from a schema.yaml.

        Args:
            content (Dict[str, Any]): Schema.yml Content.
            model_name (str): Name of the model for which the description will be changed.

        Returns:
            Dict[str, Any]: Schema.yml content updated.
        """
        model_doc_payload: List[Mapping[str, Any]] = [
            {
                "type": "confirm",
                "name": "wants_to_document_model",
                "message": f"Do you want to change the {model_name} model description?",
                "default": True,
            },
            {
                "type": "text",
                "name": "model_description",
                "message": "Please write down your description:",
            },
        ]
        user_input = UserInputCollector("model", model_doc_payload).collect()
        if user_input.get("model_description", None):
            for model in content.get("models", []):
                if model["name"] == model_name:
                    model["description"] = user_input["model_description"]
        return content

    def find_model_in_dbt(self, model_name: str) -> Tuple[Optional[Path], bool]:
        """
        Method to find a model name in the dbt project.

            - If we find the sql of the model but there is no schema we return the Path
            and False (to create the schema).
            - If we find the sql of the model and there is schema we return the Path and True.

        Args:
            model_name (str): model name to find in the dbt project.

        Returns:
            Tuple[Optional[Path], bool]: Optional path of the sql model if found
            and boolean indicating whether the schema.yml exists.
        """
        for root, _, files in os.walk(self.repository_path):
            files = [f for f in files if not re.match(EXCLUDE_TARGET_FILES_PATTERN, f)]
            for file in files:
                if file == f"{model_name}.sql":
                    path_file = Path(os.path.join(root, "schema.yml"))
                    if path_file.is_file():
                        return path_file, True
                    else:
                        return path_file, False
        return None, False

    def orchestrate_model_documentation(self, model_name: str, columns_sql: List[str]) -> int:
        """
        Orchestrator to fully document a model will:

            - Create or update the columns found the database table into the schema.yml.
            - Gives the user the posibility to change the model description.
            - Gives the user the posibility to document the undocumented columns.
            - Updates all the new columns definitions in all dbt.

        Args:
            model_name (str): Name of the model to document.
            columns_sql (List[str]): Columns names that the model have in the database.

        Returns:
            int: with the status of the execution. 1 for fail, and 0 for ok!
        """
        content = None
        path, exists = self.find_model_in_dbt(model_name)
        if not path:
            logger.info("Not able to find the model in the repository.")
            return 1
        if exists:
            content = open_yaml(path)
        content = self.process_model(content, model_name, columns_sql)
        content = self.change_model_description(content, model_name)
        save_yaml(path, content)

        not_documented_columns = self.get_not_documented_columns(content, model_name)
        self.document_columns(not_documented_columns)

        self.update_column_descriptions(self.columns_to_update)
        return 0

    def document_columns(self, columns: Dict[str, str]) -> None:
        """
        Method to document the columns from a model.

        Will ask the user which columns they want to document and collect the new definition.

        Args:
            columns (Dict[str, str]): Dict of columns with the column name as the key
            and the column description to populate schema.yml as the value.
        """
        columns_names = list(columns.keys())
        for i in range(0, len(columns_names), NUMBER_COLUMNS_TO_PRINT_PER_ITERACTION):
            final_index = i + NUMBER_COLUMNS_TO_PRINT_PER_ITERACTION
            undocumented_columns_payload: List[Mapping[str, Any]] = [
                {
                    "type": "checkbox",
                    "name": "cols_to_document",
                    "choices": columns_names[i:final_index],
                    "message": "Select the columns you want to document.",
                }
            ]
            user_input = UserInputCollector(
                "undocumented_columns", undocumented_columns_payload
            ).collect()
            # user_input_dict = cast(Dict[str, str], user_finput)
            self.columns_to_update.update(user_input)

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
        self, content: Optional[Dict[str, Any]], model_name: str, columns_sql: List[str]
    ) -> Dict[str, Any]:
        """Method to create a new model in a schema.yaml content.

        Args:
            content (Dict[str, Any]): content of the schema.yaml.
            model_name (str): Name of the model for which to create entry in schema.yml.
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

    def process_model(
        self, content: Optional[Dict[str, Any]], model_name: str, columns_sql: List[str]
    ) -> Dict[str, Any]:
        """Method to update/create a model entry in the schema.yml.

        Args:
            model_name (str): Name of the model for which to create or update entry in schema.yml.
            columns_sql (List[str]): List of columns names found in the database for this model.
        """
        if self.is_model_in_schema_content(content, model_name) and content:
            content = self.update_model(content, model_name, columns_sql)
        else:
            content = self.create_new_model(content, model_name, columns_sql)
        return content
