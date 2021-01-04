"""Document Task module."""
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from dbt_sugar.core.clients.dbt import DbtProfile
from dbt_sugar.core.clients.yaml_helpers import open_yaml, save_yaml
from dbt_sugar.core.connectors.postgres_connector import PostgresConnector
from dbt_sugar.core.connectors.snowflake_connector import SnowflakeConnector
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.logger import GLOBAL_LOGGER as logger
from dbt_sugar.core.task.base import MODEL_NOT_DOCUMENTED, BaseTask


class DocumentationTask(BaseTask):
    """Documentation Task object.

    Holds methods and attrs necessary to orchestrate a model documentation task.
    """

    def __init__(self, flags: FlagParser) -> None:
        super().__init__()
        self._flags = flags

    def load_dbt_credentials(self) -> Dict[str, str]:
        """Method to load the DBT profile credentials."""
        # TODO: Feed project_name dynamically at run time from CLI or config.
        dbt_profile = DbtProfile(
            project_name="default", target_name="dev", profiles_dir=self._flags.profiles_dir
        )
        dbt_profile.read_profile()
        dbt_credentials = dbt_profile.profile
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

        self.process_model(model, columns_sql)
        return 0

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
                    content = None
                    if path_file.is_file():
                        content = open_yaml(path_file)

                    if self.is_model_in_schema_content(content, model_name) and content:
                        content = self.update_model(content, model_name, columns_sql)
                    else:
                        content = self.create_new_model(content, model_name, columns_sql)
                    save_yaml(path_file, content)
