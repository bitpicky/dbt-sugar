"""Document Task module."""
import copy
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from rich.console import Console
from rich.progress import BarColumn, Progress

from dbt_sugar.core.clients.dbt import DbtProfile
from dbt_sugar.core.clients.yaml_helpers import open_yaml, save_yaml
from dbt_sugar.core.config.config import DbtSugarConfig
from dbt_sugar.core.connectors.postgres_connector import PostgresConnector
from dbt_sugar.core.connectors.snowflake_connector import SnowflakeConnector
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.logger import GLOBAL_LOGGER as logger
from dbt_sugar.core.task.base import MODEL_NOT_DOCUMENTED, BaseTask
from dbt_sugar.core.ui.cli_ui import UserInputCollector

console = Console()
NUMBER_COLUMNS_TO_PRINT_PER_ITERACTION = 5

DB_CONNECTORS = {
    "postgres": PostgresConnector,
    "snowflake": SnowflakeConnector,
}


class DocumentationTask(BaseTask):
    """Documentation Task object.

    Holds methods and attrs necessary to orchestrate a model documentation task.
    """

    def __init__(
        self, flags: FlagParser, dbt_profile: DbtProfile, config: DbtSugarConfig, dbt_path: Path
    ) -> None:
        super().__init__(flags=flags, dbt_path=dbt_path, sugar_config=config)
        self.column_update_payload: Dict[str, Dict[str, Any]] = {}
        self._flags = flags
        self._dbt_profile = dbt_profile
        # self._sugar_config = config

    def run(self) -> int:
        """Main script to run the command doc"""
        columns_sql = []

        model = self._flags.model
        schema = self._dbt_profile.profile.get("target_schema", "")

        dbt_credentials = self._dbt_profile.profile
        connector = DB_CONNECTORS.get(dbt_credentials.get("type", ""))
        if not connector:
            raise NotImplementedError(
                f"Connector '{dbt_credentials.get('type')}' is not implemented."
            )

        self.connector = connector(dbt_credentials)

        # exit early if model is in the excluded_models list
        _ = self.is_exluded_model(model)
        columns_sql = self.connector.get_columns_from_table(model, schema)
        if columns_sql:
            return self.orchestrate_model_documentation(schema, model, columns_sql)
        return 1

    def update_model_description(
        self, content: Dict[str, Any], model_name: str, is_already_documented: bool = False
    ) -> Dict[str, Any]:
        """Updates the model description from a schema.yaml.

        Args:
            content (Dict[str, Any]): Schema.yml Content.
            model_name (str): Name of the model for which the description will be changed.

        Returns:
            Dict[str, Any]: Schema.yml content updated.
        """
        message = f"Do you want to write a description for {model_name}"
        if is_already_documented:
            message = f"Do you want to change the model description of {model_name}"
        model_doc_payload: List[Mapping[str, Any]] = [
            {
                "type": "confirm",
                "name": "wants_to_document_model",
                "message": message,
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

    def move_name_and_description_to_first_position(self, model: Dict[str, Any]):
        """
        Move keys name and description to the beginning of the dictionary.

        Args:
            model (Dict[str, Any]): Name of the model to document.

        Returns:
            Dict[str, Any]: with the keys name and description in the begining of the dictionary.
        """
        # DEPRECATION: Drop ordered dict when dropping python 3.6 support
        ordered_dict = OrderedDict(model)
        ordered_dict.move_to_end("description", last=False)
        ordered_dict.move_to_end("name", last=False)
        return ordered_dict

    def order_schema_yml(self, content_yml: Dict[str, Any]):
        """
        Order the content from a schema.yml. Will order:

            - The schema.yml content in alphabetical order.
            - The models in alphabetical order.
            - The columns from each model in alphabetical order.

        Args:
            content_yml (Dict[str, Any]): schema yml content to order.
        Returns:
            content_yml (Dict[str, Any]): schema yml content with the content ordered.
        """
        for i, model in enumerate(content_yml.get("models", {})):
            # Ensure model name and description outer keys are in first position
            content_yml["models"][i] = self.move_name_and_description_to_first_position(
                content_yml["models"][i]
            )

            # Sort columns names in alphabetical order inside the model.
            if model.get("columns", None):
                content_yml["models"][i]["columns"] = sorted(
                    model["columns"],
                    key=lambda k: k["name"].lower(),
                )
                # ensure name and description are in first position for each column entry.
                content_yml["models"][i]["columns"] = [
                    self.move_name_and_description_to_first_position(column_dict)
                    for column_dict in content_yml["models"][i]["columns"]
                ]
        # Sorting models names in alphabetical order.
        content_yml["models"] = sorted(content_yml["models"], key=lambda k: k["name"].lower())
        return content_yml

    def orchestrate_model_documentation(
        self, schema: str, model_name: str, columns_sql: List[str]
    ) -> int:
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
        schema_file_path, schema_exists, is_already_documented = self.find_model_schema_file(
            model_name
        )

        if not schema_file_path:
            raise FileNotFoundError(
                f"Model: '{model_name}' could not be found in your dbt project."
            )
        if schema_exists:
            content = open_yaml(schema_file_path)

        content = self.create_or_update_model_entry(
            is_already_documented, content, model_name, columns_sql
        )
        try:
            content = self.update_model_description(content, model_name, is_already_documented)

            not_documented_columns = self.get_not_documented_columns(content, model_name)
            self.document_columns(not_documented_columns, "undocumented_columns")

            documented_columns = self.get_documented_columns(content, model_name)
            self.document_columns(documented_columns, "documented_columns")
        except KeyboardInterrupt:
            logger.info("The user has exited the doc task, all changes have been discarded.")
            return 0
        save_yaml(schema_file_path, self.order_schema_yml(content))
        self.check_tests(schema, model_name)
        self.update_model_description_test_tags(
            schema_file_path, model_name, self.column_update_payload
        )
        # Method to update the descriptions in all the schemas.yml
        self.update_column_descriptions(self.column_update_payload)

        return 0

    def check_tests(self, schema: str, model_name: str) -> None:
        """
        Method to run and add test into a schema.yml, this method will:

        Run the tests and if they have been successful it will add them into the schema.yml.

        Args:
            schema (str): Name of the schema where the model lives.
            model_name (str): Name of the model to document.
        """
        with Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            transient=True,
        ) as progress:
            test_checking_task = progress.add_task(
                "[bold] checking your tests...", total=len(self.column_update_payload.keys())
            )
            for column in self.column_update_payload.keys():
                tests = self.column_update_payload[column].get("tests", [])
                tests_ = copy.deepcopy(tests)
                for test in tests_:
                    has_passed = self.connector.run_test(
                        test,
                        schema,
                        model_name,
                        column,
                    )
                    message = self._generate_test_success_message(test, column, has_passed)
                    progress.console.log(message)
                    if not has_passed:
                        tests.remove(test)
                progress.advance(test_checking_task)

    @staticmethod
    def _generate_test_success_message(test_name: str, column_name: str, has_passed: bool):
        if has_passed:
            return f"The [bold]{test_name}[/bold] test on '{column_name}' [green]PASSED"
        return (
            f"The [bold]{test_name}[/bold] test on '{column_name}' [red]FAILED[/red]. \n\t└[bold]It will not be added "
            "to your schema.yml.[/bold]"
        )

    def document_columns(
        self, columns: Dict[str, str], question_type: str = "undocumented_columns"
    ) -> None:
        """
        Method to document the columns from a model.

        Will ask the user which columns they want to document and collect the new definition.

        Args:
            columns (Dict[str, str]): Dict of columns with the column name as the key
            and the column description to populate schema.yml as the value.
        """
        allowed_question_types_map = {
            "undocumented_columns": "undocumented columns",
            "documented_columns": "documented columns",
        }
        assert (
            question_type in allowed_question_types_map.keys()
        ), f"question_type must be one of those: {list(allowed_question_types_map.keys())}"

        # set up pagination messaging
        columns_names = list(columns.keys())
        number_of_colums_to_document = len(columns_names)
        is_paginated = number_of_colums_to_document > NUMBER_COLUMNS_TO_PRINT_PER_ITERACTION
        if is_paginated:
            logger.info(
                f"There are {number_of_colums_to_document} columns to document in total we will show them to you "
                f"{NUMBER_COLUMNS_TO_PRINT_PER_ITERACTION} at a time."
            )

        # go through columns to document
        for i in range(0, number_of_colums_to_document, NUMBER_COLUMNS_TO_PRINT_PER_ITERACTION):
            final_index = i + NUMBER_COLUMNS_TO_PRINT_PER_ITERACTION
            is_first_page = True if final_index <= NUMBER_COLUMNS_TO_PRINT_PER_ITERACTION else False

            choices_undocumented = columns_names[i:final_index]
            choices_documented = {}
            # Feed the current description into the choices messahe.
            if question_type == "documented_columns":
                choices_documented = {key: columns[key] for key in choices_undocumented}
            choices = choices_documented if choices_documented else choices_undocumented

            payload: List[Mapping[str, Any]] = [
                {
                    "type": "checkbox",
                    "name": "cols_to_document",
                    "choices": choices,
                    "message": "Select the columns you want to document.",
                }
            ]
            user_input = UserInputCollector(
                question_type,
                payload,
                ask_for_tests=self._sugar_config.config["always_enforce_tests"],
                ask_for_tags=self._sugar_config.config["always_add_tags"],
                is_paginated=is_paginated,
                is_first_page=is_first_page,
            ).collect()
            self.column_update_payload.update(user_input)

    def update_model(
        self, content: Dict[str, Any], model_name: str, columns_on_db: List[str]
    ) -> Dict[str, Any]:
        """Method to update the columns from a model in a schema.yaml content.

        Args:
            content (Dict[str, Any]): content of the schema.yaml.
            model_name (str): model name to update.
            columns_on_db (List[str]): List of columns that the model has in the database.

        Returns:
            Dict[str, Any]: with the content of the schema.yml with the model updated.
        """
        logger.info(f"The model '{model_name}' already exists, updating its documentation.")
        for model in content.get("models", []):
            if model["name"] == model_name:
                for column in columns_on_db:
                    # Check whether the model has columns already documented in schema yaml
                    if not model.get("columns", None):
                        model["columns"] = []

                    columns = model.get("columns", [])
                    columns_names = [column["name"] for column in columns]
                    if column not in columns_names:
                        description = self.get_column_description_from_dbt_definitions(column)
                        logger.info(f"Updating column '{column.lower()}'")
                        columns.append({"name": column, "description": description})
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
        logger.info(f"The model '{model_name}' has not been documented yet. Creating a new entry.")
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

    def create_or_update_model_entry(
        self,
        is_already_documented: bool,
        content: Optional[Dict[str, Any]],
        model_name: str,
        columns_sql: List[str],
    ) -> Dict[str, Any]:
        """Method to update/create a model entry in the schema.yml.

        Args:
            model_name (str): Name of the model for which to create or update entry in schema.yml.
            columns_sql (List[str]): List of columns names found in the database for this model.
        """
        if is_already_documented and content:
            content = self.update_model(content, model_name, columns_sql)
        else:
            content = self.create_new_model(content, model_name, columns_sql)
        return content
