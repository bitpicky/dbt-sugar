"""Document Task module."""
from typing import Any, Dict, List

from rich.console import Console
from rich.table import Table

from dbt_sugar.core.clients.yaml_helpers import open_yaml
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.logger import GLOBAL_LOGGER as logger
from dbt_sugar.core.task.base import COLUMN_NOT_DOCUMENTED, BaseTask

console = Console()
NUMBER_COLUMNS_TO_PRINT_PER_ITERACTION = 5


class AuditTask(BaseTask):
    """Audit Task object.

    Holds methods and attrs necessary to audit a model or a DBT project.
    """

    def __init__(self, flags: FlagParser) -> None:
        super().__init__()
        self.column_update_payload: Dict[str, Dict[str, Any]] = {}
        self._flags = flags
        self.model_name = self._flags.model

    def run(self) -> int:
        """Main script to run the command doc"""
        if self.model_name:
            path_file, schema_exists = self.find_model_in_dbt(self.model_name)
            if not path_file:
                logger.info("Could not find the Model in the DBT project")
                return 1
            if not schema_exists:
                logger.info("The model is not documented.")
                return 1

            self.model_content = open_yaml(path_file)
            self.get_columns_descriptions_statistics_for_model()
            self.get_columns_tests_statistics_for_model()
        else:
            self.get_all_columns_descriptions_statistics_per_model()
            self.get_all_columns_tests_statistics_per_model()
        return 0

    def get_columns_tests_statistics_for_model(self) -> None:
        """
        Method to get the tests statistics from a model.

        For the process a column is not tested if it does not have any tests.
        """
        columns = self.dbt_tests.get(self.model_name)

        if not columns:
            logger.info(f"Not able to get the test statistics for the model {self.model_name}")
            return

        # Init variables
        model_number_columns = 0
        model_columns_without_tests = 0
        untested_columns = []

        for column in columns:
            tests = column["tests"]
            model_number_columns += 1
            if len(tests) == 0:
                model_columns_without_tests += 1
                untested_columns.append(column["name"])

        percentage_not_tested_columns = round(
            100 - ((model_columns_without_tests / model_number_columns) * 100), 2
        )

        # To print nice the data with the total coverage at the last column.
        data = {
            (column): (
                str(percentage_not_tested_columns) if i == (len(untested_columns) - 1) else ""
            )
            for i, column in enumerate(untested_columns)
        }

        self.create_table(
            title="Test Coverage", columns=["undocument columns", "coverage"], data=data
        )

    def get_columns_descriptions_statistics_for_model(self) -> None:
        """
        Method to get the descriptions statistics from a model.

        For the process a column is not documented if the description is:
        Not No description for this column.
        """
        documented_columns = self.get_documented_columns(
            content=self.model_content,
            model_name=self.model_name,
        )
        not_documented_columns = self.get_not_documented_columns(
            content=self.model_content,
            model_name=self.model_name,
        )

        number_documented_columns = len(documented_columns.keys())
        number_not_documented_columns = len(not_documented_columns.keys())

        total_number_columns = number_documented_columns + number_not_documented_columns
        percentage_not_documented_columns = round(
            100 - ((number_not_documented_columns / total_number_columns) * 100), 2
        )

        # To print nice the data with the total coverage at the last column.
        data = {
            (column): (
                str(percentage_not_documented_columns)
                if i == (len(not_documented_columns.keys()) - 1)
                else ""
            )
            for i, column in enumerate(not_documented_columns.keys())
        }

        self.create_table(
            title="Documentation Coverage", columns=["undocument columns", "coverage"], data=data
        )

    def create_table(self, title: str, columns: List[str], data: Dict[str, Any]) -> None:
        """
        Method to create a nice table to print the results.

        Args:
            title (str): Title that you want to give to the table.
            columns (List[str]): List of columns that the table is going to have.
            data (Dict[str, Any]): with the rows that we want to print.
        """
        table = Table(title)
        for column in columns:
            table.add_column(column, justify="right", style="bright_yellow", no_wrap=True)

        for model, percentage in data.items():
            table.add_row("", model, str(percentage))

        console = Console()
        console.print(table)

    def get_all_columns_tests_statistics_per_model(self) -> None:
        """
        Method to get the model descriptions statistics per DBT project.

        For the process a column is not tested if it does not have any tests.
        """
        print_statistics = {}
        total_number_columns = 0
        number_columns_without_tests = 0

        for model_name in self.dbt_tests.keys():
            columns = self.dbt_tests[model_name]
            model_number_columns = 0
            model_columns_without_tests = 0
            for column in columns:
                tests = column["tests"]
                total_number_columns += 1
                model_number_columns += 1

                if len(tests) == 0:
                    number_columns_without_tests += 1
                    model_columns_without_tests += 1

            print_statistics[model_name] = str(
                round(100 - ((model_columns_without_tests / model_number_columns) * 100), 2)
            )

        print_statistics[""] = ""
        print_statistics["TOTAL"] = str(
            round(100 - ((number_columns_without_tests / total_number_columns) * 100), 2)
        )

        self.create_table(
            title="Test Coverage",
            columns=["Model Name", "% coverage"],
            data=print_statistics,
        )

    def get_all_columns_descriptions_statistics_per_model(self) -> None:
        """
        Method to get the model tests statistics per DBT project.

        For the process a column is not documented if the description is:
        Not No description for this column.
        """
        print_statistics = {}
        for model_name, path in self.all_dbt_models.items():
            content = open_yaml(path)
            documented_columns = self.get_documented_columns(
                content=content,
                model_name=model_name,
            )
            not_documented_columns = self.get_not_documented_columns(
                content=content,
                model_name=model_name,
            )
            number_documented_columns = len(documented_columns.keys())
            number_not_documented_columns = len(not_documented_columns.keys())

            total_number_columns = number_documented_columns + number_not_documented_columns

            percentage_not_documented_columns = round(
                100 - ((number_not_documented_columns / total_number_columns) * 100), 2
            )
            print_statistics[model_name] = str(percentage_not_documented_columns)

        print_statistics[""] = ""
        print_statistics["TOTAL"] = self.get_total_columns_descriptions_statistics()

        self.create_table(
            title="Documentation Coverage",
            columns=["Model Name", "% coverage"],
            data=print_statistics,
        )

    def get_total_columns_descriptions_statistics(self) -> str:
        """
        Method to get the descriptions statistics for an entire DBT project.

        For the process a column is not documented if the description is:
        Not No description for this column.

        Returns:
            str: with global descriptions statistics.
        """
        number_not_documented_columns = 0
        number_documented_columns = 0

        for description in self.dbt_definitions.values():
            if description == COLUMN_NOT_DOCUMENTED:
                number_not_documented_columns += 1
            else:
                number_documented_columns += 1

        total_number_columns = number_documented_columns + number_not_documented_columns

        percentage_not_documented_columns = round(
            100 - ((number_not_documented_columns / total_number_columns) * 100), 2
        )
        return str(percentage_not_documented_columns)
