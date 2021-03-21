"""Audit Task module."""
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
        self.model_content: Dict[str, Any] = {}

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
            self.derive_model_coverage()
        else:
            self.derive_project_coverage()
        return 0

    def derive_model_coverage(self) -> None:
        """Method to get the coverage from a specific model."""
        self.get_model_column_description_coverage()
        self.get_model_test_coverage()

    def derive_project_coverage(self) -> None:
        """Method to get the coverage from a DBT project."""
        self.get_project_column_description_coverage()
        self.get_project_test_coverage()

    def get_model_test_coverage(self) -> None:
        """Method to get the tests coverage from a specific model."""
        # Init variables
        model_number_columns = 0
        model_columns_without_tests = 0
        untested_columns = []

        columns = self.dbt_tests.get(self.model_name)

        if not columns:
            logger.info(f"Not able to get the test statistics for the model {self.model_name}")
            return

        for column in columns:
            model_number_columns += 1
            if len(column["tests"]) == 0:
                model_columns_without_tests += 1
                untested_columns.append(column["name"])

        percentage_not_tested_columns = self.calculate_coverage_percentage(
            number_failures=model_columns_without_tests, total=model_number_columns
        )

        data = self.print_nicely_the_data(
            data=untested_columns, total=percentage_not_tested_columns
        )

        self.create_table(
            title="Test Coverage", columns=["undocument columns", "coverage"], data=data
        )

    def get_model_column_description_coverage(self) -> None:
        """Method to get the descriptions coverage from a specific model."""
        not_documented_columns = self.get_not_documented_columns(
            content=self.model_content,
            model_name=self.model_name,
        ).keys()

        number_not_documented_columns = len(not_documented_columns)
        number_documented_columns = len(
            self.get_documented_columns(
                content=self.model_content,
                model_name=self.model_name,
            )
        )

        percentage_not_documented_columns = self.calculate_coverage_percentage(
            number_failures=number_not_documented_columns,
            total=(number_documented_columns + number_not_documented_columns),
        )

        data = self.print_nicely_the_data(
            data=list(not_documented_columns), total=percentage_not_documented_columns
        )

        self.create_table(
            title="Documentation Coverage", columns=["undocument columns", "coverage"], data=data
        )

    def print_nicely_the_data(self, data: List[str], total: str) -> Dict[str, str]:
        """
        This method to transform a list into a dictionary with the data

        as the keys and the total as the last element value.

        Args:
            data (List): list of data to modify.

        Returns:
            Dict[str, str]: with a dictionary with the data as keys and
            the total as a value but only for the last element in the list.
        """
        return {
            (column): (str(total) if i == (len(data) - 1) else "") for i, column in enumerate(data)
        }

    def create_table(self, title: str, columns: List[str], data: Dict[str, str]) -> None:
        """
        Method to create a nice table to print the results.

        Args:
            title (str): Title that you want to give to the table.
            columns (List[str]): List of columns that the table is going to have.
            data (Dict[str, str]): with the rows that we want to print.
        """
        table = Table(title)
        for column in columns:
            table.add_column(column, justify="right", style="bright_yellow", no_wrap=True)

        for model, percentage in data.items():
            table.add_row("", model, percentage)

        console = Console()
        console.print(table)

    def get_project_test_coverage(self) -> None:
        """Method to get the model tests coverage per model in a DBT project."""
        print_statistics = {}
        total_number_columns = 0
        number_columns_without_tests = 0

        for model_name in self.dbt_tests.keys():
            columns = self.dbt_tests[model_name]

            model_number_columns = 0
            model_columns_without_tests = 0

            for column in columns:
                total_number_columns += 1
                model_number_columns += 1

                if len(column["tests"]) == 0:
                    number_columns_without_tests += 1
                    model_columns_without_tests += 1

            print_statistics[model_name] = self.calculate_coverage_percentage(
                number_failures=model_columns_without_tests, total=model_number_columns
            )

        print_statistics[""] = ""
        print_statistics["TOTAL"] = self.calculate_coverage_percentage(
            number_failures=number_columns_without_tests, total=total_number_columns
        )

        self.create_table(
            title="Test Coverage",
            columns=["Model Name", "% coverage"],
            data=print_statistics,
        )

    def get_project_column_description_coverage(self) -> None:
        """Method to get the model descriptions coverage per model in a DBT project."""
        print_statistics = {}
        for model_name, path in self.all_dbt_models.items():
            content = open_yaml(path)

            number_documented_columns = len(
                self.get_documented_columns(
                    content=content,
                    model_name=model_name,
                )
            )

            number_not_documented_columns = len(
                self.get_not_documented_columns(
                    content=content,
                    model_name=model_name,
                )
            )

            print_statistics[model_name] = self.calculate_coverage_percentage(
                number_failures=number_not_documented_columns,
                total=(number_documented_columns + number_not_documented_columns),
            )

        print_statistics[""] = ""
        print_statistics["TOTAL"] = self.get_project_total_test_coverage()

        self.create_table(
            title="Documentation Coverage",
            columns=["Model Name", "% coverage"],
            data=print_statistics,
        )

    def calculate_coverage_percentage(self, number_failures: int, total: int) -> str:
        """
        Method to calculate the percentage of coverage.

        Args:
            number_failures (int): With the number of failures.
            total (int): With the number of total cases.

        Returns:
            str: with the calculation of the percentage.
        """
        if total == 0:
            return "0.0"

        percentage_failure = round((1 - (number_failures / total)) * 100, 2)
        return str(percentage_failure)

    def get_project_total_test_coverage(self) -> str:
        """
        Method to get the descriptions coverage for an entire DBT project.

        Returns:
            str: with global descriptions statistics.
        """
        number_not_documented_columns = 0
        number_of_columns = 0

        for description in self.dbt_definitions.values():
            if description == COLUMN_NOT_DOCUMENTED:
                number_not_documented_columns += 1
            number_of_columns += 1

        return self.calculate_coverage_percentage(
            number_failures=number_not_documented_columns,
            total=number_of_columns,
        )
