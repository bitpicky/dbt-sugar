"""Contains all dbt-sugar custom exceptions."""


class DbtSugarException(Exception):
    """Custom Exeption type mainly there to print things in Red."""

    def __init__(self, message: str):  # noqa D107
        # ! TODO: I would like to be able to always colour the message.
        # ! depending what colouring scheme we use we'll have to set this up
        # super().__init__(f"{Fore.RED}{message}"  # example using colorama.
        super().__init__(f"{message}")


class InvalidOrMissingCommandError(DbtSugarException):
    """when a non-implemented or missing command is asked for."""
