"""Contains all dbt-sugar custom exceptions."""


class DbtSugarException(Exception):
    """Custom Exeption type mainly there to print things in Red."""

    def __init__(self, message: str):  # noqa D107
        # ! TODO: I would like to be able to always colour the message.
        # ! depending what colouring scheme we use we'll have to set this up
        # super().__init__(f"{Fore.RED}{message}"  # example using colorama.
        super().__init__(f"{message}")


class DbtProfileFileMissing(DbtSugarException):
    """Thrown when the `profiles.yml` cannot be found in its expected or provided location."""


class YAMLFileEmptyError(DbtSugarException):
    """Thrown when a yamlfile existed but had nothing in it."""


class ProfileParsingError(DbtSugarException):
    """Thrown when no target entry could be found."""


class SyrupNotFoundError(DbtSugarException):
    """Thrown when a syrup config could not be extracted from the config.yaml."""


class NoSyrupProvided(DbtSugarException):
    """Thrown when neither a default syrup nor a cli-passed syrup can be found."""


class MissingDbtProjects(DbtSugarException):
    """Thrown when one or more in-scope dbt projects could not be found."""


class TargetNameNotProvided(DbtSugarException):
    """Thrown when no `target:` entry is provided in the profiles.yml and not passed on CLI."""


class KnownRegressionError(DbtSugarException):
    """Thrown when we want to warn users of a known regression or limitation that is not implemented."""
