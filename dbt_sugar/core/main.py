"""Main module for dbt-sugar. Sets up CLI arguments and sets up task handlers."""
import argparse
import sys
from typing import List, Union

from dbt_sugar.core._version import __version__
from dbt_sugar.core.clients.dbt import DbtProfile
from dbt_sugar.core.config.config import DbtSugarConfig
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.logger import GLOBAL_LOGGER as logger
from dbt_sugar.core.logger import log_manager
from dbt_sugar.core.task.base import BaseTask
from dbt_sugar.core.task.doc import DocumentationTask
from dbt_sugar.core.utils import check_and_compare_version


def check_and_print_version() -> str:
    """Calls check_and_compare_version and formats a message that works both for main and argparse.

    Returns:
        str: version info message ready for printing
    """
    needs_update, latest_version = check_and_compare_version()
    installed_version_message = f"Installed dbt-sugar version: {__version__}".rjust(40)
    latest_version_message = f"Latest dbt-sugar version: {latest_version}".rjust(40)
    if latest_version:
        return "\n".join([installed_version_message, latest_version_message])
    return installed_version_message


# general parser
parser = argparse.ArgumentParser(
    prog="dbt-sugar",
    formatter_class=argparse.RawTextHelpFormatter,
    description="CLI tool to help users document their dbt models",
    epilog="Select onf of the available sub-commands with --help to find out more about them.",
)

parser.add_argument("-v", "--version", action="version", version=check_and_print_version())

# base sub-parser (sets up args that need to be provided to ALL other sub parsers)
base_subparser = argparse.ArgumentParser(add_help=False)
base_subparser.add_argument(
    "--log-level", help="overrides default log level", type=str, default=str()
)
base_subparser.add_argument(
    "--full-tracebacks",
    help="When provided the length of the tracebacks will not be truncated.",
    action="store_true",
    default=False,
)
base_subparser.add_argument(
    "--syrup",
    help="Name of the syrup confi you wish to use. If left empty dbt-sugar will attempt to read your defaults.",
    type=str,
    default=str(),
)
base_subparser.add_argument(
    "--config-path", help="Full path to config.yml file if not using default."
)
base_subparser.add_argument(
    "--profiles-dir", help="Alternative path to the dbt profiles.yml file.", type=str, default=str()
)

# Task-specific argument sub parsers
sub_parsers = parser.add_subparsers(title="Available dbt-sugar commands", dest="command")

# document task parser
document_sub_parser = sub_parsers.add_parser(
    "doc", parents=[base_subparser], help="Runs documentation and test enforement task."
)
document_sub_parser.set_defaults(cls=DocumentationTask, which="doc")
document_sub_parser.add_argument(
    "-m", "--model", help="Name of the dbt model to document", type=str, default=None
)
document_sub_parser.add_argument(
    "-s",
    "--schema",
    help="Name of the database schema in which the model resides",
    type=str,
    default=None,
)
document_sub_parser.add_argument(
    "--dry-run",
    help="When provided the documentation task will not modify your files",
    action="store_true",
    default=False,
)


# task handler
def handle(
    parser: argparse.ArgumentParser,
    test_cli_args: List[str] = list(),
) -> Union[int, BaseTask]:
    """Task handler factory.

    Args:
        parser (argparse.ArgumentParser): CLI argument parser object.
    """
    flag_parser = FlagParser(parser)
    flag_parser.consume_cli_arguments(test_cli_args=test_cli_args)

    sugar_config = DbtSugarConfig(flag_parser)
    sugar_config.load_config()
    # TODO: Feed project_name dynamically at run time from CLI or config.
    dbt_profile = DbtProfile(
        profile_name="default",
        project_name="default",
        target_name="dev",
        profiles_dir=flag_parser.profiles_dir,
    )

    if flag_parser.log_level == "debug":
        log_manager.set_debug()

    if flag_parser.task == "doc":
        task: DocumentationTask = DocumentationTask(flag_parser, dbt_profile)
        # TODO: We actually need to change the behaviour of DocumentationTask to provide an interactive
        # dry run but for now this allows testing without side effects.
        # the current implementation upsets mypy also.
        if flag_parser.is_dry_run:
            logger.warning("Running in --dry-run mode no files will be modified")
            return task
        return task.run()

    raise NotImplementedError(f"{flag_parser.task} is not supported.")


def main(parser: argparse.ArgumentParser = parser, test_cli_args: List[str] = list()) -> int:
    """Just your boring main."""
    exit_code = 0
    _cli_args = list()
    if test_cli_args:
        _cli_args = test_cli_args

    # print version on every run unless doing `--version` which is better handled by argparse
    if "--version" not in sys.argv[1:]:
        version_message = check_and_print_version()
        print(version_message)
        print("\n")
    # TODO: Update this when a proper dry-run exists.
    exit_code = handle(parser, _cli_args)  # type: ignore

    exit(exit_code)
