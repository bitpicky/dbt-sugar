"""Main module for dbt-sugar. Sets up CLI arguments and sets up task handlers."""
import argparse

from dbt_sugar.core._version import __version__
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.logger import log_manager
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

# Task-specific argument sub parsers
sub_parsers = parser.add_subparsers(title="Available dbt-sugar commands", dest="command")

# document task parser
document_sub_parser = sub_parsers.add_parser(
    "doc", parents=[base_subparser], help="Runs documentation and test enforement task."
)
document_sub_parser.set_defaults(cls=DocumentationTask, which="doc")
document_sub_parser.add_argument(
    "-m", "--model", help="dbt model name to document", type=str, default=None
)


# task handler
def handle(parser: argparse.ArgumentParser) -> int:
    """Task handler factory.

    Args:
        parser (argparse.ArgumentParser): CLI argument parser object.

    Returns:
        Union[DocumentTask, InitTask]: Task object to be run.
    """
    flag_parser = FlagParser(parser)
    flag_parser.consume_cli_arguments()

    if flag_parser.log_level == "debug":
        log_manager.set_debug()

    if flag_parser.task == "doc":
        task: DocumentationTask = DocumentationTask(flag_parser)
        return task.run()

    raise NotImplementedError(f"{flag_parser.task} is not supported.")
