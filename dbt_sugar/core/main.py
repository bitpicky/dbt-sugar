"""Main module for dbt-sugar. Sets up CLI arguments and sets up task handlers."""
import argparse
import sys
from typing import List

import pyfiglet
from rich.console import Console

from dbt_sugar.core._version import __version__
from dbt_sugar.core.clients.dbt import DbtProfile, DbtProject
from dbt_sugar.core.config.config import DbtSugarConfig
from dbt_sugar.core.flags import FlagParser
from dbt_sugar.core.logger import GLOBAL_LOGGER as logger
from dbt_sugar.core.logger import log_manager
from dbt_sugar.core.task.audit import AuditTask
from dbt_sugar.core.task.doc import DocumentationTask
from dbt_sugar.core.ui.traceback_manager import DbtSugarTracebackManager
from dbt_sugar.core.utils import check_and_compare_version

console = Console()


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
    "-vv",
    "--verbose",
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
    "--profiles-dir", help="Alternative path to the dbt profiles.yml file.", type=str
)

# Task-specific argument sub parsers
sub_parsers = parser.add_subparsers(title="Available dbt-sugar commands", dest="command")

# document task parser
document_sub_parser = sub_parsers.add_parser(
    "doc", parents=[base_subparser], help="Runs documentation and test enforement task."
)
document_sub_parser.set_defaults(cls=DocumentationTask, which="doc")
# TODO: We shouldn't be requiring this if we have a `--model` format as it's considered bad practice
document_sub_parser.add_argument(
    "-m", "--model", help="Name of the dbt model to document", type=str, default=None, required=True
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
document_sub_parser.add_argument(
    "-t",
    "--target",
    help="Which target from the dbt profile to load.",
    type=str,
    default=str(),
)
# document_sub_parser.add_argument(

document_sub_parser.add_argument(
    "--no-ask-tests",
    help="When provided the documentation task will not ask for adding TAGs into the model.",
    action="store_false",
    dest="ask_for_tests",
)

document_sub_parser.add_argument(
    "--ask-tests",
    help="When passed dbt-sugar will ask you if you want to add tests to your models.",
    action="store_true",
    dest="ask_for_tests",
    default=True,
)

document_sub_parser.add_argument(
    "--no-ask-tags",
    help="When provided the documentation task will not ask for adding TAGs into the model.",
    action="store_false",
    dest="ask_for_tags",
)

document_sub_parser.add_argument(
    "--ask-tags",
    help="When passed dbt-sugar will ask you if you want to add tests to your models.",
    action="store_true",
    dest="ask_for_tags",
    default=True,
)

# document task parser
audit_sub_parser = sub_parsers.add_parser(
    "audit", parents=[base_subparser], help="Runs audit task."
)
audit_sub_parser.set_defaults(cls=AuditTask, which="audit")
audit_sub_parser.add_argument(
    "-m",
    "--model",
    help="Name of the dbt model to document",
    type=str,
    default=None,
    required=False,
)

# task handler


def handle(
    parser: argparse.ArgumentParser,
    test_cli_args: List[str] = list(),
) -> int:
    """Task handler factory.

    Args:
        parser (argparse.ArgumentParser): CLI argument parser object.
    """
    flag_parser = FlagParser(parser)
    flag_parser.consume_cli_arguments(test_cli_args=test_cli_args)

    # set up traceback manager fo prettier errors
    DbtSugarTracebackManager(flag_parser)

    sugar_config = DbtSugarConfig(flag_parser)
    sugar_config.load_config()

    dbt_project = DbtProject(
        sugar_config.dbt_project_info.get("name", str()),
        sugar_config.dbt_project_info.get("path", str()),
    )
    dbt_project.read_project()

    dbt_profile = DbtProfile(
        flags=flag_parser,
        profile_name=dbt_project.profile_name,
        target_name=flag_parser.target,
        profiles_dir=flag_parser.profiles_dir,
    )
    dbt_profile.read_profile()

    if flag_parser.log_level == "debug":
        log_manager.set_debug()

    if flag_parser.task == "doc":
        task: DocumentationTask = DocumentationTask(
            flag_parser, dbt_profile, sugar_config, dbt_project._project_dir
        )
        # TODO: We actually need to change the behaviour of DocumentationTask to provide an interactive
        # dry run but for now this allows testing without side effects.
        # the current implementation upsets mypy also.
        if flag_parser.is_dry_run:
            logger.warning("[yellow]Running in --dry-run mode no files will be modified")
            logger.info(f"Would run {task}")
            return 0
        return task.run()

    if flag_parser.task == "audit":
        audit_task: AuditTask = AuditTask(
            flag_parser, dbt_project._project_dir, sugar_config=sugar_config
        )
        return audit_task.run()

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
        # print app logo with pyfiglet
        logo_str = str(pyfiglet.figlet_format("dbt-sugar", font="slant"))
        console.print(logo_str, style="blue")
        print("Getting sweetness out of the cupboard 🍬! \n")
    # TODO: Update this when a proper dry-run exists.
    exit_code = handle(parser, _cli_args)  # type: ignore

    if exit_code > 0:
        logger.error("[red]The process you were running did not complete successfully.")
    return exit_code


if __name__ == "__main__":
    exit(main())
