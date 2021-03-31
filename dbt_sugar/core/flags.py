"""Flags module containing the FlagParser "Factory"."""
import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import List, Optional


class FlagParser:
    """Sets flags from defaults or by parsing CLI arguments.

    In order to not have to always parse args when testing etc. We set the defaults explicitly here.
    This is a bit strict and not very DRY but it saves from surprises which for now is good.
    """

    def __init__(self, cli_parser: ArgumentParser) -> None:
        """Constructor for FlagParser.

        Holds explicit defaults and consumes parsed flags if asked for it.

        Args:
            cli_parser (ArgumentParser): CLI parser.
        """
        self.cli_parser = cli_parser
        self.model: str = "test_model"
        self.schema: str = ""
        self.log_level: str = "info"
        self.syrup: str = str()
        self.config_path: Path = Path(str())
        self.profiles_dir: Optional[Path] = None
        self.is_dry_run: bool = False
        self.ask_for_tests: bool = True
        self.ask_for_tags: bool = True
        self.target: str = str()
        self.verbose: bool = False

    def consume_cli_arguments(self, test_cli_args: List[str] = list()) -> None:
        if test_cli_args:
            _cli_args = test_cli_args
        else:
            _cli_args = sys.argv[1:]
        self.args = self.cli_parser.parse_args(_cli_args)

        self.task = self.args.command

        # base flags that need to be set no matter what
        if self.args:
            self.log_level = self.args.log_level
            self.verbose = self.args.verbose
            self.syrup = self.args.syrup
            if self.args.profiles_dir:
                self.profiles_dir = Path(self.args.profiles_dir).expanduser()
            if self.args.config_path:
                self.config_path = Path(self.args.config_path).expanduser()

        # task specific args consumption
        if self.task == "doc":
            self.model = self.args.model
            self.schema = self.args.schema
            self.is_dry_run = self.args.dry_run
            self.target = self.args.target
            # we reverse the flag so that we don't have double negatives later in the code
            self.ask_for_tests = self.args.ask_for_tests
            self.ask_for_tags = self.args.ask_for_tags

        if self.task == "audit":
            self.model = self.args.model
