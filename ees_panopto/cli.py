#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Cli module contains entry point for the package.

Endpoint provides a meaningful piece of functionality
related to uploading data from Network Drives
to Elastic Enterprise Search with subcommands."""

import getpass
import json
import os
from argparse import ArgumentParser, BooleanOptionalAction

from .bootstrap_command import BootstrapCommand
from .deletion_sync_command import DeletionSyncCommand
from .full_sync_command import FullSyncCommand
from .incremental_sync_command import IncrementalSyncCommand
from .permission_sync_command import PermissionSyncCommand

CMD_BOOTSTRAP = 'bootstrap'
CMD_FULL_SYNC = 'full-sync'
CMD_INCREMENTAL_SYNC = 'incremental-sync'
CMD_DELETION_SYNC = 'deletion-sync'
CMD_PERMISSION_SYNC = 'permission-sync'

commands = {
    CMD_BOOTSTRAP: BootstrapCommand,
    CMD_FULL_SYNC: FullSyncCommand,
    CMD_INCREMENTAL_SYNC: IncrementalSyncCommand,
    CMD_DELETION_SYNC: DeletionSyncCommand,
    CMD_PERMISSION_SYNC: PermissionSyncCommand,
}


def _parser():
    """Get a configured parser for the module.

    This method will initialize argument parser with a list
    of avaliable commands and their options."""
    parser = ArgumentParser(prog="ees_network_drive")
    parser.add_argument(
        "-c",
        '--config-file',
        type=str,
        metavar="CONFIGURATION_FILE_PATH",
        help="path to the configuration file"
    )
    parser.add_argument(
        "-e",
        '--error-log-file',
        type=str,
        metavar="ERROR_LOG_FILE_PATH",
        help="path to the error log file"
    )
    parser.add_argument(
        "-i",
        '--info-log-file',
        type=str,
        metavar="INFO_LOG_FILE_PATH",
        help="path to the info log file"
    )
    parser.add_argument(
        "-s",
        '--source',
        type=str,
        metavar="SOURCE",
        help="elasticsearch source"
    )
    parser.add_argument('-j', '--config-json', type=str,
                        metavar="CONFIGURATION_JSON", help='configuration json')
    parser.add_argument(
        "-r",
        '--read-config-from-db',
        metavar="READ_CONFIG_FROM_DB",
        help="read config from db",
        action=BooleanOptionalAction
    )

    subparsers = parser.add_subparsers(dest="cmd")
    subparsers.required = True
    bootstrap = subparsers.add_parser(CMD_BOOTSTRAP)
    bootstrap.add_argument(
        '-n',
        '--name',
        required=True,
        type=str,
        metavar="CONTENT_SOURCE_NAME",
        help="Name of the content source to be created"
    )
    bootstrap.add_argument(
        '-u',
        '--user',
        required=False,
        type=str,
        metavar="ENTERPRISE_SEARCH_ADMIN_USER_NAME",
        help="Username of the workplace search admin account"
    )

    subparsers.add_parser(CMD_FULL_SYNC)
    subparsers.add_parser(CMD_INCREMENTAL_SYNC)
    subparsers.add_parser(CMD_DELETION_SYNC)
    subparsers.add_parser(CMD_PERMISSION_SYNC)

    return parser


def main(args=None):
    """Entry point for the connector."""
    if args is None:
        parser = _parser()
        args = parser.parse_args()

    if args.cmd == CMD_BOOTSTRAP and args.user:
        args.password = getpass.getpass(prompt='Password: ', stream=None)

    if not args.config_file:
        args.config_file = os.path.join(os.path.expanduser(
            '~'), '.local', 'config', 'panopto_connector.yml')

    if not args.error_log_file:
        current_directory = os.getcwd()
        error_log_file = "error.log"
        args.error_log_file = os.path.join(current_directory, error_log_file)

    if not args.info_log_file:
        current_directory = os.getcwd()
        info_log_file = "info.log"
        args.info_log_file = os.path.join(current_directory, info_log_file)

    # parse json if exist
    if args.config_json:
        args.config_json = json.loads(args.config_json)

    run(args)


def run(args):
    """Run the command from the parsed args.

    This method takes already parsed and validated arguments
    and attempts to run the command with specified arguments."""
    results = commands[args.cmd](args).execute()

    if results:
        print(json.dumps(results))

    return 0
