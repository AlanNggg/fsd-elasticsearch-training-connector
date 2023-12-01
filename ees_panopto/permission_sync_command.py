#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""This module allows to run a deletion sync against a Sharepoint Server instance.

It will attempt to remove from Enterprise Search instance the documents
that have been deleted from the third-party system."""
import csv
import os

from .base_command import BaseCommand
from .checkpointing import Checkpoint


class PermissionSyncDisabledException(Exception):
    """Exception raised when permission sync is disabled, but expected to be enabled.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message="Provided configuration was invalid"):
        super().__init__(message)
        self.message = message


class PermissionSyncCommand(BaseCommand):
    """This class contains logic to sync user permissions from Sharepoint Server.

    It can be used to run the job that will periodically sync permissions
    from Sharepoint Server to Elastic Enteprise Search."""

    def __init__(self, args):
        super().__init__(args)


    def execute(self):
        """Run the command.
        This method is overridden by actual commands with logic
        that is specific to each command implementing it."""
        raise NotImplementedError
