#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""This module allows to create Content Source in Elastic Enterprise Search.

    It can be used to create a Content Source that will be used to upload the
    data to Elastic Enterprise Search instance.

    Otherwise, it's possible to use Content Source that was pre-created
    in Elastic Enterprise Search
"""

from .base_command import BaseCommand


class BootstrapCommand(BaseCommand):
    """This class defines a method to create a content source.
    """
    def execute(self):
        """Run the command.
        This method is overridden by actual commands with logic
        that is specific to each command implementing it."""
        raise NotImplementedError