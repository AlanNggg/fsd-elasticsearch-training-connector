#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Configuration module allows manipulations with application configuration.

    This module can be used to read and validate configuration file that defines
    the settings of the Network Drives Server connector.
"""
import json

import pymysql
import pymysql.cursors
import yaml
from cerberus import Validator
from yaml.error import YAMLError

from .constant import RFC_3339_DATETIME_FORMAT
from .fsd_search_portal_client import FsdSearchPortalClient
from .schema import schema


class ConfigurationInvalidException(Exception):
    """Exception raised when configuration was invalid.

    Attributes:
        errors - errors found in the configuration
        message -- explanation of the error
    """

    def __init__(self, errors):
        super().__init__(
            f"Provided configuration was invalid. Errors: {errors}.")

        self.errors = errors


class ConfigurationParsingException(Exception):
    """Exception raised when configuration could not be parsed.

    Attributes:
        file_name - name of the file that could not be parsed
    """

    def __init__(self, file_name, inner_exception):
        super().__init__("Failed to parse configuration file.")

        self.file_name = file_name
        self.inner_exception = inner_exception


class Configuration:
    """Configuration class is responsible for parsing, validating and accessing
    configuration options from connector configuration file."""

    def __init__(self, file_name, config_json=None, read_from_db=True):
        self.__configurations = {}
        self.file_name = file_name
        self.fsd_search_portal_client = None
        try:
            with open(file_name, encoding='utf-8') as stream:
                self.__configurations = yaml.safe_load(stream)

            if config_json:
                self.__configurations.update(config_json)

        except YAMLError as exception:
            raise ConfigurationParsingException(file_name, exception)
        self.__configurations = self.validate()
        if self.__configurations["start_time"] >= self.__configurations["end_time"]:
            raise ConfigurationInvalidException(f"The start_time: {self.__configurations['start_time']}  \
                    cannot be greater than or equal to the end_time: {self.__configurations['end_time']}")

        for date_config in ["start_time", "end_time"]:
            value = self.__configurations[date_config]
            self.__configurations[date_config] = self.__parse_date_config_value(
                value)

        self.create_fsd_search_portal_client()

        if read_from_db:
            if self.__configurations["include"]["ocr_path_template"]:
                self.__configurations["include"]["ocr_path_template"] += self.fsd_search_portal_client.get_custom_ocr_configure(
                    'training')
            else:
                self.__configurations["include"]["ocr_path_template"] = self.fsd_search_portal_client.get_custom_ocr_configure(
                    'training')

            if self.__configurations["categories"]:
                self.__configurations["categories"].update(
                    self.fsd_search_portal_client.get_categories())
            else:
                self.__configurations["categories"] = self.fsd_search_portal_client.get_categories(
                )

    def create_fsd_search_portal_client(self):
        if self.fsd_search_portal_client is None:
            host = self.get_value('fsd_search_db.host')
            database = self.get_value('fsd_search_db.database')
            username = self.get_value('fsd_search_db.username')
            password = self.get_value('fsd_search_db.password')

            self.fsd_search_portal_client = FsdSearchPortalClient(
                host, database, username, password)

    def validate(self):
        """Validates each properties defined in the yaml configuration file
        """
        validator = Validator(schema)
        validator.validate(self.__configurations, schema)
        if validator.errors:
            raise ConfigurationInvalidException(validator.errors)
        return validator.document

    def get_value(self, key):
        """Returns a configuration value that matches the key argument"""

        return self.__configurations.get(key)

    @staticmethod
    def __parse_date_config_value(string):
        return string.strftime(RFC_3339_DATETIME_FORMAT)
