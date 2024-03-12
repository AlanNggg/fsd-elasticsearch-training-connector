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

        if read_from_db:
            if self.__configurations["include"]["ocr_path_template"]:
                self.__configurations["include"]["ocr_path_template"] += self.get_custom_ocr_configure_from_db()
            else:
                self.__configurations["include"]["ocr_path_template"] = self.get_custom_ocr_configure_from_db(
                )

            if self.__configurations["categories"]:
                self.__configurations["categories"].update(
                    self.get_categories_from_db())
            else:
                self.__configurations["categories"] = self.get_categories_from_db(
                )

    def get_custom_ocr_configure_from_db(self):
        try:
            host = self.get_value('fsd_search_db.host')
            database = self.get_value('fsd_search_db.database')
            username = self.get_value('fsd_search_db.username')
            password = self.get_value('fsd_search_db.password')
            connection = pymysql.connect(host=host,
                                         user=username,
                                         password=password,
                                         database=database,
                                         cursorclass=pymysql.cursors.DictCursor)

            with connection:
                with connection.cursor() as cursor:
                    # Read a single record
                    sql = "SELECT path, language FROM `ocr_path_setting` WHERE `source`=%s"
                    cursor.execute(sql, ('panopto',))
                    result = cursor.fetchall()
                    paths = list(map(lambda x: x, result))

            return paths
        except Exception as exception:
            return []

    def get_categories_from_db(self):
        try:
            host = self.get_value('fsd_search_db.host')
            database = self.get_value('fsd_search_db.database')
            username = self.get_value('fsd_search_db.username')
            password = self.get_value('fsd_search_db.password')
            connection = pymysql.connect(host=host,
                                         user=username,
                                         password=password,
                                         database=database,
                                         cursorclass=pymysql.cursors.DictCursor)

            with connection:
                with connection.cursor() as cursor:
                    sql = "SELECT value, type FROM `extension`"
                    cursor.execute(sql)
                    results = cursor.fetchall()

                    categories = {}
                    for result in results:
                        value = result['value']
                        type_ = json.loads(result['type'])
                        type_ = [list(item.keys())[0] for item in type_]
                        categories[value] = type_

            return categories
        except Exception as exception:
            return {}

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
