#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""Module contains a base command interface.

Connector can run multiple commands such as full-sync, incremental-sync,
etc. This module provides convenience interface defining the shared
objects and methods that will can be used by commands."""
import logging
import sys

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from concurrent.futures import ThreadPoolExecutor, as_completed

from .configuration import Configuration
from .elastic_search_wrapper import ElasticSearchWrapper
from .enterprise_search_wrapper import EnterpriseSearchWrapper
from .indexing_rule import IndexingRules
from .local_storage import LocalStorage
from .mssql_client import MSSQL
from .panopto_client import Panopto


class BaseCommand:
    """Base interface for all module commands.
    Inherit from it and implement 'execute' method, then add
    code to cli.py to register this command."""

    def __init__(self, args):
        self.args = args

    def execute(self):
        """Run the command.
        This method is overridden by actual commands with logic
        that is specific to each command implementing it."""
        raise NotImplementedError

    @cached_property
    def logger(self):
        """Get the logger instance for the running command.
        log level will be determined by the configuration
        setting log_level.
        """
        log_level = self.config.get_value('log_level')
        logger = logging.getLogger(__name__)
        logger.propagate = True
        logger.setLevel(log_level)

        stream_handler = logging.StreamHandler()

        info_log_file = self.args.info_log_file
        info_file_handler = logging.FileHandler(info_log_file, mode='w', encoding='utf-8')

        error_log_file = self.args.error_log_file
        error_file_handler = logging.FileHandler(error_log_file, mode='w', encoding='utf-8')
        # Uncomment the following lines to output logs in ECS-compatible format
        # formatter = ecs_logging.StdlibFormatter()
        # handler.setFormatter(formatter)
        stream_handler.setLevel(log_level)
        info_file_handler.setLevel(logging.INFO)
        error_file_handler.setLevel(logging.ERROR)

        stream_handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        )
        info_file_handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        )
        error_file_handler.setFormatter(
            logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        )

        logger.addHandler(stream_handler)
        logger.addHandler(info_file_handler)
        logger.addHandler(error_file_handler)

        return logger

    @cached_property
    def workplace_search_custom_client(self):
        """Get the workplace search custom client instance for the running command.
        """
        return EnterpriseSearchWrapper(self.logger, self.config, self.args)

    @cached_property
    def elastic_search_custom_client(self):
        return ElasticSearchWrapper(self.logger, self.config, self.args)
    
    @cached_property
    def config(self):
        """Get the configuration for the connector for the running command."""
        file_name = self.args.config_file
        return Configuration(file_name)

    @cached_property
    def mssql_client(self):
        """Get the Network Drives client instance for the running command."""
        return MSSQL(self.config, self.logger)

    @cached_property
    def indexing_rules(self):
        """Get the object for indexing rules to check should the file be indexed or not
            based on the patterns defined in configuration file.
        """
        return IndexingRules(self.config)
    
    def create_jobs(self, thread_count, func, args, iterable_list):
        """Apply async calls using multithreading to the targeted function
        :param thread_count: Total number of threads to be spawned
        :param func: The target function on which the async calls would be made
        :param args: Arguments for the targeted function
        :param iterable_list: list to iterate over and create thread
        """
        documents = {}
        # If iterable_list is present, then iterate over the list and pass each list element
        # as an argument to the async function, else iterate over number of threads configured
        if iterable_list:
            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                future_to_path = {executor.submit(func, *args, list_element):
                                  list_element for list_element in iterable_list}

                for future in as_completed(future_to_path):
                    path = future_to_path[future]
                    try:
                        documents.update(future.result())
                    except Exception as exception:
                        self.logger.exception(f"Error while fetching in path {path}. Error {exception}")
        else:
            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                for _ in range(thread_count):
                    executor.submit(func, *args)
        return documents


    @staticmethod
    def producer(thread_count, func, args, items, wait=False):
        """Apply async calls using multithreading to the targeted function
        :param thread_count: Total number of threads to be spawned
        :param func: The target function on which the async calls would be made
        :param args: Arguments for the targeted function
        :param items: iterator of partition
        :param wait: wait until job completes if true, otherwise returns immediately
        """
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            futures = (executor.submit(func, *args, item) for item in items)
            if wait:
                result = [future.result() for future in as_completed(futures)]
                return result

    @staticmethod
    def consumer(thread_count, func):
        """Apply async calls using multithreading to the targeted function
        :param thread_count: Total number of threads to be spawned
        :param func: The target function on which the async calls would be made
        """
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            for _ in range(thread_count):
                executor.submit(func)

    @cached_property
    def local_storage(self):
        """Get the object for local storage to fetch and update ids stored locally"""
        return LocalStorage(self.logger)

    @cached_property
    def leadtools_engine(self):
        common_module = self.config.get_value('leadtools.common_module_python_path')
        print(common_module)
        sys.path.append(common_module)
        
        from .leadtools_engine import LeadTools

        return LeadTools(self.config, self.logger) 
    
    @cached_property
    def panopto_client(self):
        return Panopto(self.config, self.logger) 