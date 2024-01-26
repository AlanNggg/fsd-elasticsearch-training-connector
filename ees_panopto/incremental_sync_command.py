#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""This module allows to run an incremental sync against a Sharepoint Server instance.

It will attempt to sync documents that have changed or have been added in the
third-party system recently and ingest them into Enterprise Search instance.

Recency is determined by the time when the last successful incremental or full job
was ran."""
from datetime import datetime

from .base_command import BaseCommand
from .checkpointing import Checkpoint
from .connector_queue import ConnectorQueue
from .sync_elastic_search import SyncElasticSearch
from .sync_enterprise_search import SyncEnterpriseSearch
from .sync_panopto import SyncPanopto
from .utils import get_current_time, split_date_range_into_chunks

INDEXING_TYPE = "incremental"


class IncrementalSyncCommand(BaseCommand):
    """This class start execution of incremental sync feature."""

    def start_producer(self, queue, time_range):
        """This method starts async calls for the producer which is responsible for fetching documents from the
        SharePoint and pushing them in the shared queue
        :param queue: Shared queue to fetch the stored documents
        """
        self.logger.debug("Starting the incremental indexing..")

        thread_count = self.config.get_value("panopto_sync_thread_count")

        start_time, end_time = time_range["start_time"], time_range["end_time"]

        try:
            sync_panopto = SyncPanopto(
                self.config,
                self.logger,
                self.mssql_client,
                self.indexing_rules,
                queue,
                self.leadtools_engine,
                self.panopto_client,
                start_time,
                end_time,
            )
            datelist = split_date_range_into_chunks(
                start_time,
                end_time,
                thread_count,
            )
            time_range_list = [(datelist[num], datelist[num + 1])
                               for num in range(0, thread_count)]
            storage_with_collection = self.local_storage.get_storage_with_collection()
            global_keys = self.create_jobs(
                thread_count, sync_panopto.perform_sync, (), time_range_list)

            try:
                storage_with_collection["global_keys"]["videos"].update(
                    global_keys)
            except ValueError as value_error:
                self.logger.error(
                    f"Exception while updating storage: {value_error}")

            for _ in range(self.config.get_value("enterprise_search_sync_thread_count")):
                queue.end_signal()

        except Exception as exception:
            self.logger.exception(
                f"Error while fetching the objects . Error {exception}")
            raise exception
        self.local_storage.update_storage(storage_with_collection)

    def start_consumer(self, queue):
        """This method starts async calls for the consumer which is responsible for indexing documents to the
        Enterprise Search
        :param queue: Shared queue to fetch the stored documents
        """
        logger = self.logger
        thread_count = self.config.get_value(
            "enterprise_search_sync_thread_count")
        sync_es = SyncElasticSearch(
            self.config, logger, self.elastic_search_custom_client, queue)

        self.create_jobs(
            thread_count, sync_es.perform_sync, (True,), None)

        results = sync_es.get_status()

        return results

    def execute(self):
        """This function execute the start function."""
        config = self.config
        logger = self.logger
        current_time = get_current_time()

        checkpoint = Checkpoint(config, logger)

        start_time, end_time = checkpoint.get_checkpoint(
            current_time, 'panopto')
        time_range = {
            "start_time": start_time,
            "end_time": end_time,
        }
        logger.info(f"Indexing started at: {current_time}")

        queue = ConnectorQueue(logger)

        self.start_producer(queue, time_range)

        results = self.start_consumer(queue)

        checkpoint.set_checkpoint(current_time, INDEXING_TYPE, 'panopto')
        logger.info(f"Indexing ended at: {get_current_time()}")

        return results
