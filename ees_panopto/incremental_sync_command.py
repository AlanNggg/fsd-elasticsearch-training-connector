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

    def start_producer(self, documents_to_index, time_range):
        """This method starts async calls for the producer which is responsible for fetching documents from the
        SharePoint and pushing them in the shared queue
        :param queue: Shared queue to fetch the stored documents
        """
        self.logger.debug("Starting the incremental indexing..")

        start_time, end_time = time_range["start_time"], time_range["end_time"]

        try:   
            sync_panopto = SyncPanopto(
                self.config,
                self.logger,
                self.mssql_client,
                self.indexing_rules,
                documents_to_index,
                self.leadtools_engine,
                self.panopto_client,
                start_time,
                end_time,
            )
            storage_with_collection = self.local_storage.get_storage_with_collection()
            global_keys = sync_panopto.perform_sync()

            try:
                storage_with_collection["global_keys"]["videos"].update(global_keys)
            except ValueError as value_error:
                self.logger.error(f"Exception while updating storage: {value_error}")
            
        except Exception as exception:
            self.logger.exception(f"Error while fetching the objects . Error {exception}")
            raise exception
        self.local_storage.update_storage(storage_with_collection)

    def start_consumer(self, documents_to_index):
        """This method starts async calls for the consumer which is responsible for indexing documents to the
        Enterprise Search
        :param queue: Shared queue to fetch the stored documents
        """
        logger = self.logger
        sync_es = SyncElasticSearch(self.config, logger, self.elastic_search_custom_client, documents_to_index)
        sync_es.perform_sync()

    def execute(self):
        """This function execute the start function."""
        config = self.config
        logger = self.logger
        current_time = get_current_time()

        checkpoint = Checkpoint(config, logger)

        start_time, end_time = checkpoint.get_checkpoint(current_time, 'panopto')
        time_range = {
            "start_time": start_time,
            "end_time": end_time,
        }
        logger.info(f"Indexing started at: {current_time}")

        documents_to_index = []
        self.start_producer(documents_to_index, time_range)
        self.start_consumer(documents_to_index)
        checkpoint.set_checkpoint(current_time, INDEXING_TYPE, 'panopto')
        logger.info(f"Indexing ended at: {get_current_time()}")
        
