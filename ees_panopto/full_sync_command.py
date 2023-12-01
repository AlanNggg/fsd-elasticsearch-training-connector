#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""This module allows to run a full sync against a Network Drives.

    It will attempt to sync absolutely all documents that are available in the
    third-party system and ingest them into Enterprise Search instance.
"""
from datetime import datetime

from .base_command import BaseCommand
from .checkpointing import Checkpoint
from .connector_queue import ConnectorQueue
from .local_storage import LocalStorage
from .sync_elastic_search import SyncElasticSearch
from .sync_enterprise_search import SyncEnterpriseSearch
from .sync_panopto import SyncPanopto
from .utils import get_current_time, split_date_range_into_chunks

INDEXING_TYPE = "full"


class FullSyncCommand(BaseCommand):
    """This class start executions of fullsync feature."""

    def start_producer(self, documents_to_index):
        """This method starts async calls for the producer which is responsible
        for fetching documents from the Network Drive and pushing them in the shared queue
        :param queue: Shared queue to store the fetched documents
        :param time_range: Time range dictionary storing start time and end time
        """
        self.logger.debug("Starting the full indexing..")

        try:
            sync_panopto = SyncPanopto(
                self.config,
                self.logger,
                self.mssql_client,
                self.indexing_rules,
                documents_to_index,
                self.leadtools_engine,
                self.panopto_client
            )
            storage_with_collection = self.local_storage.get_storage_with_collection()
            global_keys = sync_panopto.perform_sync()

            try:
                storage_with_collection["global_keys"]["videos"].update(global_keys)
            except ValueError as value_error:
                self.logger.error(f"Exception while updating storage: {value_error}")


        except Exception as exception:
            self.logger.error("Error while Fetching from Panopto. Checkpoint not saved")
            raise exception

        self.local_storage.update_storage(storage_with_collection)

    def start_consumer(self, documents_to_index):
        """This method starts async calls for the consumer which is responsible for indexing documents to the Enterprise Search
        :param queue: Shared queue to fetch the stored documents
        """
        logger = self.logger
        sync_es = SyncElasticSearch(self.config, logger, self.elastic_search_custom_client, documents_to_index)
        sync_es.perform_sync()

    def execute(self):
        """This function execute the full sync."""
        config = self.config
        logger = self.logger
        current_time = get_current_time()
        checkpoint = Checkpoint(config, logger)

        logger.info(f"Indexing started at: {current_time}")

        documents_to_index = []
        self.start_producer(documents_to_index)
        self.start_consumer(documents_to_index)
        checkpoint.set_checkpoint(current_time, INDEXING_TYPE, 'panopto')
        logger.info(f"Indexing ended at: {get_current_time()}")
