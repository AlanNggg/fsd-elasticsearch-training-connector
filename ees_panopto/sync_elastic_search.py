
import threading

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, scan

from .checkpointing import Checkpoint
from .utils import split_documents_into_equal_chunks

BATCH_SIZE = 100
CONNECTION_TIMEOUT = 1000


class SyncElasticSearch:

    def __init__(self, config, logger, elastic_search_custom_client, queue):
        self.config = config
        self.logger = logger
        self.elastic_search_custom_client = elastic_search_custom_client
        self.queue = queue
        self.total_documents_indexed = 0
        self.total_documents_found = 0
        self.total_documents_failed = 0

        self.total_documents_appended = 0
        self.total_documents_updated = 0

    def index_documents(self, documents, upsert=False):
        if documents:
            self.total_documents_found += len(documents)

            if upsert:
                values = self.elastic_search_custom_client.index_documents_incremental(
                    documents=documents,
                    timeout=CONNECTION_TIMEOUT,
                )
                if values:
                    documents_appended, documents_updated, errors = values

                    documents_indexed = documents_appended + documents_updated

                    self.total_documents_indexed += documents_indexed
                    self.total_documents_appended += documents_appended
                    self.total_documents_updated += documents_updated

                    if errors:
                        self.total_documents_failed += len(errors)

                        for error in errors:
                            self.logger.error(
                                "Error while indexing. Error: %s"
                                % (error)
                            )
                    self.logger.info(
                        f"[{threading.get_ident()}] Successfully indexed {documents_indexed} documents to the workplace"
                    )
                else:
                    self.logger.error(
                        f"[{threading.get_ident()}] Failed to index documents to the workplace"
                    )
            else:
                values = self.elastic_search_custom_client.index_documents(
                    documents=documents,
                    timeout=CONNECTION_TIMEOUT,
                )
                if values:
                    documents_indexed, errors = values

                    self.total_documents_indexed += documents_indexed

                    if errors:
                        self.total_documents_failed += len(errors)

                        for error in errors:
                            self.logger.error(
                                "Error while indexing. Error: %s"
                                % (error)
                            )
                    self.logger.info(
                        f"[{threading.get_ident()}] Successfully indexed {documents_indexed} documents to the workplace"
                    )
                else:
                    self.logger.error(
                        f"[{threading.get_ident()}] Failed to index documents to the workplace"
                    )

    def perform_sync(self, upsert=False):
        try:
            signal_open = True

            self.logger.info(f"Thread ID: {threading.get_ident()} Total {self.total_documents_indexed} documents \
            indexed out of: {self.total_documents_found} till now..")
            while signal_open:
                documents_to_index = []
                while len(documents_to_index) < BATCH_SIZE:
                    document = self.queue.get()
                    if document.get("type") == "signal_close":
                        self.logger.info(
                            f"Found an end signal in the queue. Closing Thread ID {threading.get_ident()}")
                        signal_open = False
                        break
                    else:
                        documents_to_index.extend(document.get("data"))
                # This loop is to ensure if the last document fetched from the queue exceeds the size of
                # documents_to_index to more than the permitted chunk size, then we split the documents as per the limit
                for document_list in split_documents_into_equal_chunks(documents_to_index, BATCH_SIZE):
                    self.index_documents(document_list, upsert)
        except Exception as exception:
            self.logger.error(exception)
        self.logger.info(f"Thread ID: {threading.get_ident()} Total {self.total_documents_indexed} documents \
            indexed out of: {self.total_documents_found} till now..")

    def get_status(self):
        return self.total_documents_found, self.total_documents_indexed, self.total_documents_appended, self.total_documents_updated, self.total_documents_failed
