
import threading

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from .checkpointing import Checkpoint
from .utils import split_documents_into_equal_chunks

BATCH_SIZE = 100
CONNECTION_TIMEOUT = 1000

class SyncElasticSearch:
    def __init__(self, config, logger, elastic_search_custom_client, documents_to_index):
        self.config = config
        self.logger = logger
        self.elastic_search_custom_client = elastic_search_custom_client
        self.documents_to_index = documents_to_index
        self.total_documents_indexed = 0
        self.total_documents_found = 0
        
    def index_documents(self, documents):
        self.total_documents_found += len(documents)
        if documents:
            values = self.elastic_search_custom_client.index_documents(
                documents=documents,
                timeout=CONNECTION_TIMEOUT,
            )
            if values:
                documents_indexed, errors = values

                if errors:
                    for error in errors:
                        self.logger.error(
                            "Error while indexing. Error: %s"
                            % (error)
                        )
                self.logger.info(
                    f"[{threading.get_ident()}] Successfully indexed {documents_indexed} documents to the workplace"
                )
                self.total_documents_indexed += documents_indexed
            else:
                self.logger.error(
                    f"[{threading.get_ident()}] Failed to indexed documents to the workplace"
                )

    def perform_sync(self):
        try:
            self.index_documents(self.documents_to_index)
        except Exception as exception:
            self.logger.error(exception)
        self.logger.info(f"Total {self.total_documents_indexed} documents indexed out of: {self.total_documents_found}")
