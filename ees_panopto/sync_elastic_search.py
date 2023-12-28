
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

    def index_documents(self, documents, upsert=False):
        self.total_documents_found += len(documents)
        if documents:
            if upsert:
                query = {
                    "query": {
                        "match_all": {}
                    }
                }
                # Fetch all documents using the scan helper
                results = scan(
                    self.elastic_search_client,
                    index=self.source,
                    query=query,
                    size=1000  # Number of documents to retrieve per batch (adjust as needed)
                )

                fetched_documents = {}
                for item in results:
                    item_id = item['_source']['id']
                    fetched_documents[item_id] = {
                        '_op_type': 'update',
                        '_id': item['_id'],
                    }

                documents_to_update = []
                documents_to_insert = []
                for item in documents:
                    item_id = item['id']
                    if item_id in fetched_documents:
                        merged_item = {
                            **fetched_documents[item_id], 
                            'doc': item
                        }  # Merge the dictionaries
                        documents_to_update.append(merged_item)
                    else:
                        documents_to_insert.append(item)
                        
                documents = documents_to_update + documents_to_insert

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
                        self.logger.info(f"Found an end signal in the queue. Closing Thread ID {threading.get_ident()}")
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