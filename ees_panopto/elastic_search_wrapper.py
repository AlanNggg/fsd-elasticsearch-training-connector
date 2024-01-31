#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""This module perform operations related to Enterprise Search based on the Enterprise Search version
"""
from elasticsearch import Elasticsearch
from elasticsearch.helpers import BulkIndexError, bulk, scan, streaming_bulk


class ElasticSearchWrapper:
    """This class contains operations related to Enterprise Search such as index documents, delete documents, etc."""

    def __init__(self, logger, config, args):
        self.logger = logger
        self.host = config.get_value("elasticsearch.host_url")

        if not args.source:
            self.source = config.get_value("elasticsearch.source")
        else:
            self.source = args.source

        self.username = config.get_value("elasticsearch.username")
        self.password = config.get_value("elasticsearch.password")
        self.elastic_search_client = Elasticsearch(
            self.host,
            verify_certs=False,
            ssl_show_warn=False,
            http_auth=(self.username, self.password),
            # sniff_on_start=True,  # sniff before doing anything
            sniff_on_connection_fail=True,  # refresh nodes after a node fails to respond
            sniffer_timeout=60,  # and also every 60 seconds
            retry_on_timeout=True,
        )
        self.retry_count = int(config.get_value("retry_count"))

    def add_permissions(self, user_name, permission_list):
        raise Exception("Not Implemented")

    def list_permissions(self):
        raise Exception("Not Implemented")

    def remove_permissions(self, permission):
        raise Exception("Not Implemented")

    def create_content_source(self, schema, display, name, is_searchable):
        raise Exception("Not Implemented")

    def delete_documents(self, document_ids):
        raise Exception("Not Implemented")

    def get_all_documents(self):
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
            # Number of documents to retrieve per batch (adjust as needed)
            size=1000
        )

        return results

    def index_documents_incremental(self, documents, timeout):
        try:
            total_documents_appended = 0
            total_documents_updated = 0
            errors = []

            # Fetch all documents using the scan helper
            results = self.get_all_documents()

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

            for ok, item in streaming_bulk(
                self.elastic_search_client,
                actions=documents,
                index=self.source,
                max_retries=self.retry_count,
                request_timeout=timeout,
                raise_on_error=False,
                yield_ok=True,
                ignore_status=(),
            ):
                operation_type = list(item.keys())[0]

                if not ok:
                    errors.append(item)
                else:
                    if operation_type == 'index':
                        total_documents_appended += 1
                    elif operation_type == 'update':
                        total_documents_updated += 1

            return total_documents_appended, total_documents_updated, errors
        except Exception as exception:
            self.logger.error(
                f"Error while indexing the documents. Error: {exception}")
        return None

    def index_documents(self, documents, timeout):
        """Indexes one or more new documents into a custom content source, or updates one
        or more existing documents
        :param documents: list of documents to be indexed
        :param timeout: Timeout in seconds
        """
        try:
            # raise_on_error: DO NOT raise BulkIndexError
            responses = bulk(
                self.elastic_search_client,
                actions=documents,
                index=self.source,
                max_retries=self.retry_count,
                request_timeout=timeout,
                raise_on_error=False)
            self.logger.info(responses)
            return responses
        # except BulkIndexError as e:
        #     self.logger.exception(f"{len(e.errors)} documents failed to index:")
            # for err in e.errors:
            #     print(err)
        except Exception as exception:
            self.logger.error(
                f"Error while indexing the documents. Error: {exception}")
        return None
