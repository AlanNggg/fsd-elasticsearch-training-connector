#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""This module perform operations related to Enterprise Search based on the Enterprise Search version
"""
from elasticsearch.helpers import BulkIndexError, bulk

from elasticsearch import Elasticsearch


class ElasticSearchWrapper:
    """This class contains operations related to Enterprise Search such as index documents, delete documents, etc."""

    def __init__(self, logger, config, args):
        self.logger = logger
        self.host = config.get_value("elasticsearch.host_url")
        self.source = config.get_value("elasticsearch.source")
        self.username = config.get_value("elasticsearch.username")
        self.password = config.get_value("elasticsearch.password")
        self.elastic_search_client = Elasticsearch(self.host, verify_certs=False, ssl_show_warn=False, http_auth=(self.username, self.password))

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

    def index_documents(self, documents):
        """Indexes one or more new documents into a custom content source, or updates one
        or more existing documents
        :param documents: list of documents to be indexed
        :param timeout: Timeout in seconds
        """
        try:
            # raise_on_error: DO NOT raise BulkIndexError
            responses = bulk(self.elastic_search_client, actions=documents, index=self.source, raise_on_error=False)
            self.logger.info('responses')
            self.logger.info(responses)
        # except BulkIndexError as e:
        #     print(f"{len(e.errors)} documents failed to index:")
        #     for err in e.errors:
        #         print(err)
        except Exception as exception:
            self.logger.exception(f"Error while indexing the documents. Error: {exception}")
            raise exception
        finally:
            return responses
