#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""This module contains un-categorized utility methods.
"""
import csv
import hashlib
import os
import sys
import tempfile
import time
import urllib.parse
import uuid
from datetime import datetime
from urllib.parse import urlparse

from pdf2image import convert_from_bytes, convert_from_path
from tika import parser

from .constant import RFC_3339_DATETIME_FORMAT


def is_website_url(url):
    """Check if the given URL is a website link."""
    parsed_url = urlparse(url)
    return parsed_url.scheme in ['http', 'https'] and parsed_url.netloc != ''


def run_tika(path, content):
    file_extension = os.path.splitext(path)[1].lower()
    extracted_text = ''
    if file_extension == '.pdf':
        with tempfile.TemporaryDirectory() as temp_dir:
            images = None
            try: 
                images = convert_from_bytes(content, output_folder=temp_dir, fmt="jpeg", poppler_path=r"C:\poppler-23.08.0\Library\bin")

                # Extract text from each image
                for image in images:     
                    print(image.filename)
                    text = extract_text_from_file(image.filename)

                    if text:
                        extracted_text += text

                for image in images:
                    image.close()  # Close the image once we are done with it
            except Exception as exception:
                raise exception
            finally:
                if images:
                    for image in images:
                        image.close()
    else:
        extracted_text = extract(content)
    return extracted_text

def extract(content):
    """Extracts the contents
    :param content: content to be extracted
    Returns:
        parsed_test: parsed text
    """
    parsed = parser.from_buffer(content, 'http://localhost:9998/', requestOptions={'timeout': 300})
    parsed_text = parsed["content"]
    return parsed_text


def extract_text_from_file(path):
    # Use Apache Tika to extract text from the image
    parsed = parser.from_file(path, 'http://localhost:9998/', requestOptions={'timeout': 300})
    parsed_text = parsed["content"]
    return parsed_text

def url_encode(object_name):
    """Performs encoding on the name of objects
    containing special characters in their url, and
    replaces single quote with two single quote since quote
    is treated as an escape character in odata
    :param object_name: name that contains special characters
    """
    name = urllib.parse.quote(object_name, safe="'")
    return name.replace("'", "''")


def hash_id(file_path):
    """Hashes the file_name and path to create file id if file id
    is not present
    :param file_name: name of the file in the Network Drives
    :param file_path: path of the file in the Network Drives
    :Returns: hashed file id
    """
    encoded_path = file_path.encode('utf-8')
    return hashlib.sha256(encoded_path).hexdigest()


def retry(exception_list):
    """Decorator for retrying in case of network exceptions.
    Retries the wrapped method `times` times if the exceptions listed
    in ``exceptions`` are thrown
    :param exception_list: Lists of exceptions on which the connector should retry
    """

    def decorator(func):
        """This function used as a decorator."""

        def execute(self, *args, **kwargs):
            """This function execute the retry logic."""
            retry = 1
            while retry <= self.retry_count:
                try:
                    return func(self, *args, **kwargs)
                except exception_list as exception:
                    self.logger.exception(
                        f"Error while creating a connection. Retry count: {retry} out of {self.retry_count}. \
                            Error: {exception}"
                    )
                    time.sleep(2 ** retry)
                    retry += 1

        return execute

    return decorator


def fetch_users_from_csv_file(user_mapping, logger):
    """This method is used to map sid to username from csv file.
    :param user_mapping: path to csv file containing network drives to enterprise search mapping
    :param logger: logger object
    :returns: dictionary of sid and username
    """
    rows = {}
    if (
        user_mapping and os.path.exists(user_mapping) and os.path.getsize(user_mapping) > 0
    ):
        with open(user_mapping, encoding="utf-8") as mapping_file:
            try:
                csvreader = csv.reader(mapping_file)
                for row in csvreader:
                    rows[row[0]] = row[1]
            except csv.Error as e:
                logger.exception(
                    f"Error while reading user mapping file at the location: {user_mapping}. Error: {e}"
                )
    return rows


def split_list_into_buckets(documents, total_buckets):
    """Divide large number of documents amongst the total buckets
    :param documents: list to be partitioned
    :param total_buckets: number of buckets to be formed
    """
    if documents:
        groups = min(total_buckets, len(documents))
        group_list = []
        for i in range(groups):
            group_list.append(documents[i::groups])
        return group_list
    else:
        return []


def split_documents_into_equal_chunks(documents, chunk_size):
    """This method splits a list or dictionary into equal chunks size
    :param documents: List or Dictionary to be partitioned into chunks
    :param chunk_size: Maximum size of a chunk
    Returns:
        list_of_chunks: List containing the chunks
    """
    list_of_chunks = []
    for i in range(0, len(documents), chunk_size):
        if type(documents) is dict:
            partitioned_chunk = list(documents.items())[i: i + chunk_size]
            list_of_chunks.append(dict(partitioned_chunk))
        else:
            list_of_chunks.append(documents[i: i + chunk_size])
    return list_of_chunks



def split_date_range_into_chunks(start_time, end_time, number_of_threads):
    """Divides the timerange in equal partitions by number of threads
    :param start_time: start time of the interval
    :param end_time: end time of the interval
    :param number_of_threads: number of threads defined by user in config file
    """
    start_time = datetime.strptime(start_time, RFC_3339_DATETIME_FORMAT)
    end_time = datetime.strptime(end_time, RFC_3339_DATETIME_FORMAT)

    diff = (end_time - start_time) / number_of_threads
    datelist = []
    for idx in range(number_of_threads):
        date_time = start_time + diff * idx
        datelist.append(date_time.strftime(RFC_3339_DATETIME_FORMAT))
    formatted_end_time = end_time.strftime(RFC_3339_DATETIME_FORMAT)
    datelist.append(formatted_end_time)
    return datelist

def get_current_time():
    """Returns current time in rfc 3339 format"""
    return (datetime.utcnow()).strftime(RFC_3339_DATETIME_FORMAT)
