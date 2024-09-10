#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.
#
"""mssql_client allows to call MSSQL and returns a connection object
    that can be used to fetch files from MSSQL.
"""
import pyodbc

from .utils import retry


class MSSQL:
    """Creates an pyodbc connection object to the MSSQL and returns the object
    """
    def __init__(self, config, logger):
        self.logger = logger
        self.user = config.get_value("panopto_db.user")
        self.password = config.get_value("panopto_db.password")
        self.host = config.get_value("panopto_db.host")
        self.database = config.get_value("panopto_db.database")
        # self.port = self.configuration["port"]

        # self.client_machine_name = config.get_value("client_machine.name")
        # self.server_name = config.get_value("network_drive.server_name")
        # self.server_ip = config.get_value("network_drive.server_ip")
        # self.domain = config.get_value("network_drive.domain")
        # self.username = config.get_value("network_drive.username")
        # self.password = config.get_value("network_drive.password")
        self.retry_count = int(config.get_value("retry_count"))

    @retry(exception_list=(pyodbc.Error))
    def connect(self):
        try:
            conn = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server};'
                'SERVER=' + self.host + ';'
                'DATABASE=' + self.database + ';'
                'UID=' + self.user + ';'
                'PWD=' + self.password
            )
            return conn
        except pyodbc.Error as exception:
            raise exception
        except Exception as exception:
            self.logger.exception(f"Unknown error while connecting to MSSQL. Error: {exception}")
            raise exception

    def execute_query(self, conn, query, params=None, fetch_method='fetchall'):
        if not conn:
            raise Exception("Connection is not established.")

        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            if fetch_method == 'fetchone':
                result = cursor.fetchone()
            else:
                result = cursor.fetchall()
            cursor.close()
            return result
        except pyodbc.Error as e:
            self.logger.exception(f"Error executing query: {e}")
            raise e
