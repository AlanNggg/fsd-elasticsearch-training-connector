import json

import pymysql
import pymysql.cursors


class FsdSearchPortalClient:
    def __init__(self, host, database, username, password):
        self.host = host
        self.database = database
        self.username = username
        self.password = password

    def connect(self):
        return pymysql.connect(
            host=self.host,
            user=self.username,
            password=self.password,
            database=self.database,
            cursorclass=pymysql.cursors.DictCursor
        )

    def get_custom_ocr_configure(self, source):
        try:
            connection = self.connect()

            with connection:
                with connection.cursor() as cursor:
                    sql = "SELECT path, language FROM `ocr_path_setting` WHERE `source`=%s"
                    cursor.execute(sql, (source,))
                    result = cursor.fetchall()
                    paths = list(map(lambda x: x, result))

            return paths
        except Exception as exception:
            return []

    def get_categories(self):
        try:
            connection = self.connect()

            with connection:
                with connection.cursor() as cursor:
                    sql = "SELECT value, type FROM `extension`"
                    cursor.execute(sql)
                    results = cursor.fetchall()

                    categories = {}
                    for result in results:
                        value = result['value']
                        type_ = json.loads(result['type'])
                        type_ = [list(item.keys())[0] for item in type_]
                        categories[value] = type_

            return categories
        except Exception as exception:
            return {}

    def get_click_count(self, url):
        try:
            connection = self.connect()

            with connection:
                with connection.cursor() as cursor:
                    sql = "SELECT COUNT(*) AS click_count FROM click_log WHERE url = %s"
                    cursor.execute(sql, (url,))
                    result = cursor.fetchone()['click_count']

            return result
        except Exception as exception:
            return 0
