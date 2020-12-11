import pymongo
import requests
import unittest
from unittest import mock

import common


def mocked_mongo_update_one(*args, **kwargs):
    pass


def mocked_mongo_find_timeout(*args, **kwargs):
    raise pymongo.errors.ServerSelectionTimeoutError


def mocked_mongo_find(*args, **kwargs):
    return [
        {

            "query": "GetTopAds",
            "locationId": "637640",
            "counts": {
                "0": "1",
            }
        }
    ]


# This method will be used by the mock to replace requests.get
def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    loc_id = 637640
    query = "CorrectText"
    if args[0] == f"https://www.avito.ru/api/10/items?countOnly=1&locationId={loc_id}&query={query}&key" \
                  f"={common.avito_app_key}":
        return MockResponse({
            "status": "ok",
            "result": {
                "count": 12691,
                "totalCount": 12691
            }
        }, 200)
    elif args[0] == f"https://www.avito.ru/api/10/items?countOnly=1&locationId={loc_id}&query=GetTopAds&key" \
                    f"={common.avito_app_key}":
        return MockResponse({
            "status": "ok",
            "result": {
                "count": 1,
                "totalCount": 1
            }
        }, 200)
    elif args[0] == f"https://www.avito.ru/api/10/items?countOnly=1&query={query}&key={common.avito_app_key}":
        return MockResponse({
            "status": "ok",
            "result": {
                "count": 87792,
                "totalCount": 87792
            }
        }, 200)
    elif args[0] == f"https://www.avito.ru/api/10/items?countOnly=1&query=WrongKey&key={common.avito_app_key}":
        return MockResponse({
            "error": {
                "code": 403,
                "message": "Неизвестный ключ"
            }
        }, 433)
    elif args[0] == f"https://www.avito.ru/api/10/items?countOnly=1&query=TooManyRequests&key={common.avito_app_key}":
        return MockResponse({
            "error": {
                "code": 403,
                "message": "Неизвестный ключ"
            }
        }, 429)
    elif args[0] == f"https://www.avito.ru/api/10/items?countOnly=1&locationId=123456&query={query}" \
                    f"&key={common.avito_app_key}":
        return MockResponse({
            "status": "incorrect-data",
            "result": {
                "messages": {}
            }
        }, 200)
    elif args[0] == f"https://www.avito.ru/api/10/items?locationId={loc_id}&query=GetTopAds" \
                    f"&key={common.avito_app_key}":
        return MockResponse({
            "status": "ok",
            "result": {
                "count": 12691,
                "totalCount": 12691,
                "items": [
                    {
                        "type": "not_item",
                        "value": {"id": 1234},
                        "stats": {
                            "views": {
                                "total": 123
                            }
                        }
                    },
                    {
                        "type": "item",
                        "value": {"id": 2026965892},
                    }
                ]
            }
        }, 200)
    elif args[0] == f"https://www.avito.ru/api/16/items/2026965892?key={common.avito_app_key}":
        return MockResponse({
            "stats": {
                "views": {
                    "total": 124
                }
            }
        }, 200)

    return MockResponse(None, 404)


# Our test case class
class TestClass(unittest.TestCase):

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_get_query_count(self, mock_get):
        data = common.get_query_count("CorrectText", 637640)
        self.assertEqual(data, 12691)

        data = common.get_query_count("CorrectText", None)
        self.assertEqual(data, 87792)

        data = common.get_query_count("TooManyRequests", None)
        self.assertIsNone(data)

        data = common.get_query_count("CorrectText", 123456)
        self.assertIsNone(data)

        data = common.get_query_count("WrongKey", None)
        self.assertIsNone(data)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_get_top_ads(self, mock_get):
        data = common.get_top_ads("GetTopAds", 637640, 5)
        self.assertEqual(data, {"2026965892": "124"})

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    @mock.patch("pymongo.collection.Collection.find", side_effect=mocked_mongo_find)
    @mock.patch("pymongo.collection.Collection.update_one", side_effect=mocked_mongo_update_one)
    def test_update_mongo(self, mock_get, mock_find, mock_update_one):
        common.update_queries()
        common.update_top_ads()

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    @mock.patch("pymongo.collection.Collection.find", side_effect=mocked_mongo_find_timeout)
    @mock.patch("pymongo.collection.Collection.update_one", side_effect=mocked_mongo_update_one)
    def test_update_mongo_no_connection(self, mock_get, mock_find, mock_update_one):
        common.update_queries()
        common.update_top_ads()


if __name__ == '__main__':
    unittest.main()
