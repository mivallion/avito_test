import datetime
import requests

from pymongo import MongoClient

import time

client = MongoClient('mongodb://root:1234@127.0.0.1:27017/')
db = client['stat_database']
query_col = db['query_col']

avito_app_key = "ZaeC8aidairahqu2Eeb1quee9einaeFieboocohX"


def __get_query_count(query: str, location_id: str or None) -> str or None:
    """
    Get ads count for query

    :param query: Text of query
    :param location_id: Location (city, village etc.) id. None is equal everywhere
    :return: Count of ads for this query params
    """
    if location_id is None:
        r = requests.get(
            f"https://www.avito.ru/api/10/items?countOnly=1&query={query}&key={avito_app_key}"
        ).json()
    else:
        r = requests.get(
            f"https://www.avito.ru/api/10/items?countOnly=1&locationId={location_id}&query={query}&key={avito_app_key}"
        ).json()
    if "error" in r:
        return None
    if r["status"] == "ok":
        return r["result"]["count"]
    else:
        return None


def __update_queries():
    """
    Update queries - add new entry {timestamp: count-of-ads} in database
    """
    queries = query_col.find()
    for query in queries:
        t = str(int(datetime.datetime.now().timestamp()))
        query["counts"][t] = __get_query_count(query["query"], query["locationId"])
        db_filter = {
            "query": query["query"],
            "locationId": query["locationId"]
        }
        db_update = {
            "$set": {
                "counts": query["counts"],
            }
        }
        query_col.update_one(db_filter, db_update)


while True:
    time.sleep(3600)
    __update_queries()
