import requests
import datetime
import itertools

from pymongo import MongoClient

client = MongoClient('mongodb://root:1234@database:27017/')
db = client['stat_database']
query_col = db['query_col']

avito_app_key = "ZaeC8aidairahqu2Eeb1quee9einaeFieboocohX"


def timestamp() -> str:
    """
    Get current timestamp in seconds

    :return: String with current timestamp
    """
    return str(int(datetime.datetime.now().timestamp()))


def get_query_count(query: str, location_id: str or None) -> str or None:
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


def update_queries():
    """
    Update queries - add new entry {timestamp: count-of-ads} in database
    """
    queries = query_col.find()
    for query in queries:
        t = timestamp()
        query["counts"][t] = get_query_count(query["query"], query["locationId"])
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


def get_top_ads(query: str, location_id: str, count: int) -> {str: str} or None:
    """
    Get top ads by total views count for query

    :param query: Text of query
    :param location_id: Location (city, village etc.) id. None is equal everywhere
    :param count: Count of top ads
    :return: Dictionary {ad_id: views_count}
    """
    r = requests.get(
        f"https://www.avito.ru/api/10/items?locationId={location_id}&query={query}&key={avito_app_key}"
    ).json()
    if "error" in r:
        return None
    if r["status"] == "ok":
        items = r["result"]["items"]
        items_views = {}
        for item in items:
            if item["type"] != "item":
                continue
            item_id = item["value"]["id"]
            item_info = requests.get(
                f"https://www.avito.ru/api/16/items/{item_id}?key={avito_app_key}"
            ).json()
            views = item_info["stats"]["views"]["total"]
            items_views[str(item_id)] = str(views)

        sorted_dict = dict(sorted(items_views.items(), reverse=True, key=lambda x: int(x[1])))
        return dict(itertools.islice(sorted_dict.items(), count if count < len(items_views) else len(items_views)))


def update_top_ads():
    """
    Update top 5 ads for every query in database
    """
    queries = query_col.find()
    for query in queries:
        top_ads = get_top_ads(query['query'], query['locationId'], 5)
        db_filter = {
            "query": query["query"],
            "locationId": query["locationId"]
        }
        db_update = {
            "$set": {
                "top_ads": top_ads,
            }
        }
        query_col.update_one(db_filter, db_update)
