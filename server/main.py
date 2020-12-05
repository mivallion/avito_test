import datetime
import requests
import itertools

from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient
from bson.objectid import ObjectId


app = FastAPI()

client = MongoClient('mongodb://root:1234@127.0.0.1:27017/')
db = client['stat_database']
query_col = db['query_col']

avito_app_key = "ZaeC8aidairahqu2Eeb1quee9einaeFieboocohX"

# TODO: tests coverage 70+%
# TODO: python server to docker


class Query(BaseModel):
    """
    Query model

    Attributes:
        text (str): Text of query
        locationId (int): Location (city, village etc.) id. None is equal everywhere
    """
    text: str
    locationId: Optional[str] = None


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


def __get_top_ads(query: str, location_id: str, count: int) -> {str: str} or None:
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
        return dict(itertools.islice(sorted_dict.items(), count))


@app.put("/add")
async def add_query(query: Query):
    """
    Add query in database

    :param query: Query with text and optional locationId to store in database
    :return: Id of inserted entry
    """
    q = query_col.find_one({
        "query": query.text,
        "locationId": query.locationId,
    })
    if q is not None:
        raise HTTPException(status_code=422, detail=f"Item already added: id = {q['_id']}")
    query_count = __get_query_count(query.text, query.locationId)
    if query_count is None:
        raise HTTPException(status_code=422, detail="Bad params")
    timestamp = int(datetime.datetime.now().timestamp())
    query_entry = {
        "query": query.text,
        "locationId": query.locationId,
        "counts": {
            str(timestamp): query_count
        }
    }
    insert_res = query_col.insert_one(query_entry)
    return {"id": str(insert_res.inserted_id)}


@app.get("/stat")
async def get_stat(query_id: str, timestamp_l: str, timestamp_r: str):
    """
    Get stats for query stored in database between two timestamps

    :param query_id: Id of query
    :param timestamp_l: Left timestamp
    :param timestamp_r: Right timestamp
    :return: Dictionary {timestamp: count_of_ads}
    """
    if timestamp_r < timestamp_l:
        timestamp_l, timestamp_r = timestamp_r, timestamp_l
    query = query_col.find_one({"_id": ObjectId(query_id)})
    if query is None:
        raise HTTPException(status_code=404, detail="Item not found")
    res = {}
    for t, c in query["counts"].items():
        if timestamp_l <= t <= timestamp_r:
            res[t] = c
    return res


@app.get("/top/{query_id}")
async def get_top_query(query_id: str):
    """
    Get top 5 ads (by total views count) for query if they stored in database

    :param query_id: Id of query
    :return: Dictionary {ad_id: views_count} or empty dictionary if top ads isn't in database
    """
    query = query_col.find_one({"_id": ObjectId(query_id)})
    if query is None:
        raise HTTPException(status_code=404, detail="Item not found")
    if "top_ads" not in query:
        return {}
    return query["top_ads"]


@app.put("/top/{query_id}")
async def update_top_query(query_id: str):
    """
    Update top 5 ads (by total views count) for query

    :param query_id: Id of query
    :return: Dictionary {ad_id: views_count}
    """
    query = query_col.find_one({"_id": ObjectId(query_id)})
    if query is None:
        raise HTTPException(status_code=404, detail="Item not found")
    top_ads = __get_top_ads(query['query'], query['locationId'], 5)
    if top_ads is None:
        raise HTTPException(status_code=422, detail="Bad params")
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

    return top_ads
