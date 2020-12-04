import datetime
import requests
import asyncscheduler
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

# TODO: top 5 for every query_id
# TODO: type hints
# TODO: documentation
# TODO: tests coverage 70+%
# TODO: python app to docker


class Query(BaseModel):
    """
    Query model for PUT body

    Attributes:
        text (str): text of query
        locationId (int): Location (city, village etc.) id. None is equal everywhere
    """
    text: str
    locationId: Optional[str] = None


def __get_query_count(query: str, location_id: str) -> str or None:
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


def __update_top_ads(query: str, location_id: str) -> str or None:
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
            items_views[item_id] = views

        sorted_dict = dict(sorted(items_views.items(), key=lambda x: x[1]))
        print(dict(itertools.islice(sorted_dict.items(), 5)))



a = asyncscheduler.AsyncScheduler()
a.start()
a.repeat(3600, 1, __update_queries)  # TODO: how to stop it with ctrl+c?


@app.put("/add")
async def add_query(query: Query):
    q = query_col.find_one({
        "query": query.text,
        "locationId": query.locationId,
    })
    if q is not None:
        raise HTTPException(status_code=422, detail="Item already added")
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


@app.get("/top")
async def add_query(query: Query):
    __update_top_ads(query.text, query.locationId)
    return {}


@app.get("/stop")
async def stop_updates():
    a.stop()
    return {}
