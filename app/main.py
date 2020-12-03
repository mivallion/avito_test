import datetime
import requests
import asyncscheduler

from typing import Optional
from fastapi import FastAPI
from pydantic import BaseModel
from pymongo import MongoClient
from bson.objectid import ObjectId


app = FastAPI()

client = MongoClient('mongodb://root:1234@127.0.0.1:27017/')
db = client['stat_database']
query_col = db['query_col']

avito_app_key = "ZaeC8aidairahqu2Eeb1quee9einaeFieboocohX"


class Query(BaseModel):
    text: str
    locationId: Optional[int] = None


def get_query_count(query, location_id):
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
    queries = query_col.find()
    for query in queries:
        t = str(int(datetime.datetime.now().timestamp()))
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


# timer = Timer(30, update_queries())
a = asyncscheduler.AsyncScheduler()
a.start()
a.repeat(3600, 1, update_queries)  # TODO: how to stop it with ctrl+c?


@app.put("/add")
async def add_query(query: Query):
    q = query_col.find_one({
        "query": query.text,
        "locationId": query.locationId,
    })
    if q is not None:
        return {}  # TODO: return HTTP error: already exist
    query_count = get_query_count(query.text, query.locationId)
    if query_count is None:
        return {}  # TODO: return HTTP error: bad args
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
        return {}  # TODO: return HTTP error: entry not exist
    res = {}
    for t, c in query["counts"].items():
        if timestamp_l <= t <= timestamp_r:
            res[t] = c
    return res


@app.get("/stop")
async def stop_updates():
    a.stop()
    return {}
