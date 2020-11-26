from typing import Optional

from fastapi import FastAPI

from pydantic import BaseModel

from pymongo import MongoClient

import datetime

import requests

from asyncscheduler import AsyncScheduler

import json

app = FastAPI()

client = MongoClient('mongodb://root:1234@127.0.0.1:27017/')
db = client['stat_database']
query_col = db['query_col']

avito_app_key = "ZaeC8aidairahqu2Eeb1quee9einaeFieboocohX"

def get_query_count(query, location_id):
    r = requests.get("https://www.avito.ru/api/10/items?countOnly=1&locationId={location_id}&query={query}&key={avito_app_key}").json()
    print(r)
    if r["status"] == "ok":
        return r["result"]["count"]
    else:
        return None

def update_queries():
    queries = query_col.find()
    for query in queries:
        t = int(datetime.datetime.now().timestamp())
        if "counts" not in query:
            query["counts"] = {}
        query["counts"][t] = get_query_count(query["query"], query["locationId"])

        query_col.update_one({query["query"], query["locationId"]}, {"$set": {"counts": query["counts"]}})

    queries = query_col.find()
    for query in queries:
        print(query)

a = AsyncScheduler()
# a.start()
# a.repeat(3600, 1, update_queries)

class Query(BaseModel):
    text: str
    locationId: Optional[int] = None

@app.get("/update")
async def update():
    update_queries()
    return {}

@app.get("/stop")
async def stop():
    a.stop()

@app.put("/query")
async def add_query(query: Query):
    query_entry = {"query": query.text, "locationId": query.locationId}
    insert_res = query_col.insert_one(query_entry)
    return {"id": str(insert_res.inserted_id)}