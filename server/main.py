from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from bson.objectid import ObjectId

from common import *

app = FastAPI()


class Query(BaseModel):
    """
    Query model

    Attributes:
        text (str): Text of query
        locationId (int): Location (city, village etc.) id. None is equal everywhere
    """
    text: str
    locationId: Optional[str] = None


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
    query_count = get_query_count(query.text, query.locationId)
    if query_count is None:
        raise HTTPException(status_code=422, detail="Bad params")
    ts = timestamp()
    top_ads = get_top_ads(query.text, query.locationId, 5)
    query_entry = {
        "query": query.text,
        "locationId": query.locationId,
        "counts": {
            ts: query_count
        },
        "top_ads": top_ads,
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
