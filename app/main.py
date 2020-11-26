from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel

from pymongo import MongoClient

import datetime

app = FastAPI()

client = MongoClient('mongodb://root:1234@127.0.0.1:27017/')
db = client['stat_database']
query_col = db['query_col']

class Item(BaseModel):
    name: str
    price: float
    is_offer: Optional[bool] = None

class Query(BaseModel):
    text: str
    locationId: Optional[int] = None

@app.put("/query")
async def add_query(query: Query):
    query_entry = {"query": query.text, "locationId": query.locationId}
    insert_res = query_col.insert_one(query_entry)
    return {"id": str(insert_res.inserted_id)}