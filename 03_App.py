from fastapi import FastAPI
from pymongo import MongoClient
from datetime import datetime, timedelta
import re

# =========================================================
# MongoDB connection
# =========================================================

MONGO_URI = (
    "mongodb+srv://kvivek1023_db_user:"
"1Vwy8zwYr8EoGjQc"
"@cluster0.bnksytl.mongodb.net/"
"?appname=Cluster0"
)

client = MongoClient(MONGO_URI)
db = client.shipments_db
collection = db.shipments

# =========================================================
# FastAPI app
# =========================================================

app = FastAPI(title="Shipment Natural Language Query API")

# =========================================================
# Helpers
# =========================================================

def current_month_range():
    now = datetime.now()
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return start, now

def parse_query(query: str):
    q = query.lower()

    if "how many" in q and "month" in q:
        return {"type": "count_month"}

    if "total" in q and "cost" in q and "month" in q:
        return {"type": "sum_month"}

    if "group" in q and "status" in q:
        return {"type": "group_by_status"}

    if "top" in q and "expensive" in q:
        return {"type": "top_expensive", "limit": 5}

    if "last" in q and "7" in q and "day" in q:
        return {"type": "last_7_days"}

    return {"type": "unknown"}

# =========================================================
# Routes
# =========================================================

@app.get("/")
def health():
    return {"status": "ok", "message": "Shipment API running"}

@app.post("/query")
def query_shipments(payload: dict):
    query = payload.get("query", "")
    intent = parse_query(query)

    # -------------------------------
    # How many shipments this month
    # -------------------------------
    if intent["type"] == "count_month":
        start, end = current_month_range()
        count = collection.count_documents({
            "ship_date": {"$gte": start, "$lte": end}
        })
        return {"query": query, "result": count}

    # -------------------------------
    # Total shipment cost this month
    # -------------------------------
    if intent["type"] == "sum_month":
        start, end = current_month_range()
        pipeline = [
            {"$match": {"ship_date": {"$gte": start, "$lte": end}}},
            {"$group": {"_id": None, "total_cost": {"$sum": "$discounted_cost"}}}
        ]
        result = list(collection.aggregate(pipeline))
        return {"query": query, "result": result}

    # -------------------------------
    # Cost grouped by status
    # -------------------------------
    if intent["type"] == "group_by_status":
        pipeline = [
            {
                "$group": {
                    "_id": "$status",
                    "count": {"$sum": 1},
                    "total_cost": {"$sum": "$discounted_cost"}
                }
            }
        ]
        result = list(collection.aggregate(pipeline))
        return {"query": query, "result": result}

    # -------------------------------
    # Top 5 expensive shipments
    # -------------------------------
    if intent["type"] == "top_expensive":
        result = list(
            collection.find({}, {"_id": 0})
            .sort("discounted_cost", -1)
            .limit(intent["limit"])
        )
        return {"query": query, "result": result}

    # -------------------------------
    # Last 7 days shipments
    # -------------------------------
    if intent["type"] == "last_7_days":
        cutoff = datetime.utcnow() - timedelta(days=7)
        result = list(
            collection.find(
                {"ship_date": {"$gte": cutoff}},
                {"_id": 0}
            )
        )
        return {"query": query, "result": result}

    return {
        "query": query,
        "error": "Could not understand query"
    }
