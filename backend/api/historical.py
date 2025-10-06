from flask_restful import Resource
from flask import request
from backend.api.db import get_db
from datetime import datetime

class Historical(Resource):
    def get(self):
        """
        GET /api/historical
        Optional query param: ?limit=15
        Example: /api/historical?limit=20
        """
        limit = request.args.get("limit", default=15, type=int)

        db = get_db()
        coll = db.get_collection("btc_historical")

        cursor = coll.find({}, {"_id": 0}).sort("date", -1).limit(limit)
        data = list(cursor)

        if not data:
            return {"error": "No Bitcoin data found. Please run /api/ingest first."}, 404

        for d in data:
            if isinstance(d.get("date"), datetime):
                d["date"] = d["date"].isoformat()

        return {"ticker": "BTC-USD", "count": len(data), "data": data}, 200
