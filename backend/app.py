# backend/app.py
from flask import Flask, request
from flask_restful import Api
from backend.api.ping import Ping
from backend.api.historical import Historical
from backend.api.ingest import ingest_to_mongo

from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})
api = Api(app)

# --- ROUTES ---
api.add_resource(Ping, "/api/ping")
api.add_resource(Historical, "/api/historical")

# --- Ingestion Endpoint ---
@app.route("/api/ingest", methods=["POST"])
def trigger_ingestion():
    """
    POST /api/ingest
    Body: { "start": "YYYY-MM-DD", "end": "YYYY-MM-DD" }
    """
    data = request.get_json(force=True)
    start = data.get("start")
    end = data.get("end")
    if not (start and end):
        return {"error": "Missing 'start' or 'end' date"}, 400
    try:
        inserted = ingest_to_mongo(
            ticker="BTC-USD",
            start=start,
            end=end,
            out_csv=None
        )
        return {"status": "success", "inserted": inserted}, 200
    except Exception as e:
        return {"status": "error", "message": str(e)}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
