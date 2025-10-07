# backend/api/forecast.py
import numpy as np
from flask import request
from flask_restful import Resource
from datetime import datetime, timedelta
from pymongo import MongoClient

from backend.models.moving_average import MovingAverageModel
from backend.models.arima_model import ARIMAModel
from backend.models.ensemble import combine_predictions

try:
    from backend.models.gru_model import GRUForecaster
    use_gru = True
except ImportError:
    GRUForecaster = None
    use_gru = False


def get_db():
    mongo_uri = "mongodb://localhost:27017"
    db_name = "bitcoin_db"
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    return client[db_name]


class Forecast(Resource):
    def post(self):
        """POST /api/forecast  { "horizon": 24 }"""
        req = request.get_json(force=True)
        horizon = int(req.get("horizon", 24))

        db = get_db()
        coll_hist = db.get_collection("btc_historical")

        print("\n=== DEBUG: Fetching historical data from MongoDB ===")
        data = list(
            coll_hist.find({}, {"_id": 0, "date": 1, "close": 1})
            .sort("date", -1)
            .limit(100)
        )

        total_docs = len(data)
        print(f"DEBUG fetched {total_docs} records from btc_historical")

        if total_docs == 0:
            print("DEBUG: No data found — aborting forecast")
            return {"error": "No historical data available"}, 400

        # --- Extract numeric close prices ---
        cleaned = [
            d.get("close") for d in data
            if "close" in d and isinstance(d.get("close"), (int, float))
        ]

        if not cleaned:
            print("DEBUG: No valid numeric 'close' data found.")
            return {"error": "No valid numeric 'close' data in database"}, 400

        # Reverse to chronological order
        series = np.array(cleaned[::-1])
        print(f"DEBUG: Series length = {len(series)}, last 5 closes = {series[-5:]}")

        # --- Determine last historical date ---
        last_doc = coll_hist.find_one({}, sort=[("date", -1)])
        if last_doc and "date" in last_doc:
            last_date = last_doc["date"]
            if isinstance(last_date, str):
                try:
                    last_date = datetime.fromisoformat(last_date.replace("Z", ""))
                except Exception:
                    last_date = datetime.utcnow()
        else:
            last_date = datetime.utcnow()

        print(f"DEBUG: Last historical date = {last_date}")

        # --- Generate forecast dates (start after last historical record) ---
        forecast_dates = [
            last_date + timedelta(hours=i + 1) for i in range(horizon)
        ]

        # --- Run models ---
        print("DEBUG: Running Moving Average and ARIMA models...")
        ma_model = MovingAverageModel(window=5).fit(series)
        arima_model = ARIMAModel(order=(2, 1, 2)).fit(series)

        ma_pred = ma_model.predict(horizon)
        arima_pred = arima_model.predict(horizon)
        gru_pred = None

        if use_gru:
            print("DEBUG: Running GRU model...")
            try:
                gru_model = GRUForecaster(lookback=10, epochs=5).fit(series)
                gru_pred = gru_model.predict(horizon)
            except Exception as e:
                print(f"⚠️ WARNING: GRU model failed — {e}")
        else:
            print("DEBUG: GRU model not available, skipping.")

        # --- Combine predictions ---
        ensemble_pred = combine_predictions(ma_pred, arima_pred, gru_pred)

        # --- Evaluate (optional metrics) ---
        metrics = {"ma": {}, "arima": {}, "gru": {}, "ensemble": {}}
        if len(series) > horizon:
            true = series[-horizon:]
            metrics["ma"] = ma_model.evaluate(true, ma_pred)
            metrics["arima"] = arima_model.evaluate(true, arima_pred)
            if gru_pred is not None:
                metrics["gru"] = gru_model.evaluate(true, gru_pred)
            metrics["ensemble"] = ma_model.evaluate(true, ensemble_pred)

        # --- Save forecast to MongoDB ---
        forecast_doc = {
            "timestamp": datetime.utcnow(),
            "horizon": horizon,
            "use_gru": use_gru,
            "predictions": {
                "dates": [d.isoformat() for d in forecast_dates],
                "moving_average": ma_pred.tolist(),
                "arima": arima_pred.tolist(),
                "gru": gru_pred.tolist() if gru_pred is not None else None,
                "ensemble": ensemble_pred.tolist(),
            },
            "metrics": metrics,
        }

        db.get_collection("btc_forecasts").insert_one(forecast_doc)
        print("✅ DEBUG: Forecast inserted successfully into btc_forecasts.")

        return {
            "message": "Forecast generated successfully",
            "horizon": horizon,
            "use_gru": use_gru,
            "predictions": forecast_doc["predictions"],
            "metrics": metrics,
        }, 200
