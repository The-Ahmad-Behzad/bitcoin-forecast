import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import os
import pandas as pd
from datetime import datetime
import pytest

from backend.api import ingest as ingest_module

def make_sample_df():
    # create 3 rows with required columns
    dates = pd.date_range("2025-09-01", periods=3, freq="D")
    df = pd.DataFrame({
        "Date": dates,
        "Open": [100.0, 101.0, 102.5],
        "High": [101.0, 102.0, 103.5],
        "Low": [99.0, 100.5, 101.0],
        "Close": [100.5, 101.8, 102.9],
        "Volume": [1000, 1100, 1050],
        "Return": [0.0, 0.013, 0.010],
        "MA5": [100.0, 100.4, 100.8],
        "MA10": [99.5, 99.8, 100.1],
        "Volatility": [0.01, 0.011, 0.012],
        "headlines_concat": ["h1", "h2", "h3"],
        "news_sentiment": [0.0, 0.1, -0.05],
        "headline_count": [1, 2, 0]
    })
    return df

def test_validate_dataframe_pass():
    df = make_sample_df()
    valid, missing = ingest_module.validate_dataframe(df)
    assert valid is True
    assert missing == []

def test_ingest_to_mongo_monkeypatch(monkeypatch):
    df = make_sample_df()
    # monkeypatch build_dataset to return our df
    monkeypatch.setattr(ingest_module, "build_dataset", lambda **kwargs: df)

    # use a test DB name to avoid clobbering real data
    test_db = "test_bitcoin_db"
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")

    # run ingestion
    inserted = ingest_module.ingest_to_mongo(
        ticker="BTC-USD",
        start="2025-09-01",
        end="2025-09-03",
        out_csv=None,
        mongo_uri=mongo_uri,
        db_name=test_db
    )
    # inserted should be >= 1 (upsert counted as inserted). Since DB was fresh, expect 3.
    assert inserted == 3

    # verify documents present
    from pymongo import MongoClient
    client = MongoClient(mongo_uri)
    coll = client.get_database(test_db)[ingest_module.HIST_COLLECTION]
    count = coll.count_documents({})
    assert count >= 3

    # cleanup test db
    client.drop_database(test_db)
