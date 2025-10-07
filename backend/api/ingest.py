# # backend/api/ingest.py
# import argparse
# import logging
# import os
# import sys
# from typing import Tuple, List

# import pandas as pd
# from pymongo import MongoClient, errors, UpdateOne

# # Import the builder from backend.data (the IP file you provided).
# # Ensure you run this script from the project root so Python path resolves backend as a package:
# #   python backend/api/ingest.py ...
# try:
#     from backend.data.fin_data_builder import build_dataset
# except Exception as e:
#     # helpful error if import fails
#     raise ImportError(
#         "Could not import build_dataset from backend.data.fin_data_builder. "
#         "Make sure backend/data/fin_data_builder.py exists and you run this script from project root."
#     ) from e

# # Required schema (as specified in the IP)
# REQUIRED_COLUMNS = [
#     "Date", "Open", "High", "Low", "Close", "Volume",
#     "Return", "MA5", "MA10", "Volatility",
#     "headlines_concat", "news_sentiment", "headline_count"
# ]

# DEFAULT_MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
# DEFAULT_DB_NAME = os.environ.get("MONGO_DB", "bitcoin_db")
# HIST_COLLECTION = "btc_historical"

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )
# logger = logging.getLogger("ingest")


# def validate_dataframe(df: pd.DataFrame, required_cols: List[str] = REQUIRED_COLUMNS) -> Tuple[bool, List[str]]:
#     """Check that required columns exist in dataframe. Return (valid, missing_cols)."""
#     missing = [c for c in required_cols if c not in df.columns]
#     return (len(missing) == 0, missing)


# def normalize_records_for_mongo(df: pd.DataFrame) -> List[dict]:
#     """
#     Convert DataFrame rows into dicts suitable for MongoDB insertion.
#     - Ensures Date is a native python datetime object.
#     - Converts column names to lowercase except Date -> date to match db naming.
#     """
#     df_copy = df.copy()
#     # Ensure Date parsed
#     df_copy["Date"] = pd.to_datetime(df_copy["Date"])
#     records = []
#     for _, row in df_copy.iterrows():
#         rec = {}
#         for col, val in row.items():
#             if col == "Date":
#                 rec["date"] = pd.Timestamp(val).to_pydatetime()
#             else:
#                 rec[col.lower()] = None if pd.isna(val) else (float(val) if pd.api.types.is_numeric_dtype(type(val)) else val)
#         records.append(rec)
#     return records


# def ingest_to_mongo(ticker: str, start: str, end: str, out_csv: str = None, mongo_uri: str = DEFAULT_MONGO_URI, db_name: str = DEFAULT_DB_NAME):
#     """
#     1) Build dataset via build_dataset(...)
#     2) Validate dataset schema
#     3) Insert records into MongoDB collection `btc_historical`
#     4) Optionally save CSV to out_csv path
#     """
#     logger.info("Starting ingestion for %s from %s to %s", ticker, start, end)

#     # 1) Build dataset
#     try:
#         # The build_dataset function in the IP returns a pandas DataFrame (per IP sample)
#         df = build_dataset(exchange="CRYPTO", ticker=ticker, start=start, end=end, out_path=None)
#     except TypeError:
#         # some versions of the builder expect out_path positional - fallback try
#         df = build_dataset("CRYPTO", ticker, start, end, None)
#     except Exception as e:
#         logger.exception("Error while running build_dataset: %s", e)
#         raise

#     if df is None or len(df) == 0:
#         raise RuntimeError(f"No data returned by build_dataset for {ticker} in range {start} - {end}")

#     # Ensure 'Date' column exists — some builders set Date as index
#     if "Date" not in df.columns:
#         if isinstance(df.index, pd.DatetimeIndex):
#             logger.warning("'Date' column missing — restoring from DataFrame index.")
#             df = df.reset_index().rename(columns={"index": "Date"})
#         else:
#             raise ValueError("Dataset missing 'Date' column and index is not datetime.")
    
#     # 2) Validate schema
#     valid, missing = validate_dataframe(df, REQUIRED_COLUMNS)
#     if not valid:
#         msg = f"Missing required dataset columns for Bitcoin: {missing}"
#         logger.error(msg)
#         raise ValueError(msg)
#     logger.info("Dataset contains all required columns. Rows: %d", len(df))

#     # Optional: save CSV for debugging
#     if out_csv:
#         df.to_csv(out_csv, index=False)
#         logger.info("Saved CSV to %s", out_csv)

#     # 3) Normalize & insert into MongoDB
#     records = normalize_records_for_mongo(df)
#     logger.info("Normalized %d records for MongoDB insertion.", len(records))

#     client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
#     try:
#         # quick server check
#         client.admin.command('ping')
#     except errors.PyMongoError as e:
#         logger.exception("Cannot connect to MongoDB at %s: %s", mongo_uri, e)
#         raise

#     db = client.get_database(db_name)
#     coll = db.get_collection(HIST_COLLECTION)

#     # Option: remove or upsert duplicates. For now, we will upsert based on date to avoid dupes:
#     inserted = 0
#     for rec in records:
#         # upsert by date
#         res = coll.update_one({"date": rec["date"]}, {"$set": rec}, upsert=True)
#         # count inserted documents (upserted_id present when inserted)
#         if getattr(res, "upserted_id", None) is not None:
#             inserted += 1
#     logger.info("Upsert completed. Inserted %d new records (upsert).", inserted)
#     print(f"Inserted {inserted} records for {ticker} into MongoDB")
#     return inserted


# def parse_args_and_run():
#     parser = argparse.ArgumentParser(description="Ingest BTC dataset via fin_data_builder into MongoDB.")
#     parser.add_argument("--ticker", required=True, help="Ticker symbol. Must be BTC-USD for this project.")
#     parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
#     parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
#     parser.add_argument("--out", required=False, help="Optional CSV output path (for debugging).")
#     parser.add_argument("--mongo-uri", required=False, default=DEFAULT_MONGO_URI, help="MongoDB URI")
#     parser.add_argument("--db", required=False, default=DEFAULT_DB_NAME, help="MongoDB database name")
#     args = parser.parse_args()

#     # Enforce Bitcoin-only rule
#     if args.ticker.upper() != "BTC-USD":
#         logger.error("Ingestion restricted to BTC-USD only. You provided: %s", args.ticker)
#         raise SystemExit(2)

#     try:
#         inserted = ingest_to_mongo(
#             ticker=args.ticker,
#             start=args.start,
#             end=args.end,
#             out_csv=args.out,
#             mongo_uri=args.mongo_uri,
#             db_name=args.db
#         )
#         logger.info("Ingestion finished. Inserted %d records.", inserted)
#     except Exception as e:
#         logger.exception("Ingestion failed: %s", e)
#         raise SystemExit(1)


# if __name__ == "__main__":
#     parse_args_and_run()

# backend/api/ingest.py
import argparse
import logging
import os
import sys
from typing import Tuple, List

import pandas as pd
from pymongo import MongoClient, errors, UpdateOne

# Import the builder from backend.data
try:
    from backend.data.fin_data_builder import build_dataset
except Exception as e:
    raise ImportError(
        "Could not import build_dataset from backend.data.fin_data_builder. "
        "Make sure backend/data/fin_data_builder.py exists and you run this script from project root."
    ) from e

# Required schema
REQUIRED_COLUMNS = [
    "Date", "Open", "High", "Low", "Close", "Volume",
    "Return", "MA5", "MA10", "Volatility",
    "headlines_concat", "news_sentiment", "headline_count"
]

DEFAULT_MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DEFAULT_DB_NAME = os.environ.get("MONGO_DB", "bitcoin_db")
HIST_COLLECTION = "btc_historical"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("ingest")


def validate_dataframe(df: pd.DataFrame, required_cols: List[str] = REQUIRED_COLUMNS) -> Tuple[bool, List[str]]:
    """Check that required columns exist in dataframe. Return (valid, missing_cols)."""
    missing = [c for c in required_cols if c not in df.columns]
    return (len(missing) == 0, missing)


def normalize_records_for_mongo(df: pd.DataFrame) -> List[dict]:
    """
    Convert DataFrame rows into dicts suitable for MongoDB insertion.
    - Ensures Date is ISO string (YYYY-MM-DD)
    - Converts NaNs to None
    """
    df_copy = df.copy()
    df_copy["Date"] = pd.to_datetime(df_copy["Date"]).dt.strftime("%Y-%m-%d")
    records = df_copy.to_dict(orient="records")

    # Convert NaN to None
    for r in records:
        for k, v in list(r.items()):
            if pd.isna(v):
                r[k] = None
    return records


def ingest_to_mongo(
    ticker: str,
    start: str,
    end: str,
    out_csv: str = None,
    mongo_uri: str = DEFAULT_MONGO_URI,
    db_name: str = DEFAULT_DB_NAME
):
    """
    1) Build dataset via build_dataset()
    2) Validate dataset schema
    3) Upsert into MongoDB collection
    """
    logger.info("Starting ingestion for %s from %s to %s", ticker, start, end)

    # 1) Build dataset
    try:
        df = build_dataset(exchange="CRYPTO", ticker=ticker, start=start, end=end, out_path=None)
    except TypeError:
        df = build_dataset("CRYPTO", ticker, start, end, None)
    except Exception as e:
        logger.exception("Error while running build_dataset: %s", e)
        raise

    if df is None or len(df) == 0:
        raise RuntimeError(f"No data returned by build_dataset for {ticker} in range {start} - {end}")

    if "Date" not in df.columns:
        if isinstance(df.index, pd.DatetimeIndex):
            logger.warning("'Date' column missing — restoring from DataFrame index.")
            df = df.reset_index().rename(columns={"index": "Date"})
        else:
            raise ValueError("Dataset missing 'Date' column and index is not datetime.")

    # 2) Validate schema
    valid, missing = validate_dataframe(df)
    if not valid:
        msg = f"Missing required dataset columns for Bitcoin: {missing}"
        logger.error(msg)
        raise ValueError(msg)

    logger.info("Dataset contains all required columns. Rows: %d", len(df))

    # Optionally save CSV
    if out_csv:
        df.to_csv(out_csv, index=False)
        logger.info("Saved CSV to %s", out_csv)

    # 3) Normalize records
    records = normalize_records_for_mongo(df)
    logger.info("Normalized %d records for MongoDB insertion.", len(records))

    # Connect to MongoDB
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        client.admin.command("ping")
    except errors.PyMongoError as e:
        logger.exception("Cannot connect to MongoDB at %s: %s", mongo_uri, e)
        raise

    db = client.get_database(db_name)
    coll = db.get_collection(HIST_COLLECTION)

    # ✅ Bulk upsert (efficient + logs inserted/updated)
    ops = [
        UpdateOne({"Date": rec["Date"]}, {"$set": rec, "$currentDate": {"last_updated": True}}, upsert=True)
        for rec in records
    ]

    inserted, modified = 0, 0
    try:
        result = coll.bulk_write(ops, ordered=False)
        inserted = result.upserted_count or 0
        modified = result.modified_count or 0
        logger.info(f"Upsert completed. Inserted {inserted}, Updated {modified}.")
    except Exception as e:
        logger.error(f"Mongo bulk upsert failed: {e}")
        raise

    total = inserted + modified
    print(f"✅ Upsert done: {inserted} inserted, {modified} updated, total {total}.")
    return total


def parse_args_and_run():
    parser = argparse.ArgumentParser(description="Ingest BTC dataset via fin_data_builder into MongoDB.")
    parser.add_argument("--ticker", required=True, help="Ticker symbol (BTC-USD only).")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--out", required=False, help="Optional CSV output path (for debugging).")
    parser.add_argument("--mongo-uri", required=False, default=DEFAULT_MONGO_URI, help="MongoDB URI")
    parser.add_argument("--db", required=False, default=DEFAULT_DB_NAME, help="MongoDB database name")
    args = parser.parse_args()

    if args.ticker.upper() != "BTC-USD":
        logger.error("Ingestion restricted to BTC-USD only. You provided: %s", args.ticker)
        raise SystemExit(2)

    try:
        total = ingest_to_mongo(
            ticker=args.ticker,
            start=args.start,
            end=args.end,
            out_csv=args.out,
            mongo_uri=args.mongo_uri,
            db_name=args.db
        )
        logger.info("Ingestion finished. Total upserts: %d", total)
    except Exception as e:
        logger.exception("Ingestion failed: %s", e)
        raise SystemExit(1)


if __name__ == "__main__":
    parse_args_and_run()
