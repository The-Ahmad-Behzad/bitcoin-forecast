"""
Given an exchange and ticker, fetch OHLCV via yfinance, compute minimal indicators,
scrape headlines (Yahoo Finance / Reuters / CoinDesk), compute sentiment (VADER),
align news by date, and save CSV/JSON dataset ready for modeling.
"""

"""
fin_data_builder.py - improved column mapping and diagnostics

Usage:
    python fin_data_builder.py --exchange NASDAQ --ticker AAPL --start 2025-08-15 --end 2025-09-16 --out aapl_dataset.csv
"""

import feedparser
from dateutil import parser as dateparser
import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import List
import pandas as pd
import numpy as np
import yfinance as yf
import requests
from bs4 import BeautifulSoup
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from requests.adapters import HTTPAdapter, Retry

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

def requests_session_with_retries(retries=3, backoff=0.5):
    s = requests.Session()
    retries = Retry(total=retries, backoff_factor=backoff,
                    status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s

SESSION = requests_session_with_retries()
SENT_ANALYZER = SentimentIntensityAnalyzer()

### ---------- Structured data ----------
def flatten_columns_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    cols = df.columns
    if isinstance(cols, pd.MultiIndex):
        new_cols = []
        for col in cols:
            if isinstance(col, tuple):
                joined = "_".join([str(c) for c in col if c not in (None, '')]).strip()
                if joined == "":
                    joined = "Unnamed"
                new_cols.append(joined)
            else:
                new_cols.append(str(col))
        df.columns = new_cols
    else:
        df.columns = [str(c) for c in df.columns]
    return df

def map_price_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map available DataFrame columns to canonical set: Open, High, Low, Close, Volume
    Uses case-insensitive substring matching and tries sensible fallbacks (e.g. 'Adj Close' -> 'Close').
    Returns a DataFrame renamed to canonical columns (only columns found).
    """
    df = flatten_columns_if_needed(df.copy())
    original_cols = list(df.columns)
    logging.info(f"Original price columns from source: {original_cols}")

    mapping = {}
    lowered = {c: c.lower() for c in df.columns}
    # helper to find first column containing keyword
    def find_col(keyword):
        # exact match preferred
        for c in df.columns:
            if c.lower() == keyword:
                return c
        # substring match
        for c in df.columns:
            if keyword in c.lower():
                return c
        return None

    # Map each canonical column
    col_open = find_col('open')
    col_high = find_col('high')
    col_low = find_col('low')
    col_close = find_col('close')  # will match 'Adj Close' too
    col_volume = find_col('volume')

    if col_open: mapping[col_open] = 'Open'
    if col_high: mapping[col_high] = 'High'
    if col_low: mapping[col_low] = 'Low'
    if col_close: mapping[col_close] = 'Close'
    if col_volume: mapping[col_volume] = 'Volume'

    # Special fallback: sometimes yfinance returns 'Adj Close' but not 'Close'
    if ('Adj Close' in df.columns) and ('Close' not in mapping.values()):
        mapping['Adj Close'] = 'Close'

    if not mapping:
        # nothing found, return empty to allow nicer error downstream
        return pd.DataFrame()

    renamed = df.rename(columns=mapping)
    # Return only the canonical columns we found
    found_cols = [c for c in ['Open', 'High', 'Low', 'Close', 'Volume'] if c in renamed.columns]
    return renamed[found_cols]

def fetch_prices(ticker: str, start: str, end: str) -> pd.DataFrame:
    logging.info(f"Fetching price data for {ticker} from {start} to {end} via yfinance")
    # ask yfinance for raw history (auto_adjust explicitly set to avoid futures)
    df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=False)
    if df.empty:
        raise RuntimeError(f"No price data returned for {ticker}. Check ticker or date range.")
    # map incoming columns to canonical ones
    mapped = map_price_columns(df)
    if mapped.empty:
        # diagnostic: show columns and raise helpful error
        raise RuntimeError(
            f"Unable to find Open/High/Low/Close/Volume columns in the price data. "
            f"Returned columns: {list(df.columns)}"
        )
    mapped.index = pd.to_datetime(mapped.index).normalize()
    return mapped

def compute_indicators(df: pd.DataFrame, ma_windows: List[int] = [5, 10], vol_window: int = 5) -> pd.DataFrame:
    out = df.copy()
    if 'Close' not in out.columns:
        raise RuntimeError(f"Close column missing from price data; cannot compute indicators. Available cols: {list(out.columns)}")
    # vectorized safe computations
    out['Return'] = np.log(out['Close'] / out['Close'].shift(1))
    for w in ma_windows:
        out[f'MA{w}'] = out['Close'].rolling(window=w, min_periods=1).mean()
    out['Volatility'] = out['Return'].rolling(window=vol_window, min_periods=1).std()
    return out

### ---------- Unstructured (news) ----------
def fetch_yahoo_headlines(ticker: str):
    base = f"https://finance.yahoo.com/quote/{ticker}/news?p={ticker}"
    headers = {"User-Agent": "Mozilla/5.0"}
    logging.info(f"Scraping Yahoo news for {ticker} ({base})")
    items = []
    try:
        r = SESSION.get(base, headers=headers, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.select("h3 a"):
            headline = a.get_text(strip=True)
            parent = a.find_parent('li')
            date_obj = None
            if parent:
                time_el = parent.find('time')
                if time_el and time_el.has_attr('datetime'):
                    try:
                        date_obj = pd.to_datetime(time_el['datetime']).date()
                    except Exception:
                        date_obj = None
            if date_obj is None:
                date_obj = datetime.utcnow().date()
            items.append({'date': pd.to_datetime(date_obj), 'headline': headline, 'source': 'yahoo'})
    except Exception as e:
        logging.warning(f"Yahoo scraping failed for {ticker}: {e}")
    return items

def fetch_yf_news(ticker: str):
    items = []
    try:
        tk = yf.Ticker(ticker)
        news = tk.news
        if isinstance(news, list):
            for n in news:
                title = n.get('title') or n.get('linkText') or n.get('summary') or None
                tstamp = n.get('providerPublishTime') or n.get('time') or None
                if not title:
                    continue
                # robust timestamp handling
                date_obj = None
                if tstamp is not None:
                    try:
                        t_int = int(tstamp)
                        # heuristic: if > 1e12 it's ms; if < 1e12 it's seconds
                        if t_int > 1_000_000_000_000:
                            date_obj = pd.to_datetime(t_int, unit='ms').date()
                        else:
                            date_obj = pd.to_datetime(t_int, unit='s').date()
                    except Exception:
                        try:
                            date_obj = pd.to_datetime(tstamp).date()
                        except Exception:
                            date_obj = datetime.utcnow().date()
                else:
                    date_obj = datetime.utcnow().date()
                items.append({'date': pd.to_datetime(date_obj), 'headline': title, 'source': 'yfinance_news'})
    except Exception as e:
        logging.warning(f"yfinance news fallback failed for {ticker}: {e}")
    return items

def fetch_coindesk_headlines(coin_slug='bitcoin'):
    base = f"https://www.coindesk.com/tag/{coin_slug}"
    items = []
    try:
        r = SESSION.get(base, timeout=10)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.select("h3 a, h4 a"):
            headline = a.get_text(strip=True)
            items.append({'date': pd.to_datetime(datetime.utcnow().date()), 'headline': headline, 'source': 'coindesk'})
    except Exception as e:
        logging.warning(f"CoinDesk scraping failed: {e}")
    return items

def fetch_google_news_rss(query: str, max_items=50):
    """
    Fetch headlines from Google News RSS for the given query (e.g., ticker or company name).
    Returns list of {'date': Timestamp, 'headline': str, 'source': 'google_rss'}.
    """
    items = []
    try:
        rss_url = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        logging.info(f"Fetching Google News RSS for query='{query}' -> {rss_url}")
        feed = feedparser.parse(rss_url)

        for entry in feed.entries[:max_items]:
            title = entry.get('title') or ''
            if not title:
                continue
            date_str = entry.get('published') or entry.get('updated') or None
            try:
                date_obj = dateparser.parse(date_str).date() if date_str else datetime.utcnow().date()
            except Exception:
                date_obj = datetime.utcnow().date()
            items.append({
                'date': pd.to_datetime(date_obj),
                'headline': title,
                'source': 'google_rss'
            })
    except Exception as e:
        logging.warning(f"Google News RSS failed for query='{query}': {e}")
    return items


def score_headline_sentiment(text: str) -> float:
    vs = SENT_ANALYZER.polarity_scores(text)
    return vs['compound']

def aggregate_news_by_date(news_items: List[dict]) -> pd.DataFrame:
    if not news_items:
        return pd.DataFrame(columns=['headlines_concat', 'news_sentiment', 'headline_count'])
    df = pd.DataFrame(news_items)
    df['date'] = pd.to_datetime(df['date']).dt.normalize()
    df['sentiment'] = df['headline'].apply(score_headline_sentiment)
    grouped = df.groupby('date').agg(
        headlines_concat=('headline', lambda arr: " || ".join(list(arr))),
        news_sentiment=('sentiment', 'mean'),
        headline_count=('headline', 'count')
    ).sort_index()
    grouped.index.name = 'Date'
    return grouped

### ---------- Merge & Save ----------
def merge_and_save(price_df: pd.DataFrame, news_agg_df: pd.DataFrame, out_path: str, fmt='csv'):
    """
    Aligns price rows with news rows using a merge_asof (backward) so that
    price on Date D gets the most recent news on or before D (within 1 day tolerance).
    This is more robust than exact-date matching.
    """
    # flatten price columns if needed
    price_df = flatten_columns_if_needed(price_df)
    left = price_df.reset_index()
    if left.columns[0].lower() != 'date':
        left = left.rename(columns={left.columns[0]: 'Date'})
    left['Date'] = pd.to_datetime(left['Date']).dt.normalize()
    left_sorted = left.sort_values('Date').reset_index(drop=True)

    if news_agg_df is None or news_agg_df.empty:
        # create an empty news frame with the expected columns and date dtype
        right = pd.DataFrame(columns=['Date', 'headlines_concat', 'news_sentiment', 'headline_count'])
    else:
        right = news_agg_df.reset_index()
        if right.columns[0].lower() != 'date':
            right = right.rename(columns={right.columns[0]: 'Date'})
        right['Date'] = pd.to_datetime(right['Date']).dt.normalize()
        # keep only one record per date (should already be so), and sort
        right = right.sort_values('Date').drop_duplicates(subset=['Date']).reset_index(drop=True)
        right = flatten_columns_if_needed(right)

    # If right is empty, merge_asof will still work but we keep the structure
    if right.empty:
        merged = left_sorted.copy()
        merged['headlines_concat'] = ''
        merged['headline_count'] = 0
        merged['news_sentiment'] = 0.0
    else:
        # both must be sorted by Date ascending for merge_asof
        left_sorted = left_sorted.sort_values('Date').reset_index(drop=True)
        right = right.sort_values('Date').reset_index(drop=True)
        # Use merge_asof to attach previous-or-same-day news within 1 day tolerance
        merged = pd.merge_asof(left_sorted, right,
                               on='Date',
                               direction='backward',
                               tolerance=pd.Timedelta('1D'))
        # If no news matched within tolerance, fields will be NaN -> fill defaults
        merged['headlines_concat'] = merged.get('headlines_concat', '').fillna('')
        merged['headline_count'] = merged.get('headline_count', 0).fillna(0).astype(int)
        merged['news_sentiment'] = merged.get('news_sentiment', 0.0).fillna(0.0)
        merged['Volatility'] = merged.get('Volatility', 0.0).fillna(0.0)
        merged['Return'] = merged.get('Return', 0.0).fillna(0.0)

    # set Date back as index and save
    merged = merged.set_index('Date').sort_index()
    if fmt == 'csv':
        merged.to_csv(out_path, index=True, encoding="utf-8-sig")
    else:
        merged.to_json(out_path, orient='records', date_format='iso')
    logging.info(f"Saved merged dataset to {out_path}")
    return merged

### ---------- Main flow ----------
def build_dataset(exchange: str, ticker: str, start: str, end: str, out_path: str):
    price_df = fetch_prices(ticker, start, end)
    price_df = compute_indicators(price_df)
    
    news_items = []

    # 1. Try Yahoo scraping
    news_items += fetch_yahoo_headlines(ticker)

    # 2. Fallback to yfinance
    if not news_items:
        logging.info("Yahoo returned no headlines — trying yfinance .news fallback")
        news_items += fetch_yf_news(ticker)

    # 3. Fallback to Google News RSS with ticker
    if not news_items:
        logging.info("yfinance returned no headlines — trying Google News RSS (ticker)")
        news_items += fetch_google_news_rss(ticker)

    # 4. Fallback to Google News RSS with broader company name
    if not news_items:
        company_name = "Apple Inc" if ticker.upper() == "AAPL" else ticker
        logging.info(f"No headlines yet — trying Google News RSS with company name '{company_name}'")
        news_items += fetch_google_news_rss(company_name)

    # Log how many we finally got
    logging.info(f"Total collected news items: {len(news_items)}")

    
    if ticker.upper().endswith("USD"):
        coin = ticker.split('-')[0].lower()
        news_items += fetch_coindesk_headlines(coin)

    news_agg = aggregate_news_by_date(news_items)

    # --- DEBUG: show what we actually scraped (first run only) ---
    logging.info(f"Collected {len(news_items)} raw news items.")
    if len(news_items) > 0:
        for i, it in enumerate(news_items[:6]):
            # ensure safe printing even if fields are missing
            d = pd.to_datetime(it.get('date', pd.NaT)).date() if it.get('date') is not None else None
            logging.info(f" sample news {i}: date={d} source={it.get('source')} title={(it.get('headline') or '')[:140]}")
    logging.info(f"Aggregated daily news records: {len(news_agg)}")
    if not news_agg.empty:
        logging.info("Aggregated news head:\n" + news_agg.head().to_string())

    merged_df = merge_and_save(price_df, news_agg, out_path, fmt='csv')
    
        # ✅ Ensure 'Date' is an explicit column (not just index)
    if "Date" not in merged_df.columns:
        merged_df = merged_df.reset_index().rename(columns={"index": "Date"})
    merged_df["Date"] = pd.to_datetime(merged_df["Date"]).dt.strftime("%Y-%m-%d")

    # ✅ Drop rows missing essential data (safety)
    merged_df = merged_df.dropna(subset=["Close"], how="any")

    logging.info(f"Final dataset ready for insertion: {len(merged_df)} rows, columns={list(merged_df.columns)}")

    
    return merged_df

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('--exchange', type=str, required=True)
    p.add_argument('--ticker', type=str, required=True)
    default_end = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    default_start = (datetime.now(timezone.utc) - timedelta(days=30)).strftime('%Y-%m-%d')
    p.add_argument('--start', type=str, required=False, default=default_start)
    p.add_argument('--end', type=str, required=False, default=default_end)
    p.add_argument('--out', type=str, required=False, default='dataset.csv')
    return p.parse_args()

if __name__ == '__main__':
    args = parse_args()
    try:
        df = build_dataset(args.exchange, args.ticker, args.start, args.end, args.out)
        print(df.tail(10).to_string())
    except Exception as e:
        logging.error(f"Fatal error in building dataset: {e}")
        raise
