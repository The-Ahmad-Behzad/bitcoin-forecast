import pandas as pd
req = ["Date","Open","High","Low","Close","Volume","Return","MA5","MA10","Volatility","headlines_concat","news_sentiment","headline_count"]
df = pd.read_csv("btc_dataset.csv", parse_dates=['Date'])
missing = [c for c in req if c not in df.columns]
print("Missing columns:", missing)
if not missing:
    print("All required columns present. Rows:", len(df))