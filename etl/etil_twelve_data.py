# Welcome to your new notebook
# Type here in the cell editor to add code!
import requests
import time
import os
from datetime import datetime

# API Configuration
os.environ.get("TWELVE_DATA_API_KEY")

# All 16 tickers: (ticker, exchange or None)
tickers = [
    # India (NSE)
    ("ONGC", "NSE"),
    ("NTPC", "NSE"),
    # Germany (XETR)
    ("ENR", "XETR"),
    ("RWE", "XETR"),
    # USA
    ("XOM", None),
    ("NEE", None),
    # Australia
    ("WDS", None),
    ("STOSF", None),
    # Saudi Arabia (Country ETF)
    ("KSA", None),
    # Qatar (Country ETF)
    ("QAT", None),
    # Sector ETFs
    ("XLE", None),
    ("ICLN", None),
    ("VDE", None),
    # Commodities
    ("CL", None),
    ("BZ", None),
    ("NG", None),
]

print(f"Total tickers: {len(tickers)}")
all_rows = []

for ticker, exchange in tickers:
    if exchange:
        url = f"https://api.twelvedata.com/time_series?symbol={ticker}&exchange={exchange}&interval=1day&start_date=2019-08-01&apikey={API_KEY}"
    else:
        url = f"https://api.twelvedata.com/time_series?symbol={ticker}&interval=1day&start_date=2019-08-01&apikey={API_KEY}"
    
    response = requests.get(url)
    data = response.json()

    if "values" in data:
        values = data["values"]
        for row in values:
            all_rows.append({
                "ticker": ticker,
                "trade_date": row["datetime"],
                "open_price": row["open"],
                "high_price": row["high"],
                "low_price": row["low"],
                "close_price": row["close"],
                "volume": row["volume"],
                "api_fetched_at": datetime.now()
            })
        print(f"{ticker:<10} OK — {len(values)} rows")
    else:
        error = data.get("message", "Unknown error")[:50]
        print(f"{ticker:<10} FAILED — {error}")

    time.sleep(8)

print(f"\nDone. Total rows collected: {len(all_rows)}")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, LongType, TimestampType

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(all_rows)

# Add lakehouse as data source in the notebook first
df.write.format("delta").mode("append").saveAsTable("bronze_stock_etf_prices")

print(f"Rows written: {df.count()}")