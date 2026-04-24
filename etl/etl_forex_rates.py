# Welcome to your new notebook
# Type here in the cell editor to add code!
import requests
import time
import os
from datetime import datetime

API_KEY = os.environ.get("TWELVE_DATA_API_KEY")
pairs = ["USD/INR", "EUR/INR"]
print(f"Total pairs: {len(pairs)}")

all_rows = []

for pair in pairs:
    url = f"https://api.twelvedata.com/time_series?symbol={pair}&interval=1day&start_date=2019-08-01&apikey={API_KEY}"
    response = requests.get(url)
    data = response.json()

    if "values" in data:
        values = data["values"]
        for row in values:
            all_rows.append({
                "currency_pair": pair,
                "trade_date": row["datetime"],
                "open_rate": row["open"],
                "high_rate": row["high"],
                "low_rate": row["low"],
                "close_rate": row["close"],
                "api_fetched_at": datetime.now()
            })
        print(f"{pair:<10} OK — {len(values)} rows")
    else:
        error = data.get("message", "Unknown error")[:50]
        print(f"{pair:<10} FAILED — {error}")

    time.sleep(8)

print(f"\nDone. Total rows collected: {len(all_rows)}")

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, LongType, TimestampType
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame(all_rows)
df.write.format("delta").mode("append").saveAsTable("bronze_forex_rates")
print(f"Rows written: {df.count()}")