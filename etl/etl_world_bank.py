# Welcome to your new notebook
# Type here in the cell editor to add code!
import requests
import time
from datetime import datetime

countries = {
    "IND": "India", "SAU": "Saudi Arabia", "QAT": "Qatar",
    "AUS": "Australia", "CHN": "China", "DEU": "Germany",
    "USA": "United States", "RUS": "Russia"
}

indicators = [
    ("NY.GDP.MKTP.CD", "GDP (current US$)"),
    ("SP.POP.TOTL", "Population, total"),
]

print(f"Countries: {len(countries)}, Indicators: {len(indicators)}")
all_rows = []

for code, name in countries.items():
    for indicator_code, indicator_name in indicators:
        url = f"http://api.worldbank.org/v2/country/{code}/indicator/{indicator_code}?format=json&per_page=100&date=2000:2025"
        response = requests.get(url)
        data = response.json()

        if len(data) > 1 and data[1]:
            for row in data[1]:
                if row["value"] is not None:
                    all_rows.append({
                        "country_code": code,
                        "country_name": name,
                        "indicator_code": indicator_code,
                        "indicator_name": indicator_name,
                        "year": int(row["date"]),
                        "value": row["value"],
                        "data_source": "World Bank",
                        "api_fetched_at": datetime.now()
                    })
            print(f"{name:<18} {indicator_name:<25} OK")
        else:
            print(f"{name:<18} {indicator_name:<25} FAILED")

        time.sleep(0.3)

print(f"\nDone. Total rows collected: {len(all_rows)}")
# IMF 2025 GDP gap-fill
url = "https://www.imf.org/external/datamapper/api/v1/NGDPD"
response = requests.get(url)
imf_data = response.json()

values = imf_data.get("values", {}).get("NGDPD", {})

imf_codes = {
    "IND": "IND", "SAU": "SAU", "QAT": "QAT",
    "AUS": "AUS", "CHN": "CHN", "DEU": "DEU",
    "USA": "USA", "RUS": "RUS"
}

for code, name in countries.items():
    imf_code = imf_codes[code]
    gdp_2025 = values.get(imf_code, {}).get("2025")
    
    if gdp_2025:
        all_rows.append({
            "country_code": code,
            "country_name": name,
            "indicator_code": "NY.GDP.MKTP.CD",
            "indicator_name": "GDP (current US$)",
            "year": 2025,
            "value": float(gdp_2025) * 1_000_000_000,
            "data_source": "IMF",
            "api_fetched_at": datetime.now()
        })
        print(f"{name:<18} 2025 GDP: ${float(gdp_2025):,.1f}B (IMF)")
    else:
        print(f"{name:<18} 2025 GDP: MISSING")

print(f"\nTotal rows after IMF gap-fill: {len(all_rows)}")
for row in all_rows:
    if row["value"] is not None:
        row["value"] = float(row["value"])
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, LongType, TimestampType
spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame(all_rows)
df.write.format("delta").mode("append").saveAsTable("bronze_world_bank_indicators")
print(f"Rows written: {df.count()}")