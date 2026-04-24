# Welcome to your new notebook
# Type here in the cell editor to add code!
import requests
import time
import os
from datetime import datetime

os.environ.get("EIA_API_KEY")

countries = ["IND", "SAU", "QAT", "AUS", "CHN", "DEU", "USA", "RUS"]

# Product ID, Product Name, Activity ID, Activity Name, Frequency
queries = [
    # Crude Oil (monthly)
    ("57", "1", "monthly"),   # Production
    ("57", "3", "monthly"),   # Imports
    ("57", "4", "monthly"),   # Exports
    # Petroleum (monthly)
    ("54", "2", "monthly"),   # Consumption
    # Natural Gas (annual)
    ("26", "1", "annual"),    # Production
    ("26", "2", "annual"),    # Consumption
    ("26", "3", "annual"),    # Imports
    ("26", "4", "annual"),    # Exports
    # Coal (annual)
    ("7", "1", "annual"),     # Production
    ("7", "2", "annual"),     # Consumption
    ("7", "3", "annual"),     # Imports
    ("7", "4", "annual"),     # Exports
    # Electricity (annual)
    ("2", "12", "annual"),    # Generation
    ("2", "2", "annual"),     # Consumption
    ("2", "3", "annual"),     # Imports
    ("2", "4", "annual"),     # Exports
]

print(f"Countries: {len(countries)}, Queries per country: {len(queries)}")
print(f"Total API calls: {len(countries) * len(queries)}")