# Energy Security Intelligence Dashboard

A cloud-native data pipeline and analytics dashboard that analyzes global energy security across 8 countries — mapping production, consumption, trade flows, crisis impacts, and investment performance. Built on Microsoft Fabric with automated daily refreshes via GitHub Actions.

---

## Business Story

Global energy prices surged after the Russia-Ukraine crisis, yet India's prices remained relatively stable — leaving many wondering why. This dashboard answers that question by mapping India's complete energy supply chain: where we source our oil, gas, and electricity, how much we consume versus produce, and how dependent we are on imports. It then extends the analysis globally, comparing energy security across countries — who is self-sufficient, who is dangerously exposed, and how past crises reshaped energy trade patterns. For the opportunistic investor, the dashboard connects energy disruptions to stock market performance, revealing whether energy crises create buying opportunities or portfolio risks.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES (Live APIs)                      │
├──────────────┬──────────────────┬─────────────┬─────────────────────┤
│  EIA API     │  Twelve Data API │  World Bank │  FRED DGS10        │
│  (Monthly)   │  (Daily, $29/mo) │  (Annual)   │  (Manual/Annual)   │
│  Free        │  16 tickers      │  Free       │                    │
│  8 countries │  2 forex pairs   │  8 countries│                    │
└──────┬───────┴────────┬─────────┴──────┬──────┴─────────┬──────────┘
       │                │                │                │
       ▼                ▼                ▼                ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    FABRIC NOTEBOOKS (Bronze ETL)                     │
│          Scheduled: 2:00 AM, 2:10 AM, 2:20 AM, 2:30 AM IST         │
├──────────────────────────────────────────────────────────────────────┤
│  ETL_Twelve_Data  │  ETL_EIA_Energy  │  ETL_World_Bank  │  ETL_Forex│
│  → bronze_stock_  │  → bronze_eia_   │  → bronze_world_ │  → bronze_│
│    etf_prices     │    energy        │    bank_indicators│   forex_  │
│  (26,633 rows)    │  (25,111 rows)   │  (408 rows)      │   rates   │
│                   │                  │                   │  (3,707)  │
└──────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│              GITHUB ACTIONS + dbt (Silver & Gold)                    │
│              Scheduled: 3:00 AM IST (cron)                          │
│              Auth: Service Principal + MSAL Token                    │
├──────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  SILVER LAYER (Cleaned + Reference)          10 tables               │
│  ├── silver_stock_prices     (26,633 rows)   USD conversion + returns│
│  ├── silver_forex_rates      (3,707 rows)    Daily change %          │
│  ├── silver_eia_energy       (~25,000 rows)  Annualized volumes      │
│  ├── silver_world_bank       (~200 rows)     GDP + Population pivot  │
│  ├── silver_calendar         (9,497 rows)    Date dimension           │
│  ├── silver_countries        (8 rows)        Country reference        │
│  ├── silver_energy_products  (5 rows)        Product + conversion     │
│  ├── silver_stocks_reference (16 rows)       Ticker metadata          │
│  ├── silver_crisis_events    (4 rows)        Crisis definitions       │
│  └── silver_risk_free_rate   (8 rows)        US Treasury yields       │
│                                                                      │
│  GOLD LAYER (Analytics-Ready)                5 tables                 │
│  ├── gold_energy_overview        Self-Sufficiency, Cost Burden        │
│  ├── gold_energy_prices          Monthly benchmark trends             │
│  ├── gold_import_export_analysis Import Dependency, trade balance     │
│  ├── gold_crisis_analysis        Crisis return, drawdown, recovery    │
│  └── gold_stock_performance      Sharpe ratio, volatility, YoY       │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    POWER BI (6 Dashboard Pages)                      │
│              Scheduled Refresh: 3:30 AM IST                          │
├──────────────────────────────────────────────────────────────────────┤
│  Page 1: Energy Overview       │  Page 4: Crisis Analysis            │
│  Page 2: Energy Prices         │  Page 5: Energy Stocks & ETFs       │
│  Page 3: Import & Export       │  Page 6: Country Deep Dive          │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Dashboard Pages

| Page | Title | Key Metrics | Data Source |
|------|-------|-------------|------------|
| 1 | Energy Overview | Self-Sufficiency Ratio, Energy Cost Burden (% GDP), Per Capita Consumption | gold_energy_overview |
| 2 | Energy Prices | Monthly Benchmark Trend, YoY Change %, MoM Change % | gold_energy_prices |
| 3 | Import & Export Analysis | Import Dependency %, Net Trade Balance, YoY Trade Changes | gold_import_export_analysis |
| 4 | Crisis Analysis | Crisis Return %, Max Drawdown %, Recovery Days | gold_crisis_analysis |
| 5 | Energy Stocks & ETFs | Current Price, Sharpe Ratio, Volatility, 52-Week Range | gold_stock_performance |
| 6 | Country Deep Dive | All metrics filtered by selected country | All Gold tables |

---

## Countries Covered (8)

| Country | Role | Stock Coverage |
|---------|------|---------------|
| India | Major energy importer | ONGC, NTPC (NSE) |
| Saudi Arabia | Largest crude exporter | KSA ETF proxy (NYSE) |
| Qatar | Largest LNG exporter | QAT ETF proxy (NASDAQ) |
| Australia | Major coal/LNG exporter | WDS, STOSF (NYSE/OTC) |
| China | Largest energy consumer | EIA/World Bank only — no stocks |
| Germany | Energy crisis victim | ENR, RWE (XETR) |
| USA | Largest NA exporter | XOM, NEE (NYSE) |
| Russia | Energy weapon | EIA/World Bank only — no stocks |

---

## Tickers Tracked (16)

**Stocks (10):** ONGC, NTPC, ENR, RWE, XOM, NEE, WDS, STOSF, KSA, QAT
**Sector ETFs (3):** XLE, ICLN, VDE
**Commodities (3):** CL (WTI), BZ (Brent), NG (Natural Gas)

---

## Crisis Events Analyzed (4)

| Crisis | Period | Type |
|--------|--------|------|
| Saudi Oil Facility Attack | Sep 14 – Oct 14, 2019 | Supply shock |
| COVID-19 Demand Collapse | Mar 1 – Jun 30, 2020 | Demand collapse |
| Russia-Ukraine Energy Crisis | Feb 24, 2022 – Feb 24, 2023 | Geopolitical |
| Iran-Israel Tensions | Oct 1, 2024 – Ongoing | Geopolitical |

---

## Business Questions Answered (10)

### Descriptive (5)
1. What are the current and historical prices of energy commodities?
2. What are the current and historical prices of energy stocks and ETFs?
3. How much does each country produce, consume, import, and export?
4. What is the import vs export balance for each country?
5. How have energy trade volumes changed year-over-year?

### Analytical (5)
6. **Energy Self-Sufficiency Ratio** — can this country survive an energy embargo?
7. **Import Dependency Ratio** — how dependent is each country on imports?
8. **Crisis Impact Score** — who wins and who loses during energy crises?
9. **Energy Cost Burden** — what % of GDP is spent on energy imports?
10. **Stock-Crisis Correlation** — do producers benefit and importers suffer?

---

## Key Metrics & Formulas

| Metric | Formula |
|--------|---------|
| Self-Sufficiency Ratio | Production / Consumption × 100 |
| Import Dependency | Imports / Consumption × 100 |
| Energy Cost Burden | (Import Volume × Benchmark Price × Conversion Factor) / GDP × 100 |
| Crisis Return | (Post-Crisis Price - Pre-Crisis Price) / Pre-Crisis Price × 100 |
| Max Drawdown | (Crisis Low - Pre-Crisis Price) / Pre-Crisis Price × 100 |
| Sharpe Ratio | (Annualized Return - Risk-Free Rate) / Annualized Volatility |
| Annualized Volatility | STDEV(daily returns) × √252 |

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Cloud Platform | Microsoft Fabric (Trial) |
| Data Warehouse | Fabric Data Warehouse |
| Data Lake | Fabric Lakehouse |
| ETL | Python (Fabric Notebooks) |
| Transformation | dbt Core 1.11.8 (dbt-fabric 1.9.9) |
| Orchestration | GitHub Actions (CI/CD) + Fabric Scheduler |
| Authentication | Azure Service Principal + MSAL |
| BI Tool | Power BI (Import Mode) |
| Version Control | GitHub |

---

## Automation Pipeline

```
2:00 AM IST  →  Fabric Notebooks pull fresh API data (Bronze)
3:00 AM IST  →  GitHub Actions triggers dbt (Bronze → Silver → Gold)
3:30 AM IST  →  Power BI semantic model refreshes (Gold → Dashboard)
```

**Zero local dependency.** Laptop can be off — everything runs in the cloud.

| Component | Trigger | Runs On |
|-----------|---------|---------|
| Bronze ETL | Fabric Scheduler (daily) | Fabric cloud |
| dbt transforms | GitHub Actions cron | GitHub runner (Ubuntu) |
| Power BI refresh | Fabric Scheduler (daily) | Fabric cloud |
| Code validation | GitHub Actions (on push) | GitHub runner |

---

## Medallion Architecture

| Layer | Tables | Purpose |
|-------|--------|---------|
| Bronze | 4 | Raw API data, untouched |
| Silver | 10 (5 cleaned + 5 reference) | Cleaned, typed, standardized, annualized |
| Gold | 5 | Pre-calculated metrics, denormalized for Power BI |
| **Total** | **19** | |

---

## Project Structure

```
energy-security-intelligence/
├── .github/
│   └── workflows/
│       └── energy_pipeline.yml      # GitHub Actions: dbt + validation
├── etl/
│   ├── etl_twelve_data.py           # Stocks, ETFs, commodities
│   ├── etl_forex_rates.py           # USD/INR, EUR/INR
│   ├── etl_eia_energy.py            # EIA energy data (8 countries)
│   └── etl_world_bank.py            # GDP + population
├── energy_dbt/
│   ├── dbt_project.yml              # dbt configuration
│   ├── models/
│   │   ├── sources.yml              # Bronze source definitions
│   │   ├── Silver/
│   │   │   ├── silver_calendar.sql
│   │   │   ├── silver_forex_rates.sql
│   │   │   ├── silver_stock_prices.sql
│   │   │   ├── silver_eia_energy.sql
│   │   │   └── silver_world_bank.sql
│   │   └── Gold/
│   │       ├── gold_energy_overview.sql
│   │       ├── gold_energy_prices.sql
│   │       ├── gold_import_export_analysis.sql
│   │       ├── gold_crisis_analysis.sql
│   │       └── gold_stock_performance.sql
│   └── seeds/
│       ├── silver_countries.csv
│       ├── silver_energy_products.csv
│       ├── silver_stocks_reference.csv
│       ├── silver_crisis_events.csv
│       └── silver_risk_free_rate.csv
├── docs/
│   ├── bronze_etl_documentation.md
│   ├── dbt_setup_documentation.md
│   ├── silver_cleaned_tables_documentation.md
│   ├── gold_layer_documentation.md
│   ├── data_limitations_documentation.md
│   └── automation_documentation.md
└── README.md
```

---

## Data Limitations (Documented Transparently)

| Limitation | Impact | Resolution |
|-----------|--------|-----------|
| Petroleum: consumption only from EIA | No trade metrics for Petroleum | Use Crude Oil for trade analysis |
| Crude Oil: no consumption data | Self-Sufficiency can't be calculated | Consumption tracked under Petroleum |
| Coal/Electricity: no benchmark price | Energy Cost Burden shows NULL | Regional pricing unavailable in Twelve Data |
| China/Russia: no stock coverage | Pages 4-5 empty for these countries | Groww plan doesn't cover SSE/MOEX |
| Benchmark prices as proxy | Import costs are estimates | Actual prices vary by bilateral agreements |

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Benchmark | WTI (CL) over Brent (BZ) | BZ data starts Jun 2021 only; CL covers all crises |
| Energy Role | Dynamically derived, not hardcoded | Self-Sufficiency > 100% = Producer, < 100% = Consumer |
| Grain | Annual for cross-source, Daily for stocks | Prevents inflating GDP denominators across months |
| Sharpe Ratio | US 10-Year Treasury as risk-free rate | Global benchmark, more accurate than 0% assumption |
| Currency | USD for all Gold calculations | INR available via DAX measures in Power BI |

---

## What Makes This Different from Projects 1 & 2

| Aspect | Project 1 | Project 2 | Project 3 |
|--------|-----------|-----------|-----------|
| Database | Local SQL Server | Local SQL Server | Microsoft Fabric (Cloud) |
| Transforms | Stored Procedures | Stored Procedures | dbt |
| Orchestration | Task Scheduler | SQL Server Agent | GitHub Actions + Fabric |
| Local Dependency | Yes | Yes | **Zero** |
| Data Sources | 1 API | 1 API + 4 files | 3 Live APIs |
| Tables | 15 | 23 | 19 |
| Auth | Personal login | Personal login | Service Principal |

---

## How to Run Locally

### Prerequisites
- Python 3.12
- dbt-fabric (`pip install dbt-fabric`)
- Azure CLI (`az login --allow-no-subscriptions`)
- Access to Fabric Workspace

### Run dbt manually
```bash
cd energy_dbt
dbt seed
dbt run
```

### Run ETL manually
```bash
# These scripts are designed for Fabric Notebooks (Spark)
# For local testing, modify to use local database connections
python etl/etl_twelve_data.py
```

---

## Author

**Arravind Shri**
- Building a portfolio of cloud-native data engineering projects
- Transitioning from Customer Success Engineer to Data/BI Analyst
- GitHub: [@ArravindShri](https://github.com/ArravindShri)

---

*Built: April 2026 | Fabric Trial: 39 days remaining*
