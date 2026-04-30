# Post-Project 3 Assessment: Energy Security Intelligence

**Date:** April 24, 2026
**Assessed by:** Claude (AI Mentor)
**Previous Score:** 32/40 (Post-Project 2)
**Current Score:** 35/40

---

## Scoring Breakdown (10 Categories × 4 Points Each)

### 1. SQL Proficiency: 8/10 → 8.5/10 (Marginal Improvement)

**What improved:**
- Can write CTE chains (3-7 CTEs) without structural errors
- Understands pivot pattern (MAX CASE WHEN + GROUP BY) — applied it independently on gold_import_export_analysis after learning it in silver_world_bank
- Understands LAG with PARTITION BY and ORDER BY — correct on first attempt for MoM/YoY
- Understands NULLIF for division safety — applied proactively after first reminder
- Understands ROW_NUMBER for "latest record" pattern — new concept, grasped quickly
- Understands CROSS JOIN — new concept, understood the purpose immediately

**What didn't improve:**
- Still cannot write a complete Gold model from scratch without significant guidance. gold_crisis_analysis and gold_stock_performance required ~50% of the SQL to be provided as structure/templates
- SQL alias restriction (can't reference alias in same SELECT) still surprises him every time. This came up 4+ times across the project. By now this should be automatic: "I need a CTE because I want to reference a calculated column"
- Persistent syntax errors: missing commas, missing END in CASE statements, wrong function names (DATEDIFF vs DATEADD), colons instead of commas. These are carelessness, not knowledge gaps
- Doesn't verify column names before writing queries. Used `countryRegionID` instead of `countryRegionId`, `silver_energy` instead of `silver_eia_energy`, `sep.volume_unit` instead of `sep.annual_unit`. These waste debugging time

**Interview readiness:** Can explain any model in the project. Would struggle to write a 5-CTE model live under pressure without hints. Needs to practice writing complex queries from a blank file with only a schema for reference.

---

### 2. Data Integration & Pipeline Design: 9/10 → 9.5/10 (Strong)

**What improved:**
- Built a 3-API pipeline with proper rate limiting, error handling, and incremental patterns
- Understands cross-database queries (Lakehouse → Warehouse via sources.yml)
- Understands the Spark vs T-SQL engine separation in Fabric
- Set up Service Principal authentication independently (based on the automation doc, this was done with minimal help)
- Understands why different layers exist (Bronze = raw, Silver = clean, Gold = analytics)

**What was already strong:**
- API integration skills from Project 1 carried over cleanly
- Understands rate limiting (time.sleep(8)) and why it matters
- Data quality awareness — caught the EIA multi-unit issue, pushed to fix conversion factors immediately

**Gap remaining:**
- Didn't anticipate the EIA multi-unit issue during design — this was caught in Power BI, not during Silver model development. A more experienced engineer would have run `SELECT DISTINCT unit FROM bronze_eia_energy WHERE productId = 57` before writing the Silver model

---

### 3. Schema Design: 8/10 → 8.5/10 (Solid)

**What improved:**
- Understands why energy_role should be derived dynamically, not hardcoded
- Understands NULL handling for edge cases (Petroleum with no Production data)
- Added price_date to gold_stock_performance when he realized the dashboard needed it — good instinct for "what does the consumer of this data need?"

**What was already strong:**
- Medallion architecture understanding
- Knows when to use LEFT JOIN vs INNER JOIN
- Understands seed tables vs models

**Gap remaining:**
- Didn't catch the volume_to_price_conversion error (1037000000 vs 1037000) during design or seed creation. This was a 1000x error that should have been caught with a simple sanity check: "If USA imports 2,551 BCF and gas costs ~$2.50/MMBtu, the import cost should be roughly $2,551 × 1,037,000 × $2.50 = $6.6 billion, which is ~0.03% of $21T GDP." Always sanity-check calculated metrics against known benchmarks.

---

### 4. dbt Proficiency (NEW): 7/10

**Assessment:** First time using dbt. Learned:
- Config blocks, materialization types
- source() vs ref() and when to use each
- Seed files with --full-refresh for schema changes
- Model dependency chains
- dbt debug, dbt run --select, dbt seed
- profiles.yml configuration
- sources.yml for cross-database references

**Gaps:**
- Hasn't written dbt tests (not_null, unique, accepted_values)
- Hasn't used incremental materialization (full table rebuild every time)
- Hasn't used dbt documentation generation (dbt docs generate)
- Doesn't yet understand dbt macros or Jinja templating beyond basic ref/source

**For Project 1 migration:** dbt basics are solid. Focus on adding tests and incremental models.

---

### 5. Cloud Platform (Fabric) Proficiency (NEW): 7.5/10

**Assessment:** First time using any cloud data platform. Learned:
- Workspace and resource creation
- Lakehouse vs Warehouse distinction (and why it matters)
- Notebook development with Spark
- saveAsTable() for Delta tables
- Notebook scheduling
- Semantic model configuration
- Service Principal setup
- Tenant settings and API permissions

**Gaps:**
- Struggled with the Spark vs T-SQL boundary — didn't anticipate that notebooks can't run T-SQL against the Warehouse
- Power BI connection to Fabric was confusing (Direct Lake vs Import vs Warehouse connector)
- Semantic model refresh credentials required troubleshooting

**For interviews:** Can explain the Fabric architecture and why Lakehouse and Warehouse are separate. This is a differentiator — most candidates haven't touched Fabric.

---

### 6. Power BI Proficiency: 7/10 → 7.5/10 (Incremental)

**What improved:**
- Built 6 pages with multiple visual types
- Created a DAX measure (Per Capita Consumption readable) — first time writing DAX
- Understands slicer interactions (Edit Interactions → None for line charts)
- Understands Import mode vs DirectQuery and why Import is better for development

**Persistent issues:**
- Repeatedly puts fields in wrong wells (Value vs Category vs Axis)
- Doesn't check aggregation type (Sum vs Average) before adding fields — results in inflated numbers
- Date hierarchy confusion — every date field defaults to hierarchy, needs to be switched to raw date. This happened 3 times.
- Visual type confusion — used KPI visual when Card was needed
- Formatting takes too long — spends time on labels and titles before verifying data correctness

**For interviews:** Can explain what each page shows and why those visuals were chosen. Would need practice building a dashboard from scratch in under 30 minutes.

---

### 7. GitHub & Version Control: 6/10 → 8/10 (Major Improvement)

**What improved (massive jump):**
- Created repo, cloned, committed, pushed — full workflow
- Set up GitHub Secrets for API keys and Azure credentials
- Created GitHub Actions workflow with YAML
- Understands CI/CD concept: validate on push, execute on schedule
- Understands why API keys should never be in code
- .gitignore for target/ and logs/

**What was flagged in Projects 1 & 2:** "GitHub Actions for orchestration — must be done this time." DONE. Not just done — done with Service Principal authentication and dbt execution in the cloud.

**Gap remaining:**
- Typos in filenames (etil_twelve_data.py) — always double-check before committing
- Didn't realize files needed to be git-added after copying (assumed xcopy = committed)
- No branching strategy — everything on main. Not critical for a solo project, but worth learning for team environments

---

### 8. Debugging & Problem-Solving: 7/10 → 7.5/10 (Improving)

**What improved:**
- Traces errors to root cause more often (checked Fabric query results to debug Power BI issues)
- Runs verification queries after fixes (checked distinct years, distinct products)
- Good instinct for "this number doesn't look right" — caught the 98.89K% cost burden

**Persistent issues:**
- Still asks "can you fix this for me" when facing multiple small errors. The pattern: writes 90% correct SQL, has 3-4 minor issues (missing comma, wrong alias, wrong function), and wants them all fixed rather than finding them himself
- Doesn't read error messages carefully. "Invalid object name 'calculated'" clearly says the CTE name is wrong — but asked for help instead of checking the case
- Doesn't check compiled SQL in target/ folder when dbt errors are unclear. This folder exists specifically for debugging

**The rule that matters:** When you hit an error, spend 5 minutes trying to fix it yourself before asking. Read the error message word by word. Check the line number. Look at the compiled SQL. THEN ask if still stuck.

---

### 9. Independence & Initiative: 6.5/10 → 7/10 (Improving but still the biggest gap)

**Evidence of improvement:**
- Pushed back on shortcuts: "No lets fix the issue" (volume_to_price_conversion), "Lets not compromise the quality" (recovery_days), "I want the end to end to be automated" (Service Principal), "Should not we do the proper way" (rejected Fabric scheduler compromise)
- Proposed visual choices: "Can we have 52 week high and low as gauge" — good instinct
- Caught the missing price_date column: "should not we display the date of the price" — thinking from the user's perspective

**Evidence of remaining dependency:**
- "Can you give me the structure" — asked 6+ times for CTE structures before writing
- "Can you fix these for me" — asked 4+ times for minor corrections
- "Can you provide two corrections" — when there were 2 simple fixes to make
- "What's next" — asked after almost every completed step instead of checking the plan
- "Which table?" — when the page-to-table mapping was already provided in the schema doc

**The interview test:** An interviewer will say "Walk me through how you'd build this crisis analysis model." If the answer starts with "I'd ask my mentor for the CTE structure," that's a fail. The answer should be: "I'd start by defining the analysis windows, then CROSS JOIN tickers with crises, aggregate daily prices to find pre/post/high/low, join back for actual prices, then calculate returns and drawdowns."

**Recommendation:** Before Project 1 migration, take 2 of the Gold models (gold_crisis_analysis and gold_stock_performance) and rewrite them from scratch. Open a blank SQL file, look only at the schema, and write. Don't look at the existing code. Time yourself. This is how you internalize the patterns.

---

### 10. Time Management & Project Planning: 7/10 (Unchanged)

**What went well:**
- Completed all 19 tables in ~3 days of focused work
- Built 6 dashboard pages in one session
- Set up GitHub + automation in one evening

**What could be better:**
- Spent too long on formatting issues (per capita scientific notation, slicer styling) before finishing all pages. Build all 6 pages with data first, polish after.
- Debugging data issues (EIA multi-unit, conversion factor) during Power BI development instead of catching them in Silver/Gold verification. Add a verification step after each Gold table: run 3-4 sanity check queries before moving on.
- The "fix it now vs document it later" tension: his instinct to fix immediately is good for quality but cost time. Learn to distinguish "this breaks the dashboard" (fix now) from "this looks ugly" (fix later).

---

## Overall Score: 35/40 (Up from 32/40)

### Score Progression Across Projects

| Category | Post-P1 (28/40) | Post-P2 (32/40) | Post-P3 (35/40) |
|----------|-----------------|-----------------|-----------------|
| SQL | 7 | 8 | 8.5 |
| Data Integration | 8 | 9 | 9.5 |
| Schema Design | 7 | 8 | 8.5 |
| dbt | — | — | 7 |
| Cloud Platform | — | — | 7.5 |
| Power BI/Viz | 6 | 7 | 7.5 |
| GitHub/CI-CD | 4 | 6 | 8 |
| Debugging | 6 | 7 | 7.5 |
| Independence | 5 | 6.5 | 7 |
| Time Management | 6 | 7 | 7 |

### What Hiring Managers Will See

**Strengths they'll notice:**
1. Cloud-native pipeline with zero local dependency — rare for entry-level candidates
2. Service Principal + GitHub Actions + dbt — enterprise tooling
3. 3 live API integrations with proper rate limiting
4. Medallion architecture with 19 tables
5. Comprehensive documentation
6. Can explain WHY every design decision was made

**Gaps they might probe:**
1. "Write a window function on this whiteboard" — practice LAG, ROW_NUMBER, RANK without IDE
2. "What would you change if you had to rebuild this?" — have a clear answer about EIA data structure, incremental dbt models, dbt tests
3. "How would you handle a new country being added?" — should be "add a row to silver_countries seed, rerun dbt" — test if he understands the architecture's extensibility
4. "Show me a complex query you wrote" — gold_crisis_analysis is the answer, but he needs to be able to explain every CTE without notes

---

## Specific Recommendations for Project 1 Migration

### Do Before Starting:
1. **Rewrite gold_crisis_analysis from scratch** — blank file, schema only, no reference code. Time yourself.
2. **Rewrite gold_stock_performance from scratch** — same exercise. These are the two models where you received the most help.
3. **Practice explaining the automation pipeline** — say it out loud: "Fabric notebooks pull API data at 2 AM. GitHub Actions triggers dbt at 3 AM using a Service Principal. Power BI refreshes at 3:30 AM."

### During Migration:
1. **Propose the plan before asking** — "I think I should copy the stored procedures, strip the CREATE TABLE, add config blocks, replace table names with ref(), and adjust for Fabric's T-SQL dialect. Does that sound right?"
2. **Verify each table after building** — run 3 sanity check queries per Gold table before moving on
3. **Don't ask for CTE structures** — you've built 5 Gold tables now. The patterns are in your head.
4. **Track your own errors** — keep a running list of syntax mistakes (missing comma, wrong alias, etc.) and check against it before running dbt

### For the Portfolio Website:
1. The architecture diagram in the README is interview-ready
2. Prepare a 2-minute walkthrough of each dashboard page
3. Have the GitHub repo URL memorized
4. Be ready to explain: "Why Fabric instead of Snowflake?" Answer: "Fabric trial was free, integrates natively with Power BI, and the Lakehouse + Warehouse architecture taught me both Spark and T-SQL."

---

## The Honest Summary

You went from "never touched cloud, never used dbt, never set up CI/CD" to "fully automated cloud pipeline with Service Principal authentication" in 10 days. That's not nothing.

But the gap between "built with heavy guidance" and "can build independently" is still real. The gold_crisis_analysis model — your most complex work — was about 50% guided. In an interview, you'll need to own every line of it. The fix is simple: rewrite it alone, twice, until you can do it without looking.

Your strongest asset isn't any single technical skill — it's your refusal to compromise. You pushed back on every shortcut: "No lets fix the issue," "Lets not compromise the quality," "Should not we do the proper way." That mindset is more valuable than SQL syntax. Companies can teach syntax. They can't teach standards.

35/40. Three points higher. Earn the remaining 5 by building Project 1 migration without asking for structures.

---

*Assessment date: April 24, 2026*
*Next milestone: Project 1 Migration → Fabric*
*Target completion: April 27, 2026*
*Job application deadline: June 10, 2026*
