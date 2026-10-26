# Festival Data Detective — PySpark Analytics

## 1. Project Overview

**Festival Data Detective** is a data engineering and analytics project that investigates operational data from a three-day music festival. It uses Python, PySpark DataFrames, Spark SQL, window functions, data validation, and Parquet storage to turn raw festival records into structured datasets and useful operational insights.

The project focuses on five key areas:

- **Stage activity and capacity:** Examine valid ticket-scan activity by stage and compare it with the listed stage capacity.
- **Repeated ticket scans:** Identify exact duplicates and flag potentially repeated scans for further review.
- **Food-stall performance:** Calculate completed-order revenue, completed-order counts, and average order value by food stall.
- **Artist and genre analysis:** Compare performances using a scan-based attendance proxy and summarize results by genre.
- **Incident and time-based analysis:** Explore incident counts by stage and compare activity across time periods.

The objective is to create a reproducible analysis that helps festival organizers investigate data quality, understand operational patterns, and make evidence-informed decisions.

## 2. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Main programming language |
| PySpark | Distributed data processing, cleaning, joins, aggregations, and transformations |
| Spark SQL | Express analytical questions using SQL |
| Spark DataFrame API | Build reusable data-processing workflows |
| Window functions | Compare records across ordered groups, identify repeated scans, and rank results |
| Parquet | Store cleaned and analytical datasets in a columnar format |
| Matplotlib | Visualize selected analytical results |
| Jupyter Notebook | Run the workflow interactively and inspect results |

The notebook is configured for local Spark execution using `local[*]`, which allows Spark to use the available local processor cores.

## 3. Input Data

Place the seven source CSV files in `Data/raw/`, relative to the notebook:

```text
Festival_Data_Detective_Spark/
├── festive_analysis.ipynb
├── Data/
│   └── raw/
│       ├── attendees.csv
│       ├── ticket_scans.csv
│       ├── performances.csv
│       ├── stages.csv
│       ├── food_orders.csv
│       ├── food_stalls.csv
│       └── incidents.csv
├── output/
└── README.md
```

### Dataset descriptions

| File | Description |
|---|---|
| `attendees.csv` | Attendee profile information, including ticket type, age group, and home city |
| `ticket_scans.csv` | Ticket-scan events, including attendee, stage, timestamp, and scan status |
| `performances.csv` | Artist, stage, genre, and scheduled performance times |
| `stages.csv` | Stage names, capacities, and zones |
| `food_orders.csv` | Food-order timestamps, stalls, amounts, and order statuses |
| `food_stalls.csv` | Food-stall details, cuisine, and associated stage |
| `incidents.csv` | Reported incidents, including stage, timestamp, type, and severity |

The notebook uses explicit schemas for important fields such as identifiers, timestamps, capacities, and monetary amounts. Timestamps are expected in the format `yyyy-MM-dd HH:mm:ss`; food-order amounts use a decimal type with two fractional digits.

## 4. How the Project Works

The workflow is organized into the following stages.

### Stage 1 — Data ingestion and profiling

- Read the supplied CSV files into Spark DataFrames.
- Inspect schemas, record counts, sample rows, and data completeness.
- Examine the structure and quality of the source data before analysis.
- Use explicit schemas for important columns rather than relying entirely on inferred types.

### Stage 2 — Data standardization

- Trim leading and trailing whitespace from text fields.
- Convert empty or whitespace-only strings to null values.
- Standardize selected categorical values, including converting selected categories to lowercase.
- Parse timestamps and represent numerical fields using suitable data types.

### Stage 3 — Data-quality checks and quarantine

The project checks records against data-quality and business rules. Records that do not satisfy selected rules are separated into quarantine outputs so that questionable data remains available for inspection.

Examples of checks include:

- Exact duplicate records.
- Duplicate primary-key values.
- Missing required identifiers or selected required fields.
- Missing, negative, or otherwise invalid food-order amounts.
- Missing order timestamps or order statuses.
- Missing performance start/end times, or an end time that is not after the start time.
- Missing or non-positive stage capacity.
- Selected unmatched references between attendees, stalls, stages, scans, and related datasets.

The exact checks are defined by the notebook's implemented validation rules.

### Stage 4 — Analytical processing

PySpark DataFrame operations and Spark SQL are used to join related datasets, group records, calculate metrics, and answer operational questions. Window functions support ordered comparisons and record classification.

The main analytical areas include:

1. Valid scan activity by stage and time window.
2. Stage activity compared with listed capacity.
3. Potentially repeated ticket scans.
4. Food-stall revenue, completed-order count, and average order value.
5. Performance and genre rankings using a scan-based attendance proxy.
6. Hourly patterns in scans, food orders, and incidents.
7. Incident counts by stage.
8. Performance schedule checks, including possible overlaps.
9. Comparisons between equivalent SQL and DataFrame calculations.

### Stage 5 — Validation

The workflow includes checks intended to make results easier to audit, such as:

- Comparing raw, clean, and quarantined record counts.
- Checking key uniqueness after cleaning.
- Inspecting quarantine reasons and unmatched references.
- Checking important business rules.
- Comparing results produced through Spark SQL and the DataFrame API.
- Inspecting Spark execution plans for selected analytical operations.

### Stage 6 — Save outputs

The notebook writes cleaned datasets, quarantined records, and analytical outputs beneath the `output/` directory. Parquet is used for persisted datasets because it is a columnar format that supports typed data and efficient analytical reads.

## 5. Key Metrics

### Stage activity

Valid ticket scans are grouped by stage and relevant time periods. Activity may be compared with the stage's listed capacity to highlight stages or periods worth investigating.

A scan count or scan-based ratio should be interpreted as an **activity indicator**, not a confirmed headcount of people physically present.

### Repeated scans

The analysis looks for exact duplicate events and repeated valid scans by the same attendee at the same stage. Under the implemented rule, a valid scan occurring within **0–10 minutes** of the previous valid scan by that attendee at that stage is flagged as a potential repeat.

These records are candidates for review; the flag alone does not establish fraudulent activity.

### Food-stall performance

For food orders, the analysis uses orders whose status is `completed` to calculate:

- **Completed-order revenue:** Sum of the amounts for completed orders.
- **Completed-order count:** Number of completed orders.
- **Average order value:** Average amount among completed orders.

The results can be used to rank stalls and compare their order performance.

### Performance attendance proxy

Performances are compared using a scan-derived attendance proxy. The analysis associates valid scan activity with performance schedules and stages, where the available data supports that relationship. The resulting value is an estimate of activity associated with a performance, not a sensor-measured attendance count.

### Incidents

Incident records are grouped to examine incident counts by stage and, where implemented, by time period or other categories. These summaries can help identify patterns that merit operational review.

## 6. Assumptions and Business Rules

The following assumptions and rules guide the analysis. They are analytical choices made to produce consistent and auditable outputs.

1. **Whitespace and blank values:** Leading/trailing whitespace is removed, and empty or whitespace-only strings are converted to nulls.
2. **Category standardization:** Selected categorical fields are converted to lowercase to reduce inconsistencies caused by capitalization.
3. **Exact duplicates:** Extra copies of fully identical records are quarantined rather than silently counted as separate events.
4. **Duplicate identifiers:** Records with duplicate primary-key values are quarantined according to the notebook's validation process.
5. **Required values:** Records missing required identifiers or selected required fields are quarantined.
6. **Food-order validity:** Orders with null amounts, negative amounts, missing order times, or missing order statuses are quarantined by the current rules.
7. **Revenue definition:** Only records with `order_status = 'completed'` contribute to completed-order revenue and average order value. Cancelled, pending, and other statuses are not included in these metrics.
8. **Performance schedules:** Performance records must have start and end times, and the end time must be later than the start time.
9. **Stage capacity:** A stage must have a positive capacity to pass the capacity validation.
10. **Reference consistency:** Selected records with unmatched attendee, stall, or stage references are quarantined.
11. **Valid scans:** Only records with `scan_status = 'valid'` are used in valid-scan analyses.
12. **Potential repeat window:** Valid scans by the same attendee at the same stage within 0–10 minutes of the previous valid scan are flagged as potential repeats.
13. **Incident severity:** An incident with a missing severity can remain in the cleaned dataset if it passes the other validation checks; severity completeness can be examined separately.
14. **Attendance proxy:** Ticket-scan activity is used as a proxy for performance-related activity when the required stage and time relationships can be established.
15. **Join grain:** Event-level datasets should be aggregated to a compatible level before being joined when necessary, to avoid multiplying rows and inflating totals.
16. **Correlation and interpretation:** A relationship between scans, orders, and incidents is treated as an observed pattern, not proof that one caused another.
17. **Time interpretation:** Timestamps are interpreted according to the supplied data and the notebook's parsing rules; the analysis assumes the timestamps are sufficiently consistent for comparisons across stages and time periods.

### Important interpretation choices

- A negative food-order amount may represent a refund in some systems. Under the current rule, negative amounts are quarantined rather than netted against revenue.
- A repeated scan may be legitimate. The repeat flag is intended to support investigation, not automatically label a record as fraudulent.
- Scan-based performance activity is not equivalent to a verified count of unique people physically present unless the analysis explicitly deduplicates attendees and the data supports that interpretation.
- Incident patterns should be interpreted in the context of incident-record completeness and timestamp quality.

## 7. Requirements

Install and configure:

- Python 3
- A Java runtime compatible with the installed PySpark version
- PySpark
- Jupyter Notebook or JupyterLab
- Matplotlib for the chart cells

Install the main packages in the environment used by your notebook:

```bash
python -m pip install pyspark jupyter matplotlib
```

If using a virtual environment, activate it before installing packages and starting Jupyter. Make sure the notebook kernel uses that same environment.

Confirm Java is available:

```bash
java -version
```

Confirm PySpark is installed:

```bash
python -c "import pyspark; print(pyspark.__version__)"
```

## 8. How to Run the Project

### Step 1 — Prepare the files

Make sure the project directory contains `festive_analysis.ipynb` and the seven CSV files inside `Data/raw/`, following the structure shown above.

### Step 2 — Set up the Python environment

Open a terminal in the project directory. Create and activate a virtual environment if needed, then install the dependencies:

```bash
python -m venv .venv
```

On Windows Command Prompt:

```bat
.venv\Scripts\activate
python -m pip install pyspark jupyter matplotlib
```

On Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
python -m pip install pyspark jupyter matplotlib
```

If the project already has a working environment, use that environment instead of creating another one.

### Step 3 — Start Jupyter

```bash
jupyter notebook
```

Open `festive_analysis.ipynb` in the browser. JupyterLab can also be used if installed.

### Step 4 — Run the notebook from top to bottom

Run each cell in order so that the Spark session, input DataFrames, cleaning functions, validation outputs, analytical DataFrames, and saved outputs are created before later cells use them.

If the CSV files are stored somewhere else, update the notebook's `DATA_DIR` setting to the correct location.

### Step 5 — Review the outputs

Inspect the profiling and data-quality summaries, quarantine records, analytical tables, validation checks, and SQL/DataFrame comparisons.

The main charts can be generated after their corresponding analytical DataFrames have been created. The chart code uses Matplotlib and can collect a small, sorted result from Spark for plotting without converting the full DataFrame to Pandas.

### Step 6 — Find the generated files

Inspect the `output/` directory for the cleaned data, quarantine outputs, and analytical datasets written by the notebook. The exact subdirectories depend on the output paths configured in the notebook.

## 9. Analytical Charts

The project includes or can produce charts for key findings, including:

- **Top food stalls by revenue:** Compares completed-order revenue across stalls.
- **Incidents by stage:** Shows how reported incident counts differ across stages.
- **Top performances by attendance proxy:** Ranks performances by scan-derived activity.

Additional charts can examine stage activity against capacity, average order value, daily food revenue, genre comparisons, and hourly patterns when the corresponding analytical DataFrames are available.

Charts should be interpreted alongside their underlying tables and the assumptions used to calculate each metric.

## 10. Output and Reproducibility

The project separates data preparation from analysis so the same cleaning and aggregation rules can be rerun on the supplied inputs. Cleaned records and quarantined records are kept as distinct outputs, and analytical datasets can be persisted in Parquet for later inspection or reuse.

For a reproducible run:

- Use the same source CSV files and notebook configuration.
- Run the notebook cells in order.
- Review the data-quality summaries and validation checks.
- Recreate the analytical outputs before generating charts.
- Record the actual counts, rankings, and observations from the run when preparing a findings report.

## 11. Project Outcome

Festival Data Detective demonstrates an end-to-end PySpark analytics workflow: ingesting structured CSV data, profiling and cleaning records, applying explicit business rules, preserving questionable records for review, using joins and aggregations to answer operational questions, applying window functions, comparing Spark SQL with DataFrame calculations, and saving outputs for further analysis.

The resulting datasets and charts help festival organizers examine stage activity, food-stall performance, performance-related scan activity, repeated scans, and incident patterns using a consistent and documented analytical process.
