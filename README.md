# End-to-End ETL Pipeline + Customer Analytics Dashboard

I built this project to practice what a real data engineering workflow looks like — not just writing scripts, but thinking about the full journey data takes from a raw file to something a business can actually use.

## What It Does

The pipeline takes raw customer and order data, cleans it up, loads it into a cloud PostgreSQL database, runs customer behavior analysis on it, and displays everything on a live interactive dashboard.

```
Raw Data → Extract → Transform → Load → Analyze → Dashboard
```
## The Files and What They Do

- **`extract_script.py`** — reads data from CSV files, local JSON, or a live API. I built it to handle all three so it doesn't break if the data source changes
- **`Transformation_script.py`** — checks data quality (nulls, duplicates), cleans column names, fixes data types, and normalizes missing values
- **`load_script.py`** — pushes the cleaned data to PostgreSQL on Render using SQLAlchemy. Credentials come from an environment variable, never hardcoded
- **`execution_script.py`** — the master script that runs the entire pipeline in one command
- **`analysis.py`** — merges the customers and orders tables and builds customer profiles: purchase frequency, recency, churn risk, segmentation, and demographic breakdowns
- **`app.py`** — the Streamlit dashboard that makes all the analysis interactive and visual

## Dashboard Features

- Key metrics (total customers, revenue, churn rate, average order value)
- Customer segmentation by purchase frequency (Loyal, Regular, Occasional, One-time)
- Churn risk analysis — customers inactive for 90+ days are flagged
- Revenue breakdown by segment
- Demographic insights by state and gender
- Recency vs spending scatter plot
- Sidebar filters for real-time exploration
- 
## Tech Stack

Python · Pandas · PostgreSQL · SQLAlchemy · Streamlit · Plotly · Loguru · psycopg2 · python-dotenv

## How to Run It

```bash
# Install dependencies
pip install -r requirements.txt

# Add your database URL to a .env file
DATABASE_URL=postgresql://user:password@host/db?sslmode=require

# Run the pipeline
python execution_script.py

# Launch the dashboard
streamlit run app.py
```

## A Few Things I'm Proud Of

The extract script handles CSV, JSON, and API sources with the same function — you just pass a flag. The transformation pipeline is modular so each step is isolated and easy to swap out. And the whole thing is deployed: the pipeline loads to a live cloud database and the dashboard runs on Streamlit Cloud.
