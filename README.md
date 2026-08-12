NYC Taxi Data Engineering Pipeline

A production-style data engineering project built with PySpark, Python, and Streamlit to process NYC taxi trip data through Bronze, Silver, and Gold layers and present analytics through an interactive dashboard.

Tech Stack
Python
PySpark
Pandas
PyArrow
Streamlit
Plotly
Pytest
Parquet
Pipeline Architecture
Raw CSV
   ↓
Bronze Layer
   ↓
Silver Layer
   ↓
Gold Layer
   ├── Daily Metrics
   ├── Hourly Metrics
   └── Vendor Metrics
   ↓
Streamlit Dashboard

Project Structure
data-engineering-pipeline/
│
├── dashboard/
│   └── app.py
│
├── src/
│   ├── data_quality.py
│   ├── pipeline.py
│   ├── schemas.py
│   ├── spark_session.py
│   ├── transformations.py
│   └── __init__.py
│
├── tests/
│   ├── test_data_quality.py
│   ├── test_pipeline.py
│   └── test_transformations.py
│
├── data/
│   ├── raw/
│   └── processed/
│       ├── bronze/
│       ├── silver/
│       └── gold/
│           ├── daily_metrics/
│           ├── hourly_metrics/
│           └── vendor_metrics/
│
├── requirements.txt
├── .gitignore
└── README.md

Gold Layer

The Gold layer contains the analytics-ready datasets:

Daily Metrics
pickup_date
total_trips
total_passengers
avg_trip_duration_sec
avg_trip_distance_km
avg_trip_speed_kmh
Hourly Metrics
pickup_date
pickup_hour
total_trips
total_passengers
avg_trip_duration_sec
avg_trip_distance_km
avg_trip_speed_kmh
Vendor Metrics
vendor_id
total_trips
total_passengers
avg_trip_duration_sec
avg_trip_distance_km
avg_trip_speed_kmh
Data Quality

The project includes automated validation for:

Required columns
Null values
Invalid pickup hours
Invalid vendor IDs
Business rules
Derived metrics
Gold-layer metrics
Run the Pipeline

Activate the virtual environment:

.\.venv\Scripts\Activate.ps1


Run the pipeline:

python -m src.pipeline

Run Tests

Run the complete test suite:

pytest -v


All pipeline, data-quality, and transformation tests should pass before deployment.

Run the Dashboard

Start Streamlit:

streamlit run dashboard/app.py


The dashboard provides:

Interactive date filtering
Vendor filtering
Trip volume analysis
Daily trends
Hourly analysis
Vendor analysis
Gold-layer data exploration
KPI summaries
Interactive Plotly visualizations
Dashboard Data Sources

The dashboard reads analytics data from:

data/processed/gold/daily_metrics
data/processed/gold/hourly_metrics
data/processed/gold/vendor_metrics

GitHub

Before pushing the project, make sure local environments and raw data are excluded through .gitignore.

Important exclusions include:

.venv/
__pycache__/
.pytest_cache/
data/raw/
*.pyc

Deployment

The Streamlit dashboard can be deployed using a cloud hosting service that supports Streamlit applications.

The deployment entry point is:

dashboard/app.py


The deployed environment must install the dependencies from:

requirements.txt

Project Status

Complete

PySpark ETL pipeline
Bronze/Silver/Gold architecture
Data quality validation
Automated tests
Gold analytics datasets
Interactive Streamlit dashboard
GitHub-ready project structure