import pandas as pd
import numpy as np
from sqlalchemy import MetaData, Table, create_engine, select
from datetime import datetime, timezone 
from dotenv import load_dotenv 
import os 


def get_db_engine():
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")

    if not DATABASE_URL:
        raise ValueError("DATABASE_URL not found in environmental variables")
    
    return create_engine(DATABASE_URL)  


def fetch_table_data(table_name):
    engine = get_db_engine()
    metadata = MetaData()

    table = Table(table_name, metadata, autoload_with=engine)
    stmt = select(table) 

    return pd.read_sql(stmt, engine)

def analyze_data(df):
    report = {}

    report["analysis_timestamp"] = datetime.now(timezone.utc).isoformat()
    report["row_count"] = int(len(df))
    report["column_count"] = int(len(df.columns))
    report["duplicate_rows"] = int(df.duplicated().sum())

    null_counts = df.isnull().sum()
    null_percentages = (null_counts / len(df) * 100).round(2)
    report["null_counts"] = {col: int(val) for col, val in null_counts.items()}
    report["null_percentages"] = {col: float(val) for col, val in null_percentages.items()}

    report["dtypes"] = df.dtypes.astype(str).to_dict()

    numeric_cols = df.select_dtypes(include=["number"])
    if not numeric_cols.empty:
        numeric_summary = numeric_cols.describe().to_dict()
        numeric_summary_py = {}
        for col, stats in numeric_summary.items():
            numeric_summary_py[col] = {k: float(v) for k, v in stats.items()}
        report["numeric_summary"] = numeric_summary_py

        skew = numeric_cols.skew()
        report["skewness"] = {col: float(val) for col, val in skew.items()}

        corr = numeric_cols.corr()
        report["correlation_matrix"] = {col: {k: float(v) for k, v in corr[col].items()} for col in corr.columns}

    categorical_cols = df.select_dtypes(include=["object", "string"])
    if not categorical_cols.empty:
        cardinality = {col: int(df[col].nunique()) for col in categorical_cols.columns}
        top_values = {col: {k: int(v) for k, v in df[col].value_counts(dropna=False).head(5).to_dict().items()} 
                      for col in categorical_cols.columns}
        report["categorical_cardinality"] = cardinality
        report["top_5_values"] = top_values

    boolean_cols = df.select_dtypes(include=["bool"])
    if not boolean_cols.empty:
        report["boolean_summary"] = {
            col: {k: int(v) for k, v in df[col].value_counts(dropna=False).to_dict().items()}
            for col in boolean_cols.columns
        }

    datetime_cols = df.select_dtypes(include=["datetime64[ns]"])
    if not datetime_cols.empty:
        freshness = {col: {"min_date": str(df[col].min()), "max_date": str(df[col].max())} 
                     for col in datetime_cols.columns}
        report["date_range"] = freshness

    total_cells = df.shape[0] * df.shape[1]
    total_missing = df.isnull().sum().sum()
    report["data_quality_score_percent"] = round(100 - (total_missing / total_cells * 100), 2)

    return report 

def analyze_customer_behavior(df, customer_id_col="customer_id", 
                              purchase_date_col="purchase_date", 
                              amount_col="amount"):
    analysis_result = {}
    
    analysis_result["analysis_timestamp"] = datetime.now(timezone.utc).isoformat()
    analysis_result["total_customers"] = df[customer_id_col].nunique()
    
    if purchase_date_col in df.columns:
        df[purchase_date_col] = pd.to_datetime(df[purchase_date_col], errors='coerce')
    
    customer_metrics = df.groupby(customer_id_col).agg(
        first_purchase=(purchase_date_col, 'min') if purchase_date_col in df.columns else (amount_col, 'min'),
        last_purchase=(purchase_date_col, 'max') if purchase_date_col in df.columns else (amount_col, 'max'),
        total_purchases=(purchase_date_col, 'count') if purchase_date_col in df.columns else (amount_col, 'count'),
        total_spent=(amount_col, 'sum') if amount_col in df.columns else (amount_col, 'sum')
    ).reset_index()
    
    if purchase_date_col in df.columns:
        customer_metrics["tenure_days"] = (
            customer_metrics["last_purchase"] - customer_metrics["first_purchase"]
        ).dt.days
    
    freq_bins = [0, 1, 3, 6, 12, np.inf]
    freq_labels = ["1", "2-3", "4-6", "7-12", "12+"]
    customer_metrics["purchase_frequency_group"] = pd.cut(
        customer_metrics["total_purchases"],
        bins=freq_bins,
        labels=freq_labels,
        right=True
    )
    
    if purchase_date_col in df.columns:
        today = pd.Timestamp.now()
        customer_metrics["days_since_last_purchase"] = (
            today - customer_metrics["last_purchase"]
        ).dt.days
        customer_metrics["churned"] = (
            customer_metrics["days_since_last_purchase"] > 90
        )
    
    demo_cols = df.select_dtypes(include=["object", "string"]).columns.tolist()
    demo_cols = [col for col in demo_cols if col != customer_id_col]
    
    demographic_summary = {}
    for col in demo_cols:
        demographic_summary[col] = df[col].value_counts(dropna=False).to_dict()
    
    analysis_result["customer_metrics"] = customer_metrics.to_dict(orient="records")
    analysis_result["demographic_summary"] = demographic_summary
    
    numeric_cols = customer_metrics.select_dtypes(include=["number"]).columns.tolist()
    if numeric_cols:
        corr_matrix = customer_metrics[numeric_cols].corr()
        analysis_result["numeric_correlations"] = {
            col: {k: float(v) for k, v in corr_matrix[col].items()}
            for col in corr_matrix.columns
        }
    
    return analysis_result


def run_analysis(table_name, analysis_type="general",
                 customer_id_col="customer_id", 
                 purchase_date_col="purchase_date", 
                 amount_col="amount"):
    df = fetch_table_data(table_name)
    if df.empty:
        return {"message": "Table is empty"}
    
    if analysis_type == "general":
        return analyze_data(df)
    elif analysis_type == "customer":
        return analyze_customer_behavior(
            df,
            customer_id_col=customer_id_col,
            purchase_date_col=purchase_date_col,
            amount_col=amount_col
        )
    else:
        return {"message": "Invalid analysis_type. Use 'general' or 'customer'."}


if __name__ == "__main__":
    print(run_analysis("your_table_name_here"))