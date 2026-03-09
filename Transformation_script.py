import pandas as pd
from loguru import logger


logger.info("Starting Transformation module...")

def data_quality_check(df):
    logger.info("Running data quality check...")

    report = {
        "total_rows": len(df),
        "total_columns": len(df.columns),
        "null_counts": df.isnull().sum().to_dict(),
        "duplicate_rows": df.duplicated().sum(),
        "column_types": df.dtypes.astype(str).to_dict()
    }

    total_nulls = sum(report["null_counts"].values())

    if total_nulls > 0:
        logger.warning(f"Total null values found: {total_nulls}")
    else:
        logger.info("No null values found")

    if report["duplicate_rows"] > 0:
        logger.warning(f"{report['duplicate_rows']} duplicate rows found")
    else:
        logger.info("No duplicate rows found")

    logger.info("Data quality check completed")
    return report

def normalize_missing_values(df):
    logger.info("Normalizing missing values...")

    missing_patterns = ["", " ", "NA", "N/A", "null", "None", "none", "-", "--"]

    df = df.replace(missing_patterns, pd.NA)

    logger.info("Finished normalizing missing values")

    return df

def clean_column_names(df):
    logger.info("Cleaning column names...")

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^\w]", "", regex=True)
    )

    return df

def clean_string_columns(df):
    logger.info("Cleaning string columns...")

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype("string").str.strip()

    return df

def smart_type_inference(df):
    logger.info("Running smart type inference...")

    for col in df.columns:

        if df[col].dtype == "object" or df[col].dtype.name == "string": 
            converted_numeric = pd.to_numeric(df[col], errors="coerce")

            non_null_ratio_1 = converted_numeric.notna().mean()

            if non_null_ratio_1 > 0.8:
                df[col] = converted_numeric 
                logger.info(f"Column '{col}' converted to numeric")

        if df[col].dtype == "object" or df[col].dtype.name == "string":
            converted_datetime = pd.to_datetime(df[col], errors="coerce")
            non_null_ratio_2 = converted_datetime.notna().mean() 

            if non_null_ratio_2 > 0.8: 
                df[col] = converted_datetime
                logger.info(f"Column '{col}' converted to datetime") 

        if df[col].dtype == "object":
            unique_vals = df[col].dropna().str.lower().unique()

            if set(unique_vals).issubset({"true", "false", "yes", "no", "1", "0"}):
                df[col] = df[col].str.lower().map({
                    "true": True,
                    "false": False,
                    "yes": True,
                    "no": False,
                    "1": True,
                    "0": False
                })
                logger.info(f"Column '{col}' converted to boolean")

    return df

def handle_missing_values(df):
    logger.info("Handling missing values...")

    for col in df.select_dtypes(include=["number"]).columns:
        if df[col].isnull().sum() > 0:
            df[col] = df[col].fillna(df[col].median())

    for col in df.select_dtypes(include=["string"]).columns:
        if df[col].isnull().sum() > 0:
            df[col] = df[col].fillna(df[col].mode().iloc[0]) 

    return df

def remove_duplicates(df):
    before = len(df)
    df = df.drop_duplicates() 
    after = len(df) 

    if before != after:
        logger.info(f"{before - after} duplicate rows removed")

    return df 


def data_transformation(data):
    logger.info("Starting data transformation pipeline...")

    df = data.copy()

    if isinstance(df.iloc[0], dict):
        df = pd.json_normalize(df)

    df = clean_column_names(df)
    df = normalize_missing_values(df)
    df = clean_string_columns(df)
    df = smart_type_inference(df)
    df = handle_missing_values(df)
    df = remove_duplicates(df)

    df = df.reset_index(drop=True)

    logger.info("Transformation completed successfully")
    logger.info(f"Final shape: {df.shape}")
    logger.info(f"Final dtypes:\n{df.dtypes}")

    return df