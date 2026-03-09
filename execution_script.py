from extract_script import extracting_csv,structure_json
from Transformation_script import data_quality_check,data_transformation
from load_script import load_to_postgres 
from pathlib import Path 

def main():
    filepaths = [Path("Data Folder") / "synthetic_data2_orders.csv"]

    for filepath in filepaths:
        ext = Path(filepath).suffix.lower()

        if ext == ".json":
            df = structure_json(filepath)
        elif ext == ".csv":
            df = extracting_csv(filepath)
        else:
            raise ValueError(f"Unsupported file format: {filepath}")
        
        print(f"processed:{filepath}")
        print(df.head(),"\n")

        quality_report = data_quality_check(df) 
        Transformed_df  =data_transformation(df) 

        table_name = "order_data_zion" 

        load_to_postgres(Transformed_df, table_name) 

if __name__  == "__main__":
    main()  

