import os
import pandas as pd
from sqlalchemy import create_engine
from loguru import logger 
from dotenv import load_dotenv 


def load_to_postgres(df, table_name):

    logger.info("Starting load process...")
     
    load_dotenv(override=True) 

    database_url = os.getenv("DATABASE_URL") 

    if not database_url:
        raise ValueError("DATABASE_URL environment variable not set")

    try:
        engine = create_engine(database_url, echo = True)

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",
            index=False,
            method="multi"
        )

        logger.info(f"Data successfully loaded into table '{table_name}'")

    except Exception as e:
        logger.exception("Failed to load data into PostgreSQL")
        raise e 

    finally:
        engine.dispose()
        logger.info("Database connection closed") 