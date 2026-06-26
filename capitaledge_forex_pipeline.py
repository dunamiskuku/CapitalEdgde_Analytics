# CapitalEdge forex pipeline
# ETL: extract from API -> stage in Azure -> transform -> incrementally load into Postgres

# Import necessary libraries
import requests
import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine, engine
from dotenv import load_dotenv
import os
import io                         
import json                       
import logging

# AZURE: the Azure Data Lake client (pip install azure-storage-file-datalake)
from azure.storage.filedatalake import DataLakeServiceClient

# set up logging
# 4 leveles: DEBUG, INFO, WARNING, ERROR
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# DB configuration
load_dotenv()
API_KEY = os.getenv('API_KEY')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

# AZURE: storage account details
ADLS_ACCOUNT_NAME = os.getenv('ADLS_ACCOUNT_NAME')
ADLS_ACCOUNT_KEY = os.getenv('ADLS_ACCOUNT_KEY')  
ADLS_FILE_SYSTEM = os.getenv('ADLS_FILE_SYSTEM')

# Define the currency pairs to extract
pairs = [
    ("EUR", "USD"),
    ("GBP", "USD"),
    ("USD", "JPY"),
    ("AUD", "USD"),
    ("USD", "CAD"),
]


def extract_forex_data(pairs):
    all_records = []

    for from_symbol, to_symbol in pairs:
        pair = f"{from_symbol}{to_symbol}"

        try:
            url = (
                f"https://www.alphavantage.co/query?"
                f"function=FX_DAILY"
                f"&from_symbol={from_symbol}"
                f"&to_symbol={to_symbol}"
                f"&apikey={API_KEY}"
            )

            response = requests.get(url)
            response.raise_for_status()

            data = response.json()

            if "Time Series FX (Daily)" not in data:
                logger.warning(
                    f"No data returned for {pair}. Response: {data}"
                )
                continue

            ts = data["Time Series FX (Daily)"]

            pair_records = []

            for trade_date, values in ts.items():

                pair_records.append({
                    "symbol": pair,
                    "trade_date": trade_date,
                    "open": values["1. open"],
                    "high": values["2. high"],
                    "low": values["3. low"],
                    "close": values["4. close"]
                })

            all_records.extend(pair_records)

            logger.info(
                f"Extracted data for {pair}: {len(pair_records)} rows"
            )

        except requests.exceptions.RequestException as e:
            logger.error(
                f"API request failed for {pair}: {e}",
                exc_info=True
            )

        except Exception as e:
            logger.error(
                f"Unexpected error extracting {pair}: {e}",
                exc_info=True
            )

    logger.info(
        f"Total records extracted: {len(all_records)}"
    )

    return all_records


# AZURE: Data Lake functions
def get_datalake_client():
    # Connect to your Azure Data Lake storage account
    return DataLakeServiceClient(
        account_url=f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
        credential=ADLS_ACCOUNT_KEY,
    )


def upload_to_azure(path, data_bytes):
    # Write bytes to a file at `path` inside the container. Overwrites if it exists.
    service = get_datalake_client()
    file_system = service.get_file_system_client(ADLS_FILE_SYSTEM)
    file_client = file_system.get_file_client(path)
    file_client.upload_data(data_bytes, overwrite=True)
    logger.info(f"Uploaded to Azure: {ADLS_FILE_SYSTEM}/{path} ({len(data_bytes)} bytes)")


def download_from_azure(path):
    # Read a file back from `path` inside the container and return its bytes.
    service = get_datalake_client()
    file_system = service.get_file_system_client(ADLS_FILE_SYSTEM)
    file_client = file_system.get_file_client(path)
    return file_client.download_file().readall()


def stage_raw_to_azure(records):
    # Save raw extracted records as JSON in the raw zone. Returns the file path.
    today = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    path = f"raw/forex/dt={today}/raw_forex.json"
    upload_to_azure(path, json.dumps(records).encode("utf-8"))
    return path


def read_raw_from_azure(path):
    # Read the raw JSON back out of the raw zone as a Python list.
    return json.loads(download_from_azure(path).decode("utf-8"))


def stage_clean_to_azure(df):
    # Save the cleaned dataframe as parquet in the staged zone. Returns the file path.
    today = pd.Timestamp.utcnow().strftime("%Y-%m-%d")
    path = f"staged/forex/dt={today}/forex.parquet"
    buffer = io.BytesIO()            # an in-memory file
    df.to_parquet(buffer, index=False)
    upload_to_azure(path, buffer.getvalue())
    return path


def read_clean_from_azure(path):
    # Read the cleaned parquet back out of the staged zone as a dataframe.
    return pd.read_parquet(io.BytesIO(download_from_azure(path)))



# Transform the data
def transform_forex_data(records):
    try:
        if not records:
            raise ValueError("No records received for transformation.")

        # convert to dataframe
        df = pd.DataFrame(records)

        df = df.rename(columns={
            "trade_date": "datetime"
        })
        timezone = "UTC"

        # convert columns to correct data types
        df['datetime'] = pd.to_datetime(df['datetime'], utc=True)

        # convert everything else
        df = df.astype({
            'open': 'float',
            'high': 'float',
            'low': 'float',
            'close': 'float'
        })

        logger.info(f"transformed done: {len(df)} rows")
        return df

    except KeyError as e:
        logger.error(
            f"Missing required column: {e}",
            exc_info=True
        )
        raise

    except ValueError as e:
        logger.error(
            f"Data type conversion error: {e}",
            exc_info=True
        )
        raise

    except Exception as e:
        logger.error(
            f"Unexpected transformation error: {e}",
            exc_info=True
        )
        raise


# Loading
def load(df):
    engine = None

    try:
        db_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

        # create sqlalchemy engine
        engine = create_engine(db_url)

        # load dataframe to sql table
        df.to_sql('forex_prices', engine, if_exists='append', index=False)

        logger.info(f"data loaded to database: {len(df)} rows")

    except Exception as e:
        logger.error(
            f"Database load failed: {e}",
            exc_info=True
        )
        raise
    finally:
        if engine is not None:
            engine.dispose()


# Incremental pipeline
def get_last_loaded_dates(engine):

    query = (
        "SELECT symbol, MAX(datetime) AS last_loaded "
        "FROM forex_prices GROUP BY symbol"
    )
    try:
        watermarks = pd.read_sql(query, engine)
        if watermarks.empty:
            return {}

        watermarks['last_loaded'] = pd.to_datetime(
            watermarks['last_loaded'], utc=True
        )
        result = dict(zip(watermarks['symbol'], watermarks['last_loaded']))
        logger.info(f"Existing watermarks: {result}")
        return result

    except Exception as e:
        logger.warning(
            f"Could not read watermarks (treating all rows as new): {e}"
        )
        return {}


def filter_incremental(df, watermarks):

    if df.empty or not watermarks:
        return df

    row_watermark = df['symbol'].map(watermarks)
    keep_mask = row_watermark.isna() | (df['datetime'] > row_watermark)

    df_new = df[keep_mask].copy()
    logger.info(
        f"Incremental filter: kept {len(df_new)} of {len(df)} fetched rows"
    )
    return df_new


def run_pipeline_incremental():
    logger.info("incremental pipeline started")

    db_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(db_url)

    # 1. EXTRACT — pull the data from the Alpha Vantage API
    records = extract_forex_data(pairs)

    # 2. STAGE IN AZURE (raw) — save the untouched API data, read it back        
    raw_path = stage_raw_to_azure(records)                                       
    records = read_raw_from_azure(raw_path)                                      

    # 3. TRANSFORM — clean and type the data
    df = transform_forex_data(records)

    # 4. STAGE IN AZURE (clean) — save the cleaned dataframe, read it back        # >>> AZURE
    clean_path = stage_clean_to_azure(df)                                        # >>> AZURE
    df = read_clean_from_azure(clean_path)                                       # >>> AZURE

    # 5. INCREMENTAL LOAD — keep only rows newer than what's already in the DB
    watermarks = get_last_loaded_dates(engine)
    df_new = filter_incremental(df, watermarks)

    if df_new.empty:
        logger.info("No new rows to load. Pipeline complete.")
        print("No new data — database already up to date.")
        return

    load(df_new)

    df_new.to_csv('forex_prices.csv', index=False)
    print(f"Incremental load complete: {len(df_new)} new rows saved.")


# AIRFLOW DAG
try:
    from airflow.decorators import dag, task   # Airflow 2.x
    from datetime import datetime
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


if AIRFLOW_AVAILABLE:

    @dag(
        dag_id="capitaledge_forex_pipeline",
        schedule="0 6 * * 1-5",                     # run 06:00 every weekday
        start_date=datetime(2026, 6, 27),
        catchup=False,
        tags=["capitaledge", "forex", "azure"],
    )
    def capitaledge_forex_pipeline():

        @task()
        def extract():
            # Step 1 — calls your extract_forex_data()
            return extract_forex_data(pairs)

        @task()
        def stage_raw(records):
            # Step 2 — calls your stage_raw_to_azure(), returns the Azure file path
            return stage_raw_to_azure(records)

        @task()
        def transform(raw_path):
            # Step 3 — read raw from Azure, transform, save clean to Azure
            records = read_raw_from_azure(raw_path)
            df = transform_forex_data(records)
            return stage_clean_to_azure(df)

        @task()
        def incremental_load(clean_path):
            # Step 4 + 5 — read clean from Azure, filter to new rows, load to Postgres
            df = read_clean_from_azure(clean_path)

            db_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
            engine = create_engine(db_url)
            watermarks = get_last_loaded_dates(engine)
            engine.dispose()

            df_new = filter_incremental(df, watermarks)
            if df_new.empty:
                logger.info("No new rows to load.")
                return
            load(df_new)

     
        # extract -> stage_raw -> transform -> incremental_load
        raw_path = stage_raw(extract())
        clean_path = transform(raw_path)
        incremental_load(clean_path) 

    # Register the DAG with Airflow
    capitaledge_forex_pipeline()


# Run as a normal Python script
if __name__ == "__main__":
    run_pipeline_incremental()
