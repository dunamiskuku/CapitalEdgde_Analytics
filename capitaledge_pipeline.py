# CapitalEdge forex pipeline
# ETL script to extract forex data from an API, transform it, and load it into a database

# Import necessary libraries
import requests
import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine, engine
from dotenv import load_dotenv
import os
import logging

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
    
def run_pipeline():
    logger.info("pipeline started")
    records = extract_forex_data(pairs)
    df = transform_forex_data(records)
    load(df)

    df.to_csv('forex_prices.csv', index=False)
    print("Data has been successfully saved to CSV file.")

#if __name__ == "__main__":
    run_pipeline()


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

    records = extract_forex_data(pairs)
    df = transform_forex_data(records)

    watermarks = get_last_loaded_dates(engine)
    df_new = filter_incremental(df, watermarks)

    if df_new.empty:
        logger.info("No new rows to load. Pipeline complete.")
        print("No new data — database already up to date.")
        return

    load(df_new)

    df_new.to_csv('forex_prices.csv', index=False)
    print(f"Incremental load complete: {len(df_new)} new rows saved.")


if __name__ == "__main__":
    run_pipeline_incremental()  
