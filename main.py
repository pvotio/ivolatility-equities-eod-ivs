import os
import sys
import struct
import logging
import datetime
import uuid
import random

import pyodbc
import pandas as pd
from azure.identity import DefaultAzureCredential
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

import ivolatility as ivol

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_pyodbc_attrs(access_token: str) -> dict:
    """
    Format the Azure AD access token for pyodbc's SQL_COPT_SS_ACCESS_TOKEN.
    """
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    enc_token = access_token.encode('utf-16-le')
    token_struct = struct.pack('=i', len(enc_token)) + enc_token
    return {SQL_COPT_SS_ACCESS_TOKEN: token_struct}

def main():
    # 1) Read environment variables
    api_key    = os.getenv("IVOL_API_KEY", "")
    db_server  = os.getenv("DB_SERVER", "")
    db_name    = os.getenv("DB_NAME", "")
    table_name = os.getenv("TARGET_TABLE", "")

    # Date range & other parameters for iVol
    date_from  = os.getenv("DATE_FROM", "")
    date_to    = os.getenv("DATE_TO", "")
    if not date_from:
        date_from = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    if not date_to:
        date_to = datetime.datetime.utcnow().strftime('%Y-%m-%d')

    otm_from   = os.getenv("OTM_FROM", "0")
    otm_to     = os.getenv("OTM_TO", "0")
    # region_def = os.getenv("REGION", "USA")  # <-- We won't read region from ENV anymore
    p_from     = os.getenv("PERIOD_FROM", "90")
    p_to       = os.getenv("PERIOD_TO", "90")

    # Concurrency
    max_workers = int(os.getenv("MAX_WORKERS", "12"))

    # Basic checks
    if not api_key:
        logging.error("IVOL_API_KEY is not set. Exiting.")
        sys.exit(1)
    if not db_server or not db_name:
        logging.error("DB_SERVER or DB_NAME not set. Exiting.")
        sys.exit(1)
    if not date_from or not date_to:
        logging.error("DATE_FROM and DATE_TO must be set. Exiting.")
        sys.exit(1)

    logging.info(f"Fetching iVol IVS data from={date_from}, to={date_to}")
    logging.info(f"Target table: {table_name}")

    # 2) Configure IVOL API
    try:
        ivol.setLoginParams(apiKey=api_key)
        getMarketData = ivol.setMethod('/equities/eod/ivs')
    except Exception as e:
        logging.error(f"Failed to configure iVol API: {e}")
        sys.exit(1)

    # 3) Acquire Azure AD token
    logging.info("Acquiring Azure SQL token with DefaultAzureCredential...")
    try:
        credential = DefaultAzureCredential()
        token_obj = credential.get_token("https://database.windows.net/.default")
        access_token = token_obj.token
        logging.info("Successfully obtained Azure SQL token.")
    except Exception as ex:
        logging.error(f"Failed to obtain Azure SQL token: {ex}")
        sys.exit(1)

    attrs = get_pyodbc_attrs(access_token)
    odbc_conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={db_server};"
        f"DATABASE={db_name};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )

    # Create an engine
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_conn_str)}",
        connect_args={'attrs_before': attrs}
    )

    # 4) Fetch config row that has TickerSql + Region in FIRST ROW
    #    Example: we pick top 100, but only use the first row
    config_sql = """
        SELECT TOP 100 Stock_ticker AS TickerSql,
               region AS DefaultRegion
        FROM etl.ivolatility_underlying_info
        WHERE Status = 'Active'
    """
    logging.info(f"Fetching config: {config_sql.strip()}")
    try:
        config_df = pd.read_sql(config_sql, engine)
    except Exception as ex:
        logging.error(f"Failed to query config: {ex}")
        sys.exit(1)

    if config_df.empty:
        logging.error("No config row found. Exiting.")
        sys.exit(1)

    # Use only the FIRST ROW's columns
    ticker_sql     = config_df["TickerSql"].iloc[0]      # e.g. "SELECT symbol, region FROM ..."
    default_region = config_df["DefaultRegion"].iloc[0]  # e.g. "USA"

    logging.info(f"Using TICKER_SQL={ticker_sql}")
    logging.info(f"Using default_region={default_region}")

    # 5) Fetch the list of symbols from Azure SQL using ticker_sql
    logging.info(f"Fetching symbols via: {ticker_sql}")
    try:
        symbol_df = pd.read_sql(ticker_sql, engine)
    except Exception as ex:
        logging.error(f"Failed to run ticker_sql: {ex}")
        sys.exit(1)

    if symbol_df.empty:
        logging.warning("No symbols found from TICKER_SQL. Exiting.")
        return

    if "symbol" not in symbol_df.columns:
        logging.error("Your ticker_sql must return a 'symbol' column. Exiting.")
        sys.exit(1)

    symbol_records = symbol_df.to_dict("records")
    logging.info(f"Fetched {len(symbol_records)} symbols from ticker_sql.")

    # 6) Build DELETE statement to remove existing data for a symbol in date range
    delete_sql = f"""
        DELETE FROM {table_name}
    """

    def fetch_and_insert_symbol(sym_record):
        """
        For a single symbol:
          1) DELETE existing rows in the date range
          2) FETCH new data from iVol
          3) RENAME columns (if needed)
          4) INSERT into SQL
        """
        sym = sym_record["symbol"]
        # region might come from the DB row or fallback to default_region
        reg = sym_record.get("region", default_region)

        # (A) DELETE
        try:
            with pyodbc.connect(odbc_conn_str, attrs_before=attrs) as conn:
                cur = conn.cursor()
                cur.execute(delete_sql, [sym, date_from, date_to])
                conn.commit()
        except Exception as ex:
            logging.error(f"Failed to delete rows for symbol={sym}: {ex}")
            return 0

        # (B) FETCH from iVol
        try:
            data_df = getMarketData(
                symbol=sym,
                from_=date_from,
                to=date_to,
                OTMFrom=int(otm_from),
                OTMTo=int(otm_to),
                region=reg,
                periodFrom=int(p_from),
                periodTo=int(p_to)
            )
        except Exception as e:
            logging.error(f"Error fetching symbol={sym} from IVOL: {e}")
            return 0

        if data_df.empty:
            logging.info(f"No data returned for symbol={sym} in [{date_from}->{date_to}].")
            return 0

        # (C) Rename columns (if needed)
        rename_map = {
            "Call/Put": "Call_Put",
            "out-of-the-money %": "OTM"
        }
        data_df.rename(columns=rename_map, inplace=True)

        # If 'record_no' is not provided, we must generate unique IDs
        if "record_no" not in data_df.columns:
            data_df["record_no"] = [
                abs(hash(uuid.uuid4())) % 2147483647
                for _ in range(len(data_df))
            ]

        # Fill in missing columns
        if "symbol" not in data_df.columns:
            data_df["symbol"] = sym
        if "exchange" not in data_df.columns:
            data_df["exchange"] = None   # or perhaps region, if relevant

        inserted_count = len(data_df)
        # (D) Insert into SQL using pandas to_sql, chunk-based or direct:
        try:
            data_df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        except Exception as e:
            logging.error(f"Failed inserting data for symbol={sym}: {e}")
            return 0

        return inserted_count

    # 7) Multithreaded execution
    total_inserted = 0
    logging.info(f"Starting parallel fetch/insert for {len(symbol_records)} symbols. max_workers={max_workers}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(fetch_and_insert_symbol, r): r for r in symbol_records}
        for future in as_completed(future_map):
            rec = future_map[future]
            sym = rec["symbol"]
            try:
                inserted = future.result()
                total_inserted += inserted
                logging.info(f"Symbol {sym}: Inserted {inserted} row(s). (Running total: {total_inserted})")
            except Exception as exc:
                logging.error(f"Symbol {sym} failed: {exc}")

    logging.info(f"All symbols processed. Total rows inserted: {total_inserted}.")
    logging.info("ETL job completed successfully.")

if __name__ == "__main__":
    main()

