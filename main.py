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
    Convert Azure AD access token into the format required by pyodbc's SQL_COPT_SS_ACCESS_TOKEN.
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

    date_from  = os.getenv("DATE_FROM", "")
    date_to    = os.getenv("DATE_TO", "")
    if not date_from:
        date_from = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    if not date_to:
        date_to = datetime.datetime.utcnow().strftime('%Y-%m-%d')

    otm_from   = os.getenv("OTM_FROM", "0")
    otm_to     = os.getenv("OTM_TO", "0")
    p_from     = os.getenv("PERIOD_FROM", "90")
    p_to       = os.getenv("PERIOD_TO", "90")
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

    # 2) Configure iVol API
    try:
        ivol.setLoginParams(apiKey=api_key)
        getMarketData = ivol.setMethod('/equities/eod/ivs')
        logging.info("Configured iVolatility /equities/eod/ivs.")
    except Exception as e:
        logging.error(f"Failed to configure iVol API: {e}")
        sys.exit(1)

    # 3) Acquire Azure AD token for SQL
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
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={db_server};"
        f"DATABASE={db_name};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )

    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_conn_str)}",
        connect_args={'attrs_before': attrs}
    )

    # 4) First, fetch a config row that has TickerSql + Region in the FIRST row
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
    ticker_sql     = config_df["TickerSql"].iloc[0]      # e.g. "SELECT symbol, region FROM MySymbols ..."
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

    # We now have multiple tickers (and maybe a 'region' column). We'll do parallel requests.
    symbol_records = symbol_df.to_dict("records")
    logging.info(f"Fetched {len(symbol_records)} symbols from ticker_sql.")

    # 6) Define a parameterized DELETE statement if you want to remove old rows for each ticker/date
    delete_sql = f"""
        DELETE FROM {table_name}
        WHERE [symbol] = ?
          AND [date] >= ?
          AND [date] <= ?
    """

    # 7) We'll do chunk-based insert with pyodbc, no .to_sql reflection
    insert_sql = f"""
    INSERT INTO {table_name} (
        [symbol],
        [region],
        [date],
        [Call_Put],
        [OTM],
        [IV],
        [delta],
        [record_no]
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    needed_cols = ["symbol", "region", "date", "Call_Put", "OTM", "IV", "delta", "record_no"]

    def fetch_and_insert_symbol(record):
        """
        1) DELETE old rows for (symbol, date range)
        2) Fetch from iVol
        3) rename columns
        4) chunked insert
        """
        sym = record["symbol"]
        reg = record.get("region", default_region)

        # A) DELETE
        try:
            with pyodbc.connect(odbc_conn_str, attrs_before=attrs) as conn:
                cur = conn.cursor()
                cur.execute(delete_sql, [sym, date_from, date_to])
                conn.commit()
        except Exception as dex:
            logging.error(f"Delete failed for {sym}: {dex}")
            return 0

        # B) Fetch from iVol
        try:
            data_df = getMarketData(
                symbol=sym,
                from_=date_from,
                to=date_to,
                region=reg,
                OTMFrom=int(otm_from),
                OTMTo=int(otm_to),
                periodFrom=int(p_from),
                periodTo=int(p_to)
            )
        except Exception as fex:
            logging.error(f"Fetch error for symbol={sym}: {fex}")
            return 0

        if data_df.empty:
            logging.info(f"No data for {sym}, region={reg}, date range={date_from}->{date_to}.")
            return 0

        # rename columns
        rename_map = {
            "Call/Put": "Call_Put",
            "out-of-the-money %": "OTM"
        }
        data_df.rename(columns=rename_map, inplace=True)

        # Ensure columns
        data_df["symbol"] = sym
        data_df["region"] = reg

        if "record_no" not in data_df.columns:
            # generate unique int
            data_df["record_no"] = [
                abs(hash(uuid.uuid4())) % 2147483647
                for _ in range(len(data_df))
            ]

        # fill needed_cols if missing
        for col in needed_cols:
            if col not in data_df.columns:
                data_df[col] = None

        # C) chunked insert with pyodbc
        chunk_size = 5000
        inserted_count = 0

        for start_idx in range(0, len(data_df), chunk_size):
            subset = data_df.iloc[start_idx : start_idx + chunk_size]
            data_tuples = list(subset[needed_cols].itertuples(index=False, name=None))

            try:
                with pyodbc.connect(odbc_conn_str, attrs_before=attrs) as conn:
                    cur = conn.cursor()
                    cur.fast_executemany = True
                    cur.executemany(insert_sql, data_tuples)
                    conn.commit()
                inserted_count += len(data_tuples)
            except Exception as iex:
                logging.error(f"Insert chunk failed for {sym}, chunk start={start_idx}: {iex}")
                return inserted_count

        return inserted_count

    # 8) Multithreaded across all symbols
    total_inserted = 0
    logging.info(f"Starting parallel fetch/insert for {len(symbol_records)} symbols. max_workers={max_workers}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(fetch_and_insert_symbol, r): r for r in symbol_records}
        for future in as_completed(future_map):
            rec = future_map[future]
            sym = rec["symbol"]
            try:
                cnt = future.result()
                total_inserted += cnt
                logging.info(f"Symbol {sym}: inserted {cnt} rows. (Running total={total_inserted})")
            except Exception as exc:
                logging.error(f"Symbol {sym} failed concurrency: {exc}")

    logging.info(f"All symbols processed. Total inserted: {total_inserted}.")
    logging.info("ETL job completed successfully.")

if __name__ == "__main__":
    main()


