import os
import sys
import struct
import logging
import datetime
import uuid
import time

import pyodbc
import pandas as pd
from azure.identity import DefaultAzureCredential
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

import ivolatility as ivol

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def get_pyodbc_attrs(access_token: str) -> dict:
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    enc_token = access_token.encode('utf-16-le')
    token_struct = struct.pack('=i', len(enc_token)) + enc_token
    return {SQL_COPT_SS_ACCESS_TOKEN: token_struct}

def main():
    # 1) Read environment variables
    api_key    = os.getenv("IVOL_API_KEY", "")
    db_server  = os.getenv("DB_SERVER", "")
    db_name    = os.getenv("DB_NAME", "")
    table_name = os.getenv("TARGET_TABLE", "etl.ivolatility_ivs")  # Default table

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

    if not api_key:
        logging.error("IVOL_API_KEY is not set. Exiting.")
        sys.exit(1)
    if not db_server or not db_name:
        logging.error("DB_SERVER or DB_NAME not set. Exiting.")
        sys.exit(1)

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

    # 4) Fetch list of tickers
    config_sql = """
        SELECT TOP 18
               Stock_ticker AS TickerSql,
               region AS ExchangeSql
        FROM etl.ivolatility_underlying_info
        WHERE Status = 'Active'
    """
    logging.info(f"Fetching config rows via: {config_sql.strip()}")
    try:
        config_df = pd.read_sql(config_sql, engine)
    except Exception as ex:
        logging.error(f"Failed to query config: {ex}")
        sys.exit(1)

    if config_df.empty:
        logging.error("No config row found. Exiting.")
        sys.exit(1)

    # We expect each row has TickerSql, ExchangeSql
    symbol_records = config_df.to_dict("records")
    logging.info(f"Fetched {len(symbol_records)} symbols to process.")

    # --------------------------------------------------------------------------
    # STEP A: Purge the entire table once, outside the multi-threaded logic
    # --------------------------------------------------------------------------
    try:
        with pyodbc.connect(odbc_conn_str, attrs_before=attrs) as conn:
            cur = conn.cursor()
            delete_sql = f"DELETE FROM {table_name}"
            cur.execute(delete_sql)
            conn.commit()
        logging.info(f"Successfully deleted all rows from {table_name}.")
    except Exception as dex:
        logging.error(f"Failed to delete rows from {table_name}: {dex}")
        sys.exit(1)

    # --------------------------------------------------------------------------
    # fetch_and_insert_symbol function (no delete step inside)
    # --------------------------------------------------------------------------
    def fetch_and_insert_symbol(ticker, exchange):
        """
        1) Fetch data from iVol for this symbol
        2) Insert into target table
        3) Return the number of inserted rows
        """
        # If your iVol columns differ, adapt the rename_map accordingly
        rename_map = {
            "Call/Put":           "Call_Put",
            "out-of-the-money %": "OTM",
            "Days":               "period",
            "Strike":             "strike",
        }

        # The order here matches columns in the INSERT statement below
        needed_cols = [
            "record_no",
            "symbol",
            "exchange",
            "date",
            "period",
            "strike",
            "OTM",
            "Call_Put",
            "IV",
            "delta"
        ]

        insert_sql = f"""
            INSERT INTO {table_name} (
                record_no,
                symbol,
                exchange,
                date,
                period,
                strike,
                OTM,
                Call_Put,
                IV,
                delta
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # 1) Fetch iVol Data
        try:
            data_df = getMarketData(
                symbol=ticker,
                from_=date_from,
                to=date_to,
                region=exchange,
                OTMFrom=int(otm_from),
                OTMTo=int(otm_to),
                periodFrom=int(p_from),
                periodTo=int(p_to)
            )
        except Exception as fex:
            logging.error(f"Fetch error for symbol={ticker}: {fex}")
            return 0

        if data_df.empty:
            logging.info(f"No data for {ticker}, region/exchange={exchange}, date={date_from}->{date_to}.")
            return 0

        # 2) rename columns as needed
        data_df.rename(columns=rename_map, inplace=True)

        # supply the table columns
        data_df["symbol"] = ticker
        data_df["exchange"] = exchange

        # If 'period' or 'strike' is missing, set them to None
        for col in ["period", "strike"]:
            if col not in data_df.columns:
                data_df[col] = None

        # Ensure we have a unique record_no
        if "record_no" not in data_df.columns:
            data_df["record_no"] = [
                abs(hash(uuid.uuid4())) % 2147483647
                for _ in range(len(data_df))
            ]

        # Fill missing needed columns with None
        for col in needed_cols:
            if col not in data_df.columns:
                data_df[col] = None

        # 3) Perform chunked inserts
        chunk_size = 5000
        inserted_count = 0
        total_rows = len(data_df)

        for start_idx in range(0, total_rows, chunk_size):
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
                logging.error(f"Insert chunk failed for {ticker}, chunk start={start_idx}: {iex}")
                return inserted_count

        time.sleep(1)  # small sleep to reduce rapid-fire calls
        return inserted_count

    # --------------------------------------------------------------------------
    # 5) Multi-threaded processing for all symbols, after the table is empty
    # --------------------------------------------------------------------------
    total_inserted = 0
    logging.info(f"Starting parallel fetch/insert for {len(symbol_records)} symbols. max_workers={max_workers}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(fetch_and_insert_symbol, r["TickerSql"], r["ExchangeSql"]): r["TickerSql"]
            for r in symbol_records
        }
        for future in as_completed(future_map):
            ticker = future_map[future]
            try:
                cnt = future.result()
                total_inserted += cnt
                logging.info(f"Symbol {ticker}: inserted {cnt} rows. Running total={total_inserted}")
            except Exception as exc:
                logging.error(f"Symbol {ticker} concurrency failed: {exc}")

    logging.info(f"All symbols processed. Total inserted: {total_inserted}.")
    logging.info("ETL job completed successfully.")

if __name__ == "__main__":
    main()

