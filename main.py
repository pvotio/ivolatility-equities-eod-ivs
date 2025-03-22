import os
import sys
import struct
import logging
import datetime
import uuid

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
    table_name = os.getenv("TARGET_TABLE", "etl.ivolatility_ivs")  # Default to your actual table

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

    # 4) Fetch your list of tickers (and possibly exchange or region if needed)
    #    Suppose your config table returns exchange instead of region. 
    #    If not, adapt as needed.
    config_sql = """
        SELECT TOP 18000
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

    def fetch_and_insert_symbol(ticker, exchange):
        """
        1) DELETE old rows for (ticker, date range)
        2) Fetch data from iVol
        3) Insert into target table
        4) Return the number of inserted rows
        """
        delete_sql = f"""
            DELETE FROM {table_name}
        """

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

        # We will map iVol column names to match your table columns.
        # The actual iVol columns may differ! Adjust accordingly.
        rename_map = {
            "Call/Put":            "Call_Put",
            "out-of-the-money %":  "OTM",
            # If iVol returns 'Days' or 'period' for tenor:
            "Days":                "period",
            # If iVol returns 'Strike' for strike price:
            "Strike":              "strike",
        }

        # The order here matches the columns in the INSERT statement
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

        # A) DELETE
        try:
            with pyodbc.connect(odbc_conn_str, attrs_before=attrs) as conn:
                cur = conn.cursor()
                cur.execute(delete_sql, [ticker, date_from, date_to])
                conn.commit()
        except Exception as dex:
            logging.error(f"Delete failed for {ticker}: {dex}")
            return 0

        # B) Fetch iVol Data
        try:
            data_df = getMarketData(
                symbol=ticker,
                from_=date_from,
                to=date_to,
                region=exchange,   # If iVol's param is actually `region`, pass `exchange` here
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

        # rename columns to match your DB
        data_df.rename(columns=rename_map, inplace=True)

        # Supply the table columns as needed
        data_df["symbol"] = ticker
        data_df["exchange"] = exchange

        # If 'period' or 'strike' is missing, set them to default or null
        for col in ["period", "strike"]:
            if col not in data_df.columns:
                data_df[col] = None

        # Make sure 'record_no' exists and is unique
        if "record_no" not in data_df.columns:
            data_df["record_no"] = [
                abs(hash(uuid.uuid4())) % 2147483647
                for _ in range(len(data_df))
            ]

        # Ensure *every* needed col is present; fill with None if missing
        for col in needed_cols:
            if col not in data_df.columns:
                data_df[col] = None

        # C) chunked insert
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
                logging.error(f"Insert chunk failed for {ticker}, chunk start={start_idx}: {iex}")
                return inserted_count

        return inserted_count

    # 5) Multi-threaded processing for all symbols
    total_inserted = 0
    logging.info(f"Starting parallel fetch/insert for {len(symbol_records)} symbols. max_workers={max_workers}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                fetch_and_insert_symbol,
                r["TickerSql"],
                r["ExchangeSql"]
            ): r["TickerSql"]
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

