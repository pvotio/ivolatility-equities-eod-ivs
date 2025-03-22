import os
import sys
import struct
import logging
import datetime
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
    Helper to format Azure AD access token for pyodbc's SQL_COPT_SS_ACCESS_TOKEN.
    """
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    enc_token = access_token.encode('utf-16-le')
    token_struct = struct.pack('=i', len(enc_token)) + enc_token
    return {SQL_COPT_SS_ACCESS_TOKEN: token_struct}

def main():
    # 1) Environment variables
    api_key     = os.getenv("IVOL_API_KEY", "")
    db_server   = os.getenv("DB_SERVER", "")
    db_name     = os.getenv("DB_NAME", "")
    table_name  = os.getenv("TARGET_TABLE", "etl.ivolatility_ivs")

    date_from   = os.getenv("DATE_FROM", "")
    date_to     = os.getenv("DATE_TO", "")
    otm_from    = os.getenv("OTM_FROM", "0")
    otm_to      = os.getenv("OTM_TO", "0")
    default_region = os.getenv("REGION", "USA")
    p_from      = os.getenv("PERIOD_FROM", "90")
    p_to        = os.getenv("PERIOD_TO", "90")
    ticker_sql  = os.getenv("TICKER_SQL", "AAPL")
    max_workers = int(os.getenv("MAX_WORKERS", "12"))

    # Basic checks
    if not api_key:
        logging.error("IVOL_API_KEY is not set. Exiting.")
        sys.exit(1)
    if not db_server or not db_name:
        logging.error("DB_SERVER or DB_NAME not set. Exiting.")
        sys.exit(1)
    if not date_from:
        # Default date_from to "yesterday"
        date_from = (datetime.datetime.utcnow() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    if not date_to:
        # Default date_to to "today"
        date_to = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    logging.info(f"Fetching IVS data from={date_from}, to={date_to}, default_region={default_region}")
    logging.info(f"Target table: {table_name}")

    # 2) Configure iVol
    try:
        ivol.setLoginParams(apiKey=api_key)
        getMarketData = ivol.setMethod('/equities/eod/ivs')
    except Exception as e:
        logging.error(f"Failed to configure iVol API: {e}")
        sys.exit(1)

    # 3) Acquire token for Azure SQL
    logging.info("Acquiring Azure SQL token via DefaultAzureCredential...")
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

    # Create engine to run read_sql for TICKER_SQL
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_conn_str)}",
        connect_args={'attrs_before': attrs}
    )

    # 4) Fetch symbol + region from DB
    logging.info(f"Running TICKER_SQL: {ticker_sql}")
    try:
        df_symbols = pd.read_sql(ticker_sql, engine)
    except Exception as ex:
        logging.error(f"Failed to run TICKER_SQL: {ex}")
        sys.exit(1)

    if df_symbols.empty:
        logging.warning("No symbols/regions returned from TICKER_SQL. Exiting.")
        return

    # Expect "symbol" and optionally "region" columns
    if "symbol" not in df_symbols.columns:
        logging.error("TICKER_SQL must return a 'symbol' column. Exiting.")
        sys.exit(1)

    # Convert to list of dicts
    symbol_rows = df_symbols.to_dict("records")
    logging.info(f"Fetched {len(symbol_rows)} symbols from DB.")

    # 5) We'll define a delete statement
    #    Adjust your actual table columns if they're named differently
    delete_sql = f"""
    DELETE FROM {table_name}
    WHERE [symbol] = ?
      AND [date] >= ?
      AND [date] <= ?
    """

    # 6) We'll define the insert statement
    #    Example columns for your ivol-ivs table (adjust to real columns):
    insert_sql = f"""
    INSERT INTO {table_name} (
        symbol,
        region,
        [date],
        [period],
        [strike],
        [Call_Put],
        [OTM],
        [IV],
        [delta]
        -- plus any other columns you need
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    def fetch_and_insert_one(sym_record):
        """
        1) Delete rows for (symbol, date range)
        2) Fetch from iVol
        3) Insert into the table (chunked)
        """
        sym = sym_record["symbol"]
        # If 'region' col is missing, fallback to default_region
        reg = sym_record.get("region", default_region)

        # (A) Delete
        try:
            with pyodbc.connect(odbc_conn_str, attrs_before=attrs) as conn:
                cur = conn.cursor()
                cur.execute(delete_sql, [sym, date_from, date_to])
                conn.commit()
        except Exception as ex:
            logging.error(f"Delete failed for symbol={sym}: {ex}")
            return 0

        # (B) Fetch from iVol
        try:
            # iVol's '/equities/eod/ivs' docs suggest arguments: symbol=..., from_=..., to=..., region=..., etc.
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
        except Exception as ex:
            logging.error(f"Error fetching symbol={sym} from iVol: {ex}")
            return 0

        if data_df.empty:
            logging.info(f"No data for symbol={sym} in range {date_from} -> {date_to}.")
            return 0

        # (C) Insert chunked
        # Suppose the columns returned are date, period, strike, "Call/Put", "out-of-the-money %", IV, delta, ...
        # We'll rename them to match the insert statement
        rename_map = {
            "Call/Put": "Call_Put",
            "out-of-the-money %": "OTM",
        }
        data_df.rename(columns=rename_map, inplace=True)

        # Add "symbol" and "region" columns if not present
        if "symbol" not in data_df.columns:
            data_df["symbol"] = sym
        if "region" not in data_df.columns:
            data_df["region"] = reg

        # Ensure columns exist for the insert
        needed_cols = [
            "symbol",
            "region",
            "date",
            "period",
            "strike",
            "Call_Put",
            "OTM",
            "IV",
            "delta"
        ]
        for col in needed_cols:
            if col not in data_df.columns:
                data_df[col] = None

        # Insert in chunks via pyodbc
        chunk_size = 5000
        inserted_count = 0

        insert_cols = needed_cols  # same order as insert_sql

        for start_idx in range(0, len(data_df), chunk_size):
            subset = data_df.iloc[start_idx:start_idx + chunk_size]

            # Convert to list-of-tuples
            data_tuples = list(subset[insert_cols].itertuples(index=False, name=None))

            try:
                with pyodbc.connect(odbc_conn_str, attrs_before=attrs) as conn:
                    cursor = conn.cursor()
                    cursor.fast_executemany = True
                    cursor.executemany(insert_sql, data_tuples)
                    conn.commit()
                inserted_count += len(data_tuples)
            except Exception as exc:
                logging.error(f"Insertion failed for symbol={sym} chunk: {exc}")
                return inserted_count

        return inserted_count

    # 7) Parallel Execution
    total_inserted = 0
    logging.info(f"Beginning concurrency for {len(symbol_rows)} symbols with max_workers={max_workers}...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_and_insert_one, row): row for row in symbol_rows}
        for future in as_completed(futures):
            rec = futures[future]
            sym = rec["symbol"]
            try:
                cnt = future.result()
                total_inserted += cnt
                logging.info(f"Symbol={sym}: inserted {cnt} rows. [Running total={total_inserted}]")
            except Exception as e:
                logging.error(f"Symbol={sym} failed: {e}")

    logging.info(f"All done. Total rows inserted: {total_inserted}")
    logging.info("ETL job completed successfully.")

if __name__ == "__main__":
    main()
