#
# Python ETL Script for EIEP3 File Loading (SFTP or Local)
#
# This script performs an ETL (Extract, Transform, Load) process to move
# half-hourly electricity meter reading data into Snowflake. It can be configured
# to read the source file from either a remote SFTP server or the local filesystem.
#
# Required Libraries:
# - snowflake-connector-python, pandas, paramiko
#
# To install, run: pip install "snowflake-connector-python[pandas]" paramiko
#

import pandas as pd
import io
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import connect
from datetime import datetime
import paramiko # Required for SFTP

# --- Configuration Section ---
# IMPORTANT: Choose your load method and fill in the corresponding details.

# 1. CHOOSE YOUR LOAD METHOD: 'SFTP' or 'LOCAL'
LOAD_METHOD = 'LOCAL' # <-- Set to 'LOCAL' for testing

# 2. CONFIGURE FILE SOURCES
SFTP_CONFIG = {
    "hostname": "your-sftp-hostname.com",
    "port": 22,
    "username": "your-sftp-username",
    "password": "your-sftp-password",
    "filepath": "/remote/path/to/your_eiep3_file.csv"
}

LOCAL_CONFIG = {
    # This path assumes the CSV file is in the same directory as the script.
    "filepath": "./eiep3_data.csv"
}

# 3. CONFIGURE SNOWFLAKE DESTINATION
SNOWFLAKE_CONFIG = {
    "user": "petrosian",
    "password": "DrbbbGxkAFFiiV5",
    "account": "ubfeszx-kx71303",
    "warehouse": "COMPUTE_WH",
    "database": "ELECTRICITY_DW",
    "schema": "METER_READINGS"
}

# --- Data Extraction Functions ---

def _get_file_lines_from_sftp():
    """Connects to SFTP and returns the file content as a list of lines."""
    print("Connecting to SFTP server...")
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh_client.connect(**SFTP_CONFIG)
        sftp_client = ssh_client.open_sftp()
        print(f"Reading remote file: {SFTP_CONFIG['filepath']}")
        with sftp_client.open(SFTP_CONFIG["filepath"], 'r') as remote_file:
            return io.StringIO(remote_file.read()).readlines()
    finally:
        if ssh_client and ssh_client.get_transport() and ssh_client.get_transport().is_active():
            ssh_client.close()
            print("SFTP connection closed.")

def _get_file_lines_from_local():
    """Reads a local file and returns its content as a list of lines."""
    print(f"Reading local file: {LOCAL_CONFIG['filepath']}")
    with open(LOCAL_CONFIG['filepath'], 'r') as local_file:
        return local_file.readlines()

# --- Main ETL Logic ---

def load_eiep3_to_snowflake():
    """
    Orchestrates the E2E process: extracts data based on the chosen method,
    transforms it, and loads it into Snowflake.
    """
    processed_records = []
    header_info = {}
    file_lines = []

    try:
        #
        # STEP 1: EXTRACT - Get file content based on the LOAD_METHOD switch
        #
        print(f"Starting ETL process using '{LOAD_METHOD}' method.")
        if LOAD_METHOD == 'SFTP':
            file_lines = _get_file_lines_from_sftp()
        elif LOAD_METHOD == 'LOCAL':
            file_lines = _get_file_lines_from_local()
        else:
            raise ValueError(f"Invalid LOAD_METHOD: '{LOAD_METHOD}'. Please choose 'SFTP' or 'LOCAL'.")

        #
        # STEP 2: TRANSFORM - Parse each line and structure the data
        #
        print("Processing file records...")
        for line in file_lines:
            if not line.strip(): continue

            fields = [field.strip() for field in line.split(',')]
            record_type = fields[0]

            if record_type == "HDR":
                header_info = {
                    "FILE_IDENTIFIER": fields[8],
                    "CREATION_DATE": datetime.strptime(fields[6], '%d/%m/%Y').date(),
                    "CREATION_TIME": datetime.strptime(fields[7], '%H:%M:%S').time(),
                    "SENDER_PARTICIPANT_CODE": fields[3],
                    "RECEIVER_PARTICIPANT_CODE": fields[4],
                    "CONSUMPTION_MONTH": fields[10]
                }
                print("Header information parsed.")
            elif record_type == "DET":
                if not header_info:
                    raise ValueError("Invalid File Format: Detail record (DET) found before Header record (HDR).")

                reading_date = datetime.strptime(fields[4], '%d/%m/%Y').date()
                trading_period = int(fields[5])
                reading_hour = (trading_period - 1) // 2
                reading_minute = 30 * ((trading_period - 1) % 2)

                reading_datetime = datetime.combine(
                    reading_date, datetime.min.time()
                ).replace(hour=reading_hour, minute=reading_minute)

                processed_records.append({
                    **header_info,
                    "ICP_IDENTIFIER": fields[1], "METER_SERIAL_NUMBER": fields[2],
                    "READING_STATUS_FLAG": fields[3], "READING_DATETIME": reading_datetime,
                    "KWH_CONSUMPTION": float(fields[6]) if fields[6] else None,
                    "KVARH_CONSUMPTION": float(fields[7]) if fields[7] else None,
                    "FLOW_DIRECTION": fields[9]
                })

        #
        # STEP 3: LOAD - Write the transformed data into Snowflake
        #
        if not processed_records:
            print("No detail records (DET) found. Nothing to load.")
            return

        print(f"Transform complete. Preparing to load {len(processed_records)} records...")
        df = pd.DataFrame(processed_records)

        with connect(**SNOWFLAKE_CONFIG) as conn:
            success, nchunks, nrows, _ = write_pandas(
                conn=conn, df=df, table_name="RAW_EIEP3_METER_READINGS",
                database=SNOWFLAKE_CONFIG["database"], schema=SNOWFLAKE_CONFIG["schema"]
            )

            if success:
                print(f"LOAD COMPLETE: Successfully loaded {nrows} rows.")
            else:
                print("LOAD FAILED: The write_pandas operation did not complete successfully.")

    except (FileNotFoundError, paramiko.AuthenticationException) as e:
        print(f"ERROR during extraction: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# --- Script Execution ---
if __name__ == "__main__":
    load_eiep3_to_snowflake()
