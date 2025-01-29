from mods.oracle_connector import fetch_data, insert_data, clear_data
from mods.google_sheets import read_sheet, write_sheet
from mods.data_processing import clean_data
from dotenv import load_dotenv
from tqdm import tqdm
import logging
import os
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Load environment variables
load_dotenv()
oracle_table = "DADOS_RELA_VISITAS_MEDICOS"
query = """
SELECT
    DESCRICAO
FROM
    ESPECIALIDADE_MEDICA
ORDER BY
    1
"""

def main():
    try:
        logger.info("Starting data processing pipeline...")

        # 1. Clear data from Oracle table
        logger.info("Clearing data from Oracle table...")
        try:
            clear_data(oracle_table)
            logger.info("Oracle table cleared successfully.")
        except Exception as e:
            logger.error(f"Failed to clear Oracle table: {e}")
            sys.exit(1)

        # 2. Fetch data from Oracle
        logger.info("Fetching data from Oracle...")
        oracle_data = fetch_data(query)
        if oracle_data.empty:
            logger.warning("No data retrieved from Oracle.")
            sys.exit(1)  # Exit script if no data is retrieved
        logger.info(f"Fetched {len(oracle_data)} rows from Oracle.")

        # 3. Insert data into Google Sheets
        google_sheet_url = os.getenv("MEDIC_VISIT")
        if not google_sheet_url:
            logger.error("MEDIC_VISIT environment variable not set.")
            sys.exit(1)

        logger.info("Writing data to Google Sheets...")
        try:
            write_sheet(google_sheet_url, "ESPECIALIDADES", oracle_data)
            logger.info("Data successfully written to Google Sheets.")
        except Exception as e:
            logger.error(f"Error writing data to Google Sheets: {e}")
            sys.exit(1)

        # 4. Read data from Google Sheets
        logger.info("Reading data from Google Sheets...")
        try:
            google_data = read_sheet(google_sheet_url, "BASE")
            if google_data.empty:
                logger.warning("No data found in Google Sheets.")
                sys.exit(1)
            logger.info(f"Read {len(google_data)} rows from Google Sheets.")
        except Exception as e:
            logger.error(f"Error reading data from Google Sheets: {e}")
            sys.exit(1)

        # 5. Process data
        logger.info("Processing data...")
        try:
            cleaned_data = clean_data(google_data)
            if cleaned_data.empty:
                logger.warning("Data processing resulted in an empty dataset.")
                sys.exit(1)
            logger.info(f"Processed {len(cleaned_data)} rows.")
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            sys.exit(1)

        # 6. Insert data into Oracle with progress bar
        logger.info("Inserting data into Oracle with progress bar...")
        try:
            # Adding tqdm for progress bar in insert_data
            for _ in tqdm(range(len(cleaned_data)), desc="Inserting rows", unit="row"):
                insert_data(oracle_table, cleaned_data.iloc[_, :])
            logger.info("Data successfully inserted into Oracle.")
        except Exception as e:
            logger.error(f"Error inserting data into Oracle: {e}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
