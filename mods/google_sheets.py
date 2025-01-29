import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
import pandas as pd
import logging
import os
import sys

# Log configuration
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# Load environment variables
load_dotenv()

def connect_to_google_sheets():
    credentials_path = os.getenv("GOOGLE_API_CREDENTIALS")
    if not credentials_path:
        logger.error("Google API credentials path is not set in the environment.")
        sys.exit(1)  # Exit the script with an error code if credentials file is not found
    
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    try:
        creds = Credentials.from_service_account_file(credentials_path, scopes=SCOPES)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        logger.error(f"Error connecting to Google Sheets: {e}")
        sys.exit(1)  # Exit the script if there is an error connecting to Google Sheets

def read_sheet(sheet_url, sheet_name):
    """Reads data from a Google Sheets sheet."""
    try:
        client = connect_to_google_sheets()
        sheet = client.open_by_url(sheet_url).worksheet(sheet_name)
        data = sheet.get_all_records()
        if not data:  # Check if no data was returned
            logger.error(f"No data found in sheet {sheet_name}.")
        return pd.DataFrame(data)  # Returns a DataFrame, even if empty
    except Exception as e:
        logger.error(f"Error reading from sheet {sheet_name}: {e}")
        sys.exit(1)  # Exit the script in case of an error while reading the sheet

def write_sheet(sheet_url, sheet_name, data):
    """Writes data to a Google Sheets sheet."""
    if data.empty:
        logger.error(f"No data to write to sheet {sheet_name}.")
        return  # If no data, skip writing
    
    try:
        client = connect_to_google_sheets()
        sheet = client.open_by_url(sheet_url).worksheet(sheet_name)
        sheet.clear()  # Clear existing data
        sheet.append_row(data.columns.tolist())  # Append the headers
        sheet.update(range_name='A1', values=data.values.tolist())  # Append the data
    except Exception as e:
        logger.error(f"Error writing to sheet {sheet_name}: {e}")
        sys.exit(1)  # Exit the script in case of an error while writing to the sheet
