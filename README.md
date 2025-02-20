Google Sheets to Oracle:
This project collects data from Google Sheets using the Google API, processes the necessary information, and inserts it into an Oracle table. It is useful for collecting real-time data from Google Sheets and integrating it into a relational database.

Configuration:
Create a .env file in the root directory of the project with the following environment variables:

ORACLE_CONN_STRING=oracle_user:password@host/name  
GOOGLE_API_CREDENTIALS=google_Cred_json_path/intkpi-9999999999.json  
MEDIC_VISIT=https://docs.google.com/spreadsheets/d/your_spreadsheet_link  
MEDIC_EXTERNAL=https://docs.google.com/spreadsheets/d/your_spreadsheet_link  
Ensure that the credentials for both the Google API and the Oracle database are correctly configured.

Create a config folder in the root directory of the project and place the following inside:

The instantclient_19_21 folder (Oracle Instant Client)
The .json authorization file for the Google API

Usage:
Run the script to collect data from Google Sheets and insert it into the Oracle table:

python main.py
The script retrieves data from the specified Google Sheets, processes it, and inserts it into the Oracle database.
