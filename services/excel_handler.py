"""
Excel Handler Service
Handles reading and writing Excel files
"""

from io import BytesIO
import pandas as pd
import logging

from googleapiclient.discovery import build
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

# 🔐 Update if your credentials file has a different name or path
SERVICE_ACCOUNT_FILE = "credentials.json"

# Required scope for editing Google Sheets
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def read_excel_from_drive(file_bytes):
    """
    Read Excel file from raw bytes and return DataFrame
    """
    try:
        df = pd.read_excel(BytesIO(file_bytes))
        logger.info("Excel file read successfully")
        return df
    except Exception as e:
        logger.error(f"Error reading Excel file: {e}")
        raise


def update_cell_in_drive(file_id, sheet_name, cell, value):
    """
    Update a specific cell in a Google Sheets file stored in Drive.
    """

    try:
        # Authenticate using service account
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=SCOPES
        )

        service = build("sheets", "v4", credentials=credentials)

        sheet = service.spreadsheets()

        # Format range (example: Data!B2)
        range_name = f"{sheet_name}!{cell}"

        body = {
            "values": [[value]]
        }

        sheet.values().update(
            spreadsheetId=file_id,
            range=range_name,
            valueInputOption="RAW",
            body=body
        ).execute()

        logger.info(f"Updated {range_name} in file {file_id}")

        return True

    except Exception as e:
        logger.error(f"Error updating cell in Drive: {e}")
        raise
