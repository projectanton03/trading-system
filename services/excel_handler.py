"""
Excel Handler Service
Handles reading and writing Excel files
"""

from io import BytesIO
import pandas as pd
import tempfile
import os
import logging

logger = logging.getLogger(__name__)


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
    Placeholder for updating Excel file in Drive.
    (Can implement later if needed.)
    """
    raise NotImplementedError(
        "update_cell_in_drive not implemented yet"
    )
