"""
Excel Handler Service
Handles reading and writing Excel files with Google Drive integration
"""

import pandas as pd
import openpyxl
import tempfile
import os
import logging
from services import google_drive

logger = logging.getLogger(__name__)

def read_excel_from_drive(file_id, sheet_name=None):
    """
    Read Excel file from Google Drive into pandas DataFrame
    
    Args:
        file_id: Google Drive file ID
        sheet_name: Optional sheet name (defaults to first sheet)
    
    Returns:
        pandas.DataFrame: Excel data
    """
    try:
        # Download file as bytes
        file_bytes = google_drive.download_file_as_bytes(file_id)
        
        # Read with pandas
        from io import BytesIO
        df = pd.read_excel(BytesIO(file_bytes), sheet_name=sheet_name or 0)
        
        logger.info(f"Excel file read successfully from {file_id}")
        return df
        
    except Exception as e:
        logger.error(f"Error reading Excel from Drive: {e}")
        raise

def write_excel_to_drive(df, file_id, sheet_name='Sheet1'):
    """
    Write pandas DataFrame to Excel file on Google Drive
    
    Args:
        df: pandas DataFrame
        file_id: Google Drive file ID to update
        sheet_name: Sheet name to write to
    
    Returns:
        dict: Updated file metadata
    """
    try:
        # Write to temp file
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            tmp_path = tmp.name
        
        df.to_excel(tmp_path, sheet_name=sheet_name, index=False)
        
        # Upload to Drive (updates existing file)
        result = google_drive.upload_file(tmp_path, file_id=file_id)
        
        # Clean up
        os.remove(tmp_path)
        
        logger.info(f"Wrote {len(df)} rows to file {file_id}")
        return result
        
    except Exception as e:
        logger.error(f"Error writing Excel to Drive: {e}")
        raise

def update_cell_in_drive(file_id, sheet_name, cell, value):
    """
    Update a single cell in Excel file on Google Drive
    
    Args:
        file_id: Google Drive file ID
        sheet_name: Sheet name (e.g., 'Data')
        cell: Cell reference (e.g., 'B2')
        value: New value for the cell
    
    Returns:
        dict: Updated file metadata
    """
    try:
        # Download file to temp location
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            tmp_path = tmp.name
        
        logger.info(f"Downloading file {file_id} to update cell {cell}")
        google_drive.download_file(file_id, tmp_path)
        
        # Update cell using openpyxl
        wb = openpyxl.load_workbook(tmp_path)
        
        if sheet_name not in wb.sheetnames:
            logger.error(f"Sheet '{sheet_name}' not found. Available: {wb.sheetnames}")
            wb.close()
            os.remove(tmp_path)
            raise ValueError(f"Sheet '{sheet_name}' not found in workbook")
        
        ws = wb[sheet_name]
        ws[cell] = value
        wb.save(tmp_path)
        wb.close()
        
        logger.info(f"Updated cell {cell} to '{value}' in sheet '{sheet_name}'")
        
        # Upload back to Drive
        result = google_drive.upload_file(tmp_path, file_id=file_id)
        
        # Clean up
        os.remove(tmp_path)
        
        logger.info(f"Cell {cell} updated successfully in file {file_id}")
        return result
        
    except Exception as e:
        logger.error(f"Error updating cell in Drive: {e}")
        raise

def append_rows_to_drive(file_id, sheet_name, new_data):
    """
    Append rows to existing Excel file on Google Drive
    
    Args:
        file_id: Google Drive file ID
        sheet_name: Sheet name
        new_data: pandas DataFrame with new rows
    
    Returns:
        dict: Updated file metadata
    """
    try:
        # Read existing data
        existing_df = read_excel_from_drive(file_id, sheet_name)
        
        # Append new data
        combined_df = pd.concat([existing_df, new_data], ignore_index=True)
        
        # Write back
        result = write_excel_to_drive(combined_df, file_id, sheet_name)
        
        logger.info(f"Appended {len(new_data)} rows to file {file_id}")
        return result
        
    except Exception as e:
        logger.error(f"Error appending rows: {e}")
        raise

def get_excel_info(file_id):
    """
    Get information about Excel file
    
    Args:
        file_id: Google Drive file ID
    
    Returns:
        dict: File info including sheets, dimensions
    """
    try:
        # Download to temp file
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
            tmp_path = tmp.name
        
        google_drive.download_file(file_id, tmp_path)
        
        # Get workbook info
        wb = openpyxl.load_workbook(tmp_path, read_only=True)
        
        sheets_info = {}
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            sheets_info[sheet_name] = {
                'max_row': ws.max_row,
                'max_column': ws.max_column
            }
        
        wb.close()
        os.remove(tmp_path)
        
        return {
            'file_id': file_id,
            'sheets': sheets_info,
            'sheet_count': len(wb.sheetnames)
        }
        
    except Exception as e:
        logger.error(f"Error getting Excel info: {e}")
        raise
