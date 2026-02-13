"""
Data Update Strategy - Week 2 Implementation
Handles both backfill and incremental updates
"""

import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

#═══════════════════════════════════════════════════════════════════════════════
# BACKFILL STRATEGY (Run Once)
#═══════════════════════════════════════════════════════════════════════════════

def backfill_template(file_id, template_name, fetch_function, start_date='2021-01-01'):
    """
    One-time backfill of historical data
    Catches up from last date in Excel to present
    
    Args:
        file_id: Google Drive file ID
        template_name: Name of template (for logging)
        fetch_function: Function to fetch data from API
        start_date: Minimum start date (fallback)
    
    Returns:
        dict: {rows_added, start_date, end_date}
    """
    try:
        # Read current template
        df = read_excel_from_drive(file_id)
        
        # Determine last date in template
        if 'Date' in df.columns and len(df) > 0:
            last_date = pd.to_datetime(df['Date']).max()
            logger.info(f"{template_name}: Last date in template: {last_date}")
        else:
            # No data yet, use fallback
            last_date = pd.to_datetime(start_date)
            logger.info(f"{template_name}: No existing data, starting from {start_date}")
        
        # Fetch missing data
        current_date = datetime.now()
        
        if last_date >= current_date - timedelta(days=7):
            logger.info(f"{template_name}: Already up to date!")
            return {'rows_added': 0, 'already_current': True}
        
        logger.info(f"{template_name}: Fetching data from {last_date} to {current_date}")
        
        # Call the data fetching function
        new_data = fetch_function(
            start_date=last_date + timedelta(days=1),
            end_date=current_date
        )
        
        if new_data is None or len(new_data) == 0:
            logger.warning(f"{template_name}: No new data available")
            return {'rows_added': 0, 'no_data_available': True}
        
        # Append new data
        df_updated = pd.concat([df, new_data], ignore_index=True)
        
        # Remove duplicates (in case of overlap)
        df_updated = df_updated.drop_duplicates(subset=['Date'], keep='last')
        
        # Sort by date
        df_updated = df_updated.sort_values('Date').reset_index(drop=True)
        
        # Write back to Drive
        write_excel_to_drive(df_updated, file_id, sheet_name='Data')
        
        logger.info(f"✅ {template_name}: Added {len(new_data)} rows")
        
        return {
            'rows_added': len(new_data),
            'start_date': new_data['Date'].min(),
            'end_date': new_data['Date'].max(),
            'total_rows': len(df_updated)
        }
        
    except Exception as e:
        logger.error(f"❌ {template_name} backfill failed: {e}")
        raise

#═══════════════════════════════════════════════════════════════════════════════
# INCREMENTAL UPDATE STRATEGY (Run Weekly)
#═══════════════════════════════════════════════════════════════════════════════

def update_template_incremental(file_id, template_name, fetch_function):
    """
    Weekly incremental update - adds only new data
    
    Args:
        file_id: Google Drive file ID
        template_name: Name of template
        fetch_function: Function to fetch latest data
    
    Returns:
        dict: {rows_added, updated}
    """
    try:
        # Read current template
        df = read_excel_from_drive(file_id)
        
        # Get last date
        last_date = pd.to_datetime(df['Date']).max()
        logger.info(f"{template_name}: Last date: {last_date}")
        
        # Fetch only new data (from last date to now)
        new_data = fetch_function(
            start_date=last_date + timedelta(days=1),
            end_date=datetime.now()
        )
        
        if new_data is None or len(new_data) == 0:
            logger.info(f"{template_name}: No new data available (already current)")
            return {'rows_added': 0, 'updated': False}
        
        # Append new rows
        df_updated = pd.concat([df, new_data], ignore_index=True)
        
        # Remove duplicates
        df_updated = df_updated.drop_duplicates(subset=['Date'], keep='last')
        
        # Sort
        df_updated = df_updated.sort_values('Date').reset_index(drop=True)
        
        # Write back
        write_excel_to_drive(df_updated, file_id, sheet_name='Data')
        
        logger.info(f"✅ {template_name}: Added {len(new_data)} new rows")
        
        return {
            'rows_added': len(new_data),
            'updated': True,
            'new_dates': new_data['Date'].tolist()
        }
        
    except Exception as e:
        logger.error(f"❌ {template_name} update failed: {e}")
        raise

#═══════════════════════════════════════════════════════════════════════════════
# BATCH UPDATE ALL TEMPLATES
#═══════════════════════════════════════════════════════════════════════════════

def update_all_macro_templates(mode='incremental'):
    """
    Update all 52 macro templates
    
    Args:
        mode: 'backfill' (one-time) or 'incremental' (weekly)
    
    Returns:
        dict: Summary of updates
    """
    
    # Template mapping: file_id -> fetch_function
    TEMPLATE_MAPPING = {
        'ISM_Manufacturing': {
            'file_id': KEY_FILES['ism_manufacturing'],
            'fetch_func': fetch_ism_manufacturing_data,
            'frequency': 'monthly'
        },
        'ISM_Services': {
            'file_id': find_file_by_name('ISM_Services.xlsx', DRIVE_FOLDERS['macro_leading']),
            'fetch_func': fetch_ism_services_data,
            'frequency': 'monthly'
        },
        'Treasury_Yields': {
            'file_id': find_file_by_name('Treasury_Yields.xlsx', DRIVE_FOLDERS['macro_leading']),
            'fetch_func': fetch_treasury_yields_data,
            'frequency': 'daily'
        },
        'Credit_Spreads': {
            'file_id': find_file_by_name('Credit_Spreads.xlsx', DRIVE_FOLDERS['macro_leading']),
            'fetch_func': fetch_credit_spreads_data,
            'frequency': 'daily'
        },
        # ... Add all 52 templates here
    }
    
    results = {
        'total_templates': len(TEMPLATE_MAPPING),
        'updated': 0,
        'failed': 0,
        'details': []
    }
    
    for template_name, config in TEMPLATE_MAPPING.items():
        try:
            if mode == 'backfill':
                result = backfill_template(
                    file_id=config['file_id'],
                    template_name=template_name,
                    fetch_function=config['fetch_func']
                )
            else:  # incremental
                result = update_template_incremental(
                    file_id=config['file_id'],
                    template_name=template_name,
                    fetch_function=config['fetch_func']
                )
            
            results['details'].append({
                'template': template_name,
                'status': 'success',
                **result
            })
            
            if result.get('rows_added', 0) > 0:
                results['updated'] += 1
                
        except Exception as e:
            logger.error(f"Failed to update {template_name}: {e}")
            results['failed'] += 1
            results['details'].append({
                'template': template_name,
                'status': 'failed',
                'error': str(e)
            })
    
    logger.info(f"Batch update complete: {results['updated']} updated, {results['failed']} failed")
    
    return results

#═══════════════════════════════════════════════════════════════════════════════
# DATA RETENTION MANAGEMENT
#═══════════════════════════════════════════════════════════════════════════════

def cleanup_old_data(file_id, template_name, retention_years=5, keep_all_for_monthly=True):
    """
    Remove very old data to keep files manageable
    
    Args:
        file_id: Google Drive file ID
        template_name: Template name
        retention_years: How many years to keep
        keep_all_for_monthly: Keep all history for monthly indicators
    
    Returns:
        dict: {rows_removed, oldest_date_kept}
    """
    try:
        df = read_excel_from_drive(file_id)
        
        # For monthly indicators, keep ALL history
        if keep_all_for_monthly:
            logger.info(f"{template_name}: Keeping all historical data (monthly indicator)")
            return {'rows_removed': 0, 'keep_all': True}
        
        # For daily indicators, keep last N years
        cutoff_date = datetime.now() - timedelta(days=retention_years*365)
        
        original_rows = len(df)
        df_cleaned = df[pd.to_datetime(df['Date']) >= cutoff_date]
        rows_removed = original_rows - len(df_cleaned)
        
        if rows_removed > 0:
            write_excel_to_drive(df_cleaned, file_id, sheet_name='Data')
            logger.info(f"{template_name}: Removed {rows_removed} old rows (keeping last {retention_years} years)")
        
        return {
            'rows_removed': rows_removed,
            'oldest_date_kept': df_cleaned['Date'].min()
        }
        
    except Exception as e:
        logger.error(f"Cleanup failed for {template_name}: {e}")
        raise

#═══════════════════════════════════════════════════════════════════════════════
# EXAMPLE USAGE
#═══════════════════════════════════════════════════════════════════════════════

def main():
    """Example of how to use these functions"""
    
    # WEEK 2 - ONE TIME BACKFILL
    # Run this ONCE when you first deploy the system
    print("Starting one-time backfill...")
    backfill_results = update_all_macro_templates(mode='backfill')
    print(f"Backfill complete: {backfill_results['updated']} templates updated")
    
    # WEEK 3+ - WEEKLY INCREMENTAL UPDATES
    # This runs automatically every Sunday 6pm via n8n
    print("\nRunning weekly incremental update...")
    weekly_results = update_all_macro_templates(mode='incremental')
    print(f"Weekly update complete: {weekly_results['updated']} templates updated")
    
    # Generate summary report
    summary = f"""
    📊 Macro Data Update Summary
    
    Total Templates: {weekly_results['total_templates']}
    Updated: {weekly_results['updated']}
    Failed: {weekly_results['failed']}
    
    Details:
    """
    
    for detail in weekly_results['details']:
        if detail['status'] == 'success' and detail.get('rows_added', 0) > 0:
            summary += f"\n  ✅ {detail['template']}: +{detail['rows_added']} rows"
        elif detail['status'] == 'failed':
            summary += f"\n  ❌ {detail['template']}: {detail['error']}"
    
    print(summary)
    
    return weekly_results

if __name__ == '__main__':
    main()
