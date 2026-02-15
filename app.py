"""
Trading System API - Full System
Flask application with Google Drive integration and complete data pipeline

Week 1: Foundation setup with template management
"""

from flask import Flask, request, jsonify
import os
import sys
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Import configurations
try:
    import config
    config.validate_config()
    logger.info("Configuration validated successfully")
except Exception as e:
    logger.error(f"Configuration error: {e}")
    # Continue but warn

# Import Google Drive service
try:
    from services import google_drive
    logger.info("Google Drive service loaded")
except ImportError as e:
    logger.error(f"Failed to import google_drive: {e}")
    google_drive = None

# Import Excel handler (optional for now)
try:
    from services import excel_handler
    logger.info("Excel handler loaded")
except ImportError as e:
    logger.warning(f"Excel handler not available: {e}")
    excel_handler = None

#═══════════════════════════════════════════════════════════════════════════════
# HEALTH CHECK ENDPOINTS
#═══════════════════════════════════════════════════════════════════════════════

@app.route('/')
def home():
    """Main health check endpoint"""
    return jsonify({
        'status': 'running',
        'message': 'Trading System API - Full System',
        'version': '1.0.0',
        'timestamp': datetime.now().isoformat(),
        'week': 1,
        'phase': 'Infrastructure Setup'
    })

@app.route('/health')
def health():
    """Detailed health check with all systems"""
    health_status = {
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'components': {}
    }
    
    # Check API keys
    health_status['components']['api_keys'] = {
        'groq': bool(os.environ.get('GROQ_API_KEY')),
        'alpha_vantage': bool(os.environ.get('ALPHA_VANTAGE_KEY')),
        'fred': bool(os.environ.get('FRED_API_KEY')),
        'telegram': bool(os.environ.get('TELEGRAM_BOT_TOKEN'))
    }
    
    # Check Google Drive connection
    if google_drive:
        try:
            success, message = google_drive.test_drive_connection()
            health_status['components']['google_drive'] = {
                'status': 'connected' if success else 'failed',
                'message': message
            }
        except Exception as e:
            health_status['components']['google_drive'] = {
                'status': 'error',
                'message': str(e)
            }
    else:
        health_status['components']['google_drive'] = {
            'status': 'error',
            'message': 'Google Drive module not loaded'
        }

    # Overall status
    all_healthy = all(
        comp.get('status') != 'failed'
        for comp in health_status['components'].values()
        if isinstance(comp, dict) and 'status' in comp
    )
    
    if not all_healthy:
        health_status['status'] = 'degraded'
    
    return jsonify(health_status)


#═══════════════════════════════════════════════════════════════════════════════
# GOOGLE DRIVE ENDPOINTS (Week 1 Focus)
#═══════════════════════════════════════════════════════════════════════════════

@app.route('/drive/list/<folder_type>')
def list_drive_folder(folder_type):
    """
    List files in a Google Drive folder
    """
    try:
        folder_id = config.DRIVE_FOLDERS.get(folder_type)

        if not folder_id:
            return jsonify({
                'error': f'Unknown folder type: {folder_type}',
                'available_types': list(config.DRIVE_FOLDERS.keys())
            }), 400

        files = google_drive.list_files_in_folder(folder_id)

        return jsonify({
            'folder_type': folder_type,
            'folder_id': folder_id,
            'file_count': len(files),
            'files': files,
            'status': 'success'
        })

    except Exception as e:
        logger.error(f"Error listing folder: {e}")
        return jsonify({'error': str(e)}), 500


import numpy as np
import pandas as pd

@app.route('/drive/read/<file_id>')
def read_excel_file(file_id):
    """
    Read Excel file from Google Drive
    """
    try:
        if not google_drive:
            return jsonify({'error': 'Google Drive not available'}), 500

        # Download file
        file_bytes = google_drive.download_file_as_bytes(file_id)

        # Convert to dataframe
        df = excel_handler.read_excel_from_drive(file_bytes)

        # 🔥 CRITICAL FIX — Make dataframe JSON safe
        df = df.replace({np.nan: None})
        df = df.where(pd.notnull(df), None)

        preview = df.head(10).to_dict('records')

        return jsonify({
            'file_id': file_id,
            'rows': int(len(df)),
            'columns': list(df.columns),
            'preview': preview,
            'status': 'success'
        })

    except Exception as e:
        logger.error(f"Error reading file {file_id}: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/templates/test-update', methods=['POST'])
def test_template_update():
    """
    Test updating a template in Google Drive
    """
    try:
        data = request.get_json()

        file_id = data.get('file_id')
        sheet_name = data.get('sheet_name', 'Sheet1')
        cell = data.get('cell', 'A1')
        value = data.get('value')

        if not value:
            value = f'Updated {datetime.now().isoformat()}'

        excel_handler.update_cell_in_drive(file_id, sheet_name, cell, value)

        return jsonify({
            'message': f'Cell {cell} updated successfully',
            'file_id': file_id,
            'sheet_name': sheet_name,
            'new_value': value,
            'status': 'success'
        })

    except Exception as e:
        logger.error(f"Error updating template: {e}")
        return jsonify({'error': str(e)}), 500


#═══════════════════════════════════════════════════════════════════════════════
# MACRO DATA ENDPOINTS (Week 1-2 implementation)
#═══════════════════════════════════════════════════════════════════════════════

@app.route('/macro/templates')
def list_macro_templates():
    """List all macro indicator templates from Google Drive"""
    try:
        templates = []
        
        for folder_type in ['macro_leading', 'macro_coincident', 'macro_international']:
            folder_id = config.DRIVE_FOLDERS.get(folder_type)
            if folder_id:
                files = google_drive.list_files_in_folder(folder_id)
                templates.extend([{
                    **f,
                    'category': folder_type
                } for f in files])
        
        return jsonify({
            'total_templates': len(templates),
            'templates': templates,
            'status': 'success'
        })
        
    except Exception as e:
        logger.error(f"Error listing templates: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/macro/test-fetch', methods=['GET'])
def test_macro_fetch():
    """
    Test endpoint - Returns dummy macro data
    Week 2: Replace with real data fetching
    """
    logger.info("Fetching macro data (test mode - Week 1)")
    
    macro_data = {
        'ism_manufacturing': 54.2,
        'ism_services': 52.8,
        'yield_10y': 4.25,
        'yield_2y': 4.65,
        'yield_spread': -0.40,
        'credit_spread_bbb': 2.80,
        'consumer_confidence': 103.5,
        'building_permits': 1450000,
        'housing_starts': 1420000,
        'copper_price_change': 3.2,
        'china_pmi': 49.8,
        'timestamp': datetime.now().isoformat(),
        'status': 'success',
        'mode': 'test',
        'note': 'Week 1 - Dummy data. Week 2 will implement real fetching.'
    }
    
    return jsonify(macro_data)

#═══════════════════════════════════════════════════════════════════════════════
# STOCK SCREENING ENDPOINTS (Week 3-5 implementation)
#═══════════════════════════════════════════════════════════════════════════════

@app.route('/stocks/test-screen', methods=['POST'])
def test_stock_screen():
    """
    Test endpoint - Returns dummy stock screening results
    Week 3-5: Replace with real screening from US_Sector_Data.xlsm
    """
    logger.info("Screening stocks (test mode - Week 1)")
    
    data = request.get_json() or {}
    sectors = data.get('sectors', ['Financials', 'Industrials'])
    
    # Dummy stock data
    all_stocks = [
        {
            'ticker': 'JPM',
            'company': 'JPMorgan Chase',
            'sector': 'Financials',
            'price': 156.50,
            'pe': 11.2,
            'roe': 17.0,
            'eps_growth_y1': 18.0,
            'eps_growth_y2': 24.0,
            'market_cap': 445000000000,
            'beta': 1.19,
            'eg_profile': 'Profile 1 - Accelerating Outperformer'
        },
        {
            'ticker': 'BAC',
            'company': 'Bank of America',
            'sector': 'Financials',
            'price': 32.45,
            'pe': 10.8,
            'roe': 14.0,
            'eps_growth_y1': 15.0,
            'eps_growth_y2': 22.0,
            'market_cap': 265000000000,
            'beta': 1.31,
            'eg_profile': 'Profile 1 - Accelerating Outperformer'
        },
        {
            'ticker': 'CAT',
            'company': 'Caterpillar',
            'sector': 'Industrials',
            'price': 214.50,
            'pe': 14.2,
            'roe': 21.0,
            'eps_growth_y1': 12.0,
            'eps_growth_y2': 18.0,
            'market_cap': 108000000000,
            'beta': 1.21,
            'eg_profile': 'Profile 2 - Stable Outperformer'
        }
    ]
    
    filtered = [s for s in all_stocks if s['sector'] in sectors]
    
    return jsonify({
        'candidates': filtered,
        'count': len(filtered),
        'sectors_requested': sectors,
        'status': 'success',
        'mode': 'test',
        'note': 'Week 1 - Dummy data. Week 3-5 will implement real screening.'
    })

#═══════════════════════════════════════════════════════════════════════════════
# UTILITY ENDPOINTS
#═══════════════════════════════════════════════════════════════════════════════

@app.route('/ping')
def ping():
    """Simple ping for monitoring"""
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.now().isoformat()
    })

@app.route('/test/telegram', methods=['POST'])
def test_telegram():
    """Test Telegram notification"""
    try:
        data = request.get_json() or {}
        message = data.get('message', 'Test message from Trading System')
        
        # Import telegram handler
        from services import telegram_handler
        
        result = telegram_handler.send_message(message)
        
        return jsonify({
            'status': 'success',
            'message': 'Telegram message sent',
            'result': result
        })
        
    except Exception as e:
        logger.error(f"Telegram test failed: {e}")
        return jsonify({'error': str(e)}), 500

#═══════════════════════════════════════════════════════════════════════════════
# ERROR HANDLERS
#═══════════════════════════════════════════════════════════════════════════════

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'error': 'Endpoint not found',
        'available_endpoints': [
            'GET /',
            'GET /health',
            'GET /drive/list/<folder_type>',
            'GET /drive/read/<file_id>',
            'POST /templates/test-update',
            'GET /macro/templates',
            'GET /macro/test-fetch',
            'POST /stocks/test-screen',
            'POST /test/telegram'
        ]
    }), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {error}")
    return jsonify({
        'error': 'Internal server error',
        'message': str(error)
    }), 500

#═══════════════════════════════════════════════════════════════════════════════
# MAIN
#═══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_ENV') == 'development'
    
    logger.info(f"Starting Trading System API on port {port}")
    logger.info(f"Debug mode: {debug}")
    
    app.run(host='0.0.0.0', port=port, debug=debug)
