"""
FRED API Integration
Federal Reserve Economic Data - Real-time macro indicators
"""

import os
import requests
import logging
from datetime import datetime, timedelta
import pandas as pd

logger = logging.getLogger(__name__)

class FREDClient:
    """
    Client for Federal Reserve Economic Data API
    Documentation: https://fred.stlouisfed.org/docs/api/fred/
    """
    
    def __init__(self, api_key=None):
        """Initialize FRED client with API key"""
        self.api_key = api_key or os.environ.get('FRED_API_KEY')
        if not self.api_key:
            raise ValueError("FRED_API_KEY not found in environment variables")
        
        self.base_url = "https://api.stlouisfed.org/fred"
        logger.info("FRED client initialized")
    
    def get_series(self, series_id, limit=100, sort_order='desc'):
        """
        Get data for a specific FRED series
        
        Args:
            series_id: FRED series ID (e.g., 'PAYEMS' for employment)
            limit: Number of observations to return
            sort_order: 'asc' or 'desc'
        
        Returns:
            dict: Series data with observations
        """
        try:
            url = f"{self.base_url}/series/observations"
            params = {
                'series_id': series_id,
                'api_key': self.api_key,
                'file_type': 'json',
                'limit': limit,
                'sort_order': sort_order
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'observations' in data:
                logger.info(f"Successfully fetched {len(data['observations'])} observations for {series_id}")
                return data
            else:
                logger.error(f"No observations in response for {series_id}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching FRED series {series_id}: {e}")
            raise
    
    def get_latest_value(self, series_id):
        """
        Get the most recent value for a series
        
        Returns:
            tuple: (value, date) or (None, None) if error
        """
        try:
            data = self.get_series(series_id, limit=1, sort_order='desc')
            
            if data and 'observations' in data and len(data['observations']) > 0:
                obs = data['observations'][0]
                value = obs.get('value')
                date = obs.get('date')
                
                # Convert value to float if possible
                try:
                    value = float(value) if value != '.' else None
                except (ValueError, TypeError):
                    value = None
                
                logger.info(f"{series_id}: {value} (as of {date})")
                return value, date
            
            return None, None
            
        except Exception as e:
            logger.error(f"Error getting latest value for {series_id}: {e}")
            return None, None
    
    def get_multiple_series(self, series_dict):
        """
        Get latest values for multiple series
        
        Args:
            series_dict: Dictionary mapping names to FRED series IDs
                        e.g., {'employment': 'PAYEMS', 'cpi': 'CPIAUCSL'}
        
        Returns:
            dict: Dictionary with values and dates for each series
        """
        results = {}
        
        for name, series_id in series_dict.items():
            value, date = self.get_latest_value(series_id)
            results[name] = {
                'value': value,
                'date': date,
                'series_id': series_id
            }
        
        return results
    
    def get_series_info(self, series_id):
        """
        Get metadata about a series
        
        Returns:
            dict: Series information (title, units, frequency, etc.)
        """
        try:
            url = f"{self.base_url}/series"
            params = {
                'series_id': series_id,
                'api_key': self.api_key,
                'file_type': 'json'
            }
            
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'seriess' in data and len(data['seriess']) > 0:
                return data['seriess'][0]
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching info for {series_id}: {e}")
            return None


# Predefined series mappings for common indicators
MACRO_SERIES = {
    # Leading Indicators
    'building_permits': 'PERMIT',  # Building Permits (thousands)
    'housing_starts': 'HOUST',  # Housing Starts (thousands)
    'manufacturing_employment': 'MANEMP',  # Manufacturing Employment (proxy for ISM)
    'consumer_sentiment': 'UMCSENT',  # University of Michigan Consumer Sentiment
    'initial_claims': 'ICSA',  # Initial unemployment claims (thousands)
    'leading_index': 'USSLIND',  # US Leading Index (Conference Board)
    
    # Coincident Indicators
    'payroll_employment': 'PAYEMS',  # Total Nonfarm Payrolls (thousands)
    'industrial_production': 'INDPRO',  # Industrial Production Index
    'personal_income': 'PI',  # Personal Income (billions)
    'retail_sales': 'RSXFS',  # Retail Sales (millions)
    
    # Interest Rates / Yields
    'fed_funds_rate': 'DFF',  # Federal Funds Effective Rate
    'treasury_10y': 'DGS10',  # 10-Year Treasury Constant Maturity Rate
    'treasury_2y': 'DGS2',  # 2-Year Treasury Constant Maturity Rate
    'treasury_5y': 'DGS5',  # 5-Year Treasury
    'treasury_3m': 'DGS3MO',  # 3-Month Treasury
    'baa_corporate': 'DBAA',  # Moody's Seasoned Baa Corporate Bond Yield
    'aaa_corporate': 'DAAA',  # Moody's Seasoned Aaa Corporate Bond Yield
    
    # Inflation
    'cpi_all': 'CPIAUCSL',  # Consumer Price Index for All Urban Consumers
    'cpi_core': 'CPILFESL',  # CPI Less Food & Energy
    'pce': 'PCE',  # Personal Consumption Expenditures
    'pce_core': 'PCEPILFE',  # PCE Less Food & Energy
    
    # GDP & Growth
    'gdp': 'GDP',  # Gross Domestic Product
    'gdp_real': 'GDPC1',  # Real GDP
    'gdp_growth': 'A191RL1Q225SBEA',  # Real GDP % Change
    
    # International & Commodities
    'oil_wti': 'DCOILWTICO',  # WTI Crude Oil Price
    'gold': 'GOLDAMGBD228NLBM',  # Gold Fixing Price
    'copper': 'PCOPPUSDM',  # Global Copper Price
    'dollar_index': 'DTWEXBGS',  # Trade Weighted US Dollar Index
    
    # NOTE: ISM Manufacturing PMI not available in FRED
    # Will fetch from Alpha Vantage or ISM.org separately
}


def get_macro_indicators():
    """
    Fetch all major macro indicators
    
    Returns:
        dict: Complete set of macro indicators with values and dates
    """
    client = FREDClient()
    return client.get_multiple_series(MACRO_SERIES)


def calculate_yield_spread(treasury_10y, treasury_2y):
    """Calculate 10Y-2Y yield spread"""
    if treasury_10y and treasury_2y:
        return round(treasury_10y - treasury_2y, 2)
    return None


def calculate_credit_spread(corporate_baa, treasury_10y):
    """Calculate BBB corporate to Treasury spread"""
    if corporate_baa and treasury_10y:
        return round(corporate_baa - treasury_10y, 2)
    return None
