# services/stock_screener.py
"""
Stock Fundamental Screening System
Uses Alpha Vantage API to fetch fundamentals and screen for longs/shorts
"""

import requests
import pandas as pd
from datetime import datetime
import logging
import time

logger = logging.getLogger(__name__)

class StockScreener:
    """Screen stocks based on fundamental criteria"""
    
    def __init__(self, alpha_vantage_key):
        self.api_key = alpha_vantage_key
        self.base_url = "https://www.alphavantage.co/query"
        
    def get_company_overview(self, ticker):
        """
        Get fundamental data for a stock
        Returns: dict with P/E, ROE, EPS growth, etc.
        """
        try:
            params = {
                'function': 'OVERVIEW',
                'symbol': ticker,
                'apikey': self.api_key
            }
            
            response = requests.get(self.base_url, params=params, timeout=10)
            data = response.json()
            
            if 'Symbol' not in data:
                logger.warning(f"No data for {ticker}")
                return None
            
            # Extract key metrics
            overview = {
                'ticker': ticker,
                'name': data.get('Name', ''),
                'sector': data.get('Sector', ''),
                'industry': data.get('Industry', ''),
                'market_cap': float(data.get('MarketCapitalization', 0)),
                'pe_ratio': float(data.get('PERatio', 0)) if data.get('PERatio') != 'None' else None,
                'peg_ratio': float(data.get('PEGRatio', 0)) if data.get('PEGRatio') != 'None' else None,
                'roe': float(data.get('ReturnOnEquityTTM', 0)) * 100 if data.get('ReturnOnEquityTTM') != 'None' else None,
                'eps': float(data.get('EPS', 0)) if data.get('EPS') != 'None' else None,
                'revenue_per_share': float(data.get('RevenuePerShareTTM', 0)) if data.get('RevenuePerShareTTM') != 'None' else None,
                'profit_margin': float(data.get('ProfitMargin', 0)) * 100 if data.get('ProfitMargin') != 'None' else None,
                'operating_margin': float(data.get('OperatingMarginTTM', 0)) * 100 if data.get('OperatingMarginTTM') != 'None' else None,
                'revenue_growth_yoy': float(data.get('QuarterlyRevenueGrowthYOY', 0)) * 100 if data.get('QuarterlyRevenueGrowthYOY') != 'None' else None,
                'eps_growth_yoy': float(data.get('QuarterlyEarningsGrowthYOY', 0)) * 100 if data.get('QuarterlyEarningsGrowthYOY') != 'None' else None,
                'beta': float(data.get('Beta', 1.0)) if data.get('Beta') != 'None' else 1.0,
                'price': float(data.get('50DayMovingAverage', 0)) if data.get('50DayMovingAverage') != 'None' else None
            }
            
            return overview
            
        except Exception as e:
            logger.error(f"Error fetching {ticker}: {e}")
            return None
    
    def screen_sector(self, sector_tickers, criteria):
        """
        Screen a list of tickers based on criteria
        
        Args:
            sector_tickers: list of ticker symbols
            criteria: dict with screening rules
        
        Returns:
            list of stocks that pass criteria
        """
        passed = []
        failed_count = 0
        
        for ticker in sector_tickers:
            try:
                # Rate limiting (Alpha Vantage: 5 calls/min on free tier)
                time.sleep(12)  # ~5 per minute
                
                stock = self.get_company_overview(ticker)
                
                if stock is None:
                    failed_count += 1
                    continue
                
                # Apply criteria
                if self._meets_criteria(stock, criteria):
                    passed.append(stock)
                    logger.info(f"✓ {ticker} passed screening")
                
                if len(passed) % 5 == 0 and len(passed) > 0:
                    logger.info(f"Progress: {len(passed)} stocks passed so far")
                    
            except Exception as e:
                logger.error(f"Error screening {ticker}: {e}")
                failed_count += 1
        
        logger.info(f"Screening complete: {len(passed)} passed, {failed_count} failed/skipped")
        return passed
    
    def _meets_criteria(self, stock, criteria):
        """Check if stock meets all criteria"""
        
        # Market cap check
        if 'min_market_cap' in criteria:
            if stock['market_cap'] < criteria['min_market_cap']:
                return False
        
        # Revenue growth check
        if 'min_revenue_growth' in criteria:
            if stock['revenue_growth_yoy'] is None or stock['revenue_growth_yoy'] < criteria['min_revenue_growth']:
                return False
        
        # EPS growth check
        if 'min_eps_growth' in criteria:
            if stock['eps_growth_yoy'] is None or stock['eps_growth_yoy'] < criteria['min_eps_growth']:
                return False
        
        # ROE check
        if 'min_roe' in criteria:
            if stock['roe'] is None or stock['roe'] < criteria['min_roe']:
                return False
        
        # PEG ratio check (for longs)
        if 'max_peg' in criteria:
            if stock['peg_ratio'] is None or stock['peg_ratio'] > criteria['max_peg']:
                return False
        
        # P/E ratio check
        if 'min_pe' in criteria:
            if stock['pe_ratio'] is None or stock['pe_ratio'] < criteria['min_pe']:
                return False
        
        if 'max_pe' in criteria:
            if stock['pe_ratio'] is None or stock['pe_ratio'] > criteria['max_pe']:
                return False
        
        return True
    
    def screen_longs(self, sector_tickers):
        """
        Screen for long candidates (growth, quality)
        
        Criteria from Professional Trading Masterclass:
        - Market Cap > $1B
        - Revenue Growth > 10% YoY
        - EPS Growth > 20% YoY
        - ROE > 15%
        - PEG < 1.5
        """
        criteria = {
            'min_market_cap': 1_000_000_000,  # $1B
            'min_revenue_growth': 10,  # 10%
            'min_eps_growth': 20,  # 20%
            'min_roe': 15,  # 15%
            'max_peg': 1.5
        }
        
        logger.info(f"Screening {len(sector_tickers)} stocks for LONG candidates")
        logger.info(f"Criteria: {criteria}")
        
        return self.screen_sector(sector_tickers, criteria)
    
    def screen_shorts(self, sector_tickers):
        """
        Screen for short candidates (weakness, overvaluation)
        
        Criteria:
        - Market Cap > $1B (liquid)
        - Revenue Growth < 5% or negative
        - EPS Growth < 5% or negative
        - ROE declining
        - P/E > 25 (overvalued)
        """
        criteria = {
            'min_market_cap': 1_000_000_000,  # $1B for liquidity
            'max_revenue_growth': 5,  # Weak growth
            'max_eps_growth': 5,  # Weak earnings
            'max_roe': 10,  # Poor returns
            'min_pe': 25  # Expensive
        }
        
        logger.info(f"Screening {len(sector_tickers)} stocks for SHORT candidates")
        logger.info(f"Criteria: {criteria}")
        
        results = []
        
        for ticker in sector_tickers:
            try:
                time.sleep(12)
                
                stock = self.get_company_overview(ticker)
                
                if stock is None:
                    continue
                
                # For shorts, we want OPPOSITE of quality
                if (stock['market_cap'] > criteria['min_market_cap'] and
                    stock['pe_ratio'] and stock['pe_ratio'] > criteria['min_pe']):
                    
                    # Check for weakness
                    weak_revenue = (stock['revenue_growth_yoy'] is None or 
                                  stock['revenue_growth_yoy'] < criteria['max_revenue_growth'])
                    weak_eps = (stock['eps_growth_yoy'] is None or 
                              stock['eps_growth_yoy'] < criteria['max_eps_growth'])
                    weak_roe = (stock['roe'] is None or 
                              stock['roe'] < criteria['max_roe'])
                    
                    if weak_revenue or weak_eps or weak_roe:
                        results.append(stock)
                        logger.info(f"✓ {ticker} is short candidate")
                        
            except Exception as e:
                logger.error(f"Error screening {ticker} for short: {e}")
        
        logger.info(f"Short screening complete: {len(results)} candidates")
        return results


# Example sector tickers (Top holdings in each sector)
SECTOR_TICKERS = {
    'Financials': ['JPM', 'BAC', 'WFC', 'C', 'GS', 'MS', 'BLK', 'SCHW', 'AXP', 'USB'],
    'Technology': ['AAPL', 'MSFT', 'NVDA', 'AVGO', 'ORCL', 'CSCO', 'ADBE', 'CRM', 'INTC', 'AMD'],
    'Industrials': ['CAT', 'BA', 'HON', 'UNP', 'RTX', 'DE', 'LMT', 'GE', 'MMM', 'UPS'],
    'Consumer Discretionary': ['AMZN', 'TSLA', 'HD', 'MCD', 'NKE', 'SBUX', 'TGT', 'LOW', 'TJX', 'BKNG'],
    'Health Care': ['UNH', 'JNJ', 'LLY', 'ABBV', 'MRK', 'PFE', 'TMO', 'ABT', 'DHR', 'BMY'],
    'Consumer Staples': ['PG', 'KO', 'PEP', 'WMT', 'COST', 'PM', 'MO', 'CL', 'MDLZ', 'KHC'],
    'Utilities': ['NEE', 'DUK', 'SO', 'D', 'AEP', 'EXC', 'SRE', 'XEL', 'ED', 'PEG'],
    'Energy': ['XOM', 'CVX', 'COP', 'SLB', 'EOG', 'MPC', 'PSX', 'VLO', 'OXY', 'HAL'],
    'Materials': ['LIN', 'APD', 'ECL', 'SHW', 'FCX', 'NEM', 'DD', 'DOW', 'NUE', 'VMC'],
    'Real Estate': ['PLD', 'AMT', 'CCI', 'EQIX', 'PSA', 'SPG', 'O', 'WELL', 'DLR', 'AVB']
}
