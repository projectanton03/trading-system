# services/macro_analysis.py
"""
Macro Regime Analysis Engine
Determines current macroeconomic regime and recommends sector positioning
"""

import pandas as pd
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Regime to sector mappings
REGIME_TO_LONGS = {
    'EXPANSION': ['Financials', 'Industrials', 'Technology', 'Consumer Discretionary'],
    'LATE_CYCLE': ['Energy', 'Materials', 'Financials'],
    'RECESSION': ['Utilities', 'Consumer Staples', 'Health Care'],
    'RECOVERY': ['Financials', 'Industrials', 'Technology', 'Materials']
}

REGIME_TO_SHORTS = {
    'EXPANSION': ['Utilities', 'Consumer Staples'],
    'LATE_CYCLE': ['Technology', 'Consumer Discretionary', 'Industrials'],
    'RECESSION': ['Financials', 'Industrials', 'Consumer Discretionary', 'Energy'],
    'RECOVERY': ['Utilities', 'Consumer Staples']
}

# Indicator weights
WEIGHTS = {
    'yield_curve': 0.30,
    'ism': 0.25,
    'credit_spread': 0.20,
    'sentiment': 0.15,
    'permits': 0.05,
    'claims': 0.05
}


class MacroAnalyzer:
    """Analyzes macro indicators to determine regime"""
    
    def __init__(self, excel_handler, google_drive):
        self.excel = excel_handler
        self.drive = google_drive
        
    def get_yield_curve_spread(self):
        """
        Calculate 10yr - 2yr spread from Benchmark_Yields_US
        Returns: (spread_value, trend, signal)
        """
        try:
            file_id = '1I3f36ghjh-NpI_EyhlZ9JTNUnGIWDkg4'
            df = self.excel.read_sheet(file_id, 'Data', rows=10)
            
            if df.empty:
                return None, None, 0
            
            # Get most recent data (row 4)
            latest = df.iloc[0]
            yr10 = float(latest['K']) if 'K' in latest and latest['K'] else None
            yr2 = float(latest['G']) if 'G' in latest and latest['G'] else None
            
            if yr10 is None or yr2 is None:
                return None, None, 0
            
            spread = yr10 - yr2
            
            # Get previous spread to determine trend
            if len(df) > 1:
                prev = df.iloc[1]
                prev_yr10 = float(prev['K']) if 'K' in prev and prev['K'] else yr10
                prev_yr2 = float(prev['G']) if 'G' in prev and prev['G'] else yr2
                prev_spread = prev_yr10 - prev_yr2
                trend = 'STEEPENING' if spread > prev_spread else 'FLATTENING'
            else:
                trend = 'STABLE'
            
            # Determine signal
            if spread > 1.0 and trend == 'STEEPENING':
                signal = 2  # Strong expansion
            elif spread > 0.5:
                signal = 1  # Moderate expansion
            elif spread < 0:
                signal = -2  # Inverted = recession warning
            elif spread < 0.3:
                signal = -1  # Flattening = late cycle
            else:
                signal = 0
            
            return spread, trend, signal
            
        except Exception as e:
            logger.error(f"Error getting yield curve: {e}")
            return None, None, 0
    
    def get_ism_value(self):
        """
        Get current ISM Manufacturing from UMCSI file
        Returns: (value, trend, signal)
        """
        try:
            # Note: Need to backfill ISM_Manufacturing first
            # For now, return placeholder
            logger.warning("ISM data not yet available - using placeholder")
            return 52.0, 'RISING', 1
            
        except Exception as e:
            logger.error(f"Error getting ISM: {e}")
            return None, None, 0
    
    def get_consumer_sentiment(self):
        """
        Get current UMCSI value
        Returns: (value, trend, signal)
        """
        try:
            file_id = '18ExFmLHORm7boVpCzmNR7AZYK5RQ68-T'
            df = self.excel.read_sheet(file_id, 'UMCSI_VS_SP500', rows=5)
            
            if df.empty:
                return None, None, 0
            
            # Get latest value (row 2)
            latest = df.iloc[0]
            value = float(latest['B']) if 'B' in latest and latest['B'] else None
            
            if value is None:
                return None, None, 0
            
            # Determine trend
            if len(df) > 1:
                prev = df.iloc[1]
                prev_value = float(prev['B']) if 'B' in prev and prev['B'] else value
                trend = 'RISING' if value > prev_value else 'FALLING'
            else:
                trend = 'STABLE'
            
            # Determine signal
            if value > 90 and trend == 'RISING':
                signal = 2  # Strong expansion
            elif value > 80:
                signal = 1  # Moderate
            elif value < 70:
                signal = -2  # Recession
            elif value < 80 and trend == 'FALLING':
                signal = -1  # Late cycle
            else:
                signal = 0
            
            return value, trend, signal
            
        except Exception as e:
            logger.error(f"Error getting sentiment: {e}")
            return None, None, 0
    
    def score_regime(self, regime, indicators):
        """
        Score how well indicators match a specific regime
        Returns: confidence score (0.0 to 1.0)
        """
        score = 0.0
        
        # Weighted scoring based on each indicator
        for indicator, weight in WEIGHTS.items():
            if indicator in indicators and indicators[indicator]['signal'] is not None:
                signal = indicators[indicator]['signal']
                
                # Adjust signal based on regime expectations
                regime_score = self._get_regime_specific_score(regime, indicator, signal)
                score += regime_score * weight
        
        # Normalize to 0-1 range
        normalized = (score + 2.0) / 4.0  # Convert from -2 to +2 range to 0-1
        return max(0.0, min(1.0, normalized))
    
    def _get_regime_specific_score(self, regime, indicator, signal):
        """
        Adjust indicator signal based on what each regime expects
        """
        # Positive signal = matches regime, negative = contradicts
        
        if regime == 'EXPANSION':
            # All positive signals support expansion
            return signal
        
        elif regime == 'LATE_CYCLE':
            # Mixed signals - some positive (economy still growing), some negative (slowing)
            if indicator == 'yield_curve' and signal < 0:
                return abs(signal)  # Flattening supports late cycle
            elif indicator in ['ism', 'sentiment'] and signal > 0:
                return signal * 0.5  # Still positive but weakening
            return signal * 0.5
        
        elif regime == 'RECESSION':
            # Negative signals support recession
            return -signal  # Invert (negative becomes positive)
        
        elif regime == 'RECOVERY':
            # Early positive signals from low levels
            if indicator in ['sentiment', 'permits'] and signal > 0:
                return signal * 1.5  # Rising from lows very positive
            return signal
        
        return 0
    
    def analyze_regime(self):
        """
        Main function to determine current macro regime
        Returns dict with regime, confidence, and sector recommendations
        """
        try:
            logger.info("Starting macro regime analysis...")
            
            # 1. Collect all indicators
            indicators = {}
            
            # Yield curve
            spread, trend, signal = self.get_yield_curve_spread()
            indicators['yield_curve'] = {
                'value': spread,
                'trend': trend,
                'signal': signal,
                'description': f"{spread:.2f}% ({trend})" if spread else "N/A"
            }
            
            # ISM
            ism, ism_trend, ism_signal = self.get_ism_value()
            indicators['ism'] = {
                'value': ism,
                'trend': ism_trend,
                'signal': ism_signal,
                'description': f"{ism:.1f} ({ism_trend})" if ism else "N/A"
            }
            
            # Consumer Sentiment
            sent, sent_trend, sent_signal = self.get_consumer_sentiment()
            indicators['sentiment'] = {
                'value': sent,
                'trend': sent_trend,
                'signal': sent_signal,
                'description': f"{sent:.1f} ({sent_trend})" if sent else "N/A"
            }
            
            # Placeholders for other indicators
            indicators['credit_spread'] = {'value': None, 'trend': None, 'signal': 0, 'description': 'N/A'}
            indicators['permits'] = {'value': None, 'trend': None, 'signal': 0, 'description': 'N/A'}
            indicators['claims'] = {'value': None, 'trend': None, 'signal': 0, 'description': 'N/A'}
            
            # 2. Score each regime
            scores = {
                'EXPANSION': self.score_regime('EXPANSION', indicators),
                'LATE_CYCLE': self.score_regime('LATE_CYCLE', indicators),
                'RECESSION': self.score_regime('RECESSION', indicators),
                'RECOVERY': self.score_regime('RECOVERY', indicators)
            }
            
            # 3. Determine regime (highest score)
            regime = max(scores, key=scores.get)
            confidence = scores[regime]
            
            # 4. Get sector recommendations
            long_sectors = REGIME_TO_LONGS[regime]
            short_sectors = REGIME_TO_SHORTS[regime]
            
            result = {
                'regime': regime,
                'confidence': round(confidence, 2),
                'long_sectors': long_sectors,
                'short_sectors': short_sectors,
                'scores': {k: round(v, 2) for k, v in scores.items()},
                'indicators': indicators,
                'timestamp': datetime.now().isoformat()
            }
            
            logger.info(f"Regime determined: {regime} (confidence: {confidence:.0%})")
            
            return result
            
        except Exception as e:
            logger.error(f"Error in macro analysis: {e}")
            raise
