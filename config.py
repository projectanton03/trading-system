"""
Configuration for Trading System - Full System
Centralized configuration management
"""

import os
from pathlib import Path
import base64
import json

# Base directory
BASE_DIR = Path(__file__).resolve().parent

#═══════════════════════════════════════════════════════════════════════════════
# API KEYS
#═══════════════════════════════════════════════════════════════════════════════

# AI & Data APIs
GROQ_API_KEY = os.environ.get('GROQ_API_KEY')
ALPHA_VANTAGE_KEY = os.environ.get('ALPHA_VANTAGE_KEY')
FRED_API_KEY = os.environ.get('FRED_API_KEY')

# Communication
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

#═══════════════════════════════════════════════════════════════════════════════
# GOOGLE DRIVE AUTHENTICATION
#═══════════════════════════════════════════════════════════════════════════════

# Google Drive - Handle both file and base64 encoding
GOOGLE_SERVICE_ACCOUNT_BASE64 = os.environ.get('GOOGLE_SERVICE_ACCOUNT_BASE64')

if GOOGLE_SERVICE_ACCOUNT_BASE64:
    # Decode base64 and save to temp file
    import tempfile
    
    try:
        decoded = base64.b64decode(GOOGLE_SERVICE_ACCOUNT_BASE64)
        
        # Create a persistent temp file
        fd, temp_path = tempfile.mkstemp(suffix='.json', text=True)
        
        with os.fdopen(fd, 'w') as temp_file:
            temp_file.write(decoded.decode('utf-8'))
        
        GOOGLE_SERVICE_ACCOUNT_FILE = temp_path
        print(f"✅ Google service account loaded from base64 → {temp_path}")
        
    except Exception as e:
        print(f"❌ Error decoding GOOGLE_SERVICE_ACCOUNT_BASE64: {e}")
        GOOGLE_SERVICE_ACCOUNT_FILE = str(BASE_DIR / 'data' / 'google-service-account.json')
else:
    # Fallback to local file
    GOOGLE_SERVICE_ACCOUNT_FILE = os.environ.get(
        'GOOGLE_SERVICE_ACCOUNT_FILE',
        str(BASE_DIR / 'data' / 'google-service-account.json')
    )
    print(f"📁 Using local service account file: {GOOGLE_SERVICE_ACCOUNT_FILE}")

#═══════════════════════════════════════════════════════════════════════════════
# GOOGLE DRIVE FOLDER IDS
# UPDATE THESE with your actual Google Drive folder IDs after Day 1
#═══════════════════════════════════════════════════════════════════════════════

DRIVE_FOLDERS = {
    # Macro Indicators
    'macro_leading': os.environ.get('DRIVE_MACRO_LEADING', '15HzzWo006M1_JZI0Q9z72hbPkiOqQ2Qs'),
    'macro_coincident': os.environ.get('DRIVE_MACRO_COINCIDENT', '1AEiZ7kl6daGBBk9d524ooMV0cr14M6t2'),
    'macro_international': os.environ.get('DRIVE_MACRO_INTL', '1Aq4fBvJ1Fxd_6STL-olz1zuCeHsW9y40'),
    'macro_reference_analysis': os.environ.get('DRIVE_MACRO_REF', '1fC4Ms-NRcYvqq--qbuzgu9FFbR_Myd6X'),
    
    # Portfolio Management
    'portfolio_mgmt': os.environ.get('DRIVE_PORTFOLIO', '1KELQPhWMZQ6SArUdxHxLyFuxlK4p3IYK'),
    
    # Risk & Performance
    'risk_performance': os.environ.get('DRIVE_RISK', '1R_Ku1U2VoxNnKB5D9Wj6ajhnCUJYBRQy'),
    
    # Outputs
    'outputs': os.environ.get('DRIVE_OUTPUTS', '1Fqd223RMG9oZzKld7H55l68KSYayyZIL')
}

# Key template file IDs (update after Day 1)
KEY_FILES = {
    'us_sector_data': os.environ.get('FILE_US_SECTOR_DATA', '11UwhrI8uUdr7ngWy_87rizWBEejLCdqo'),
    'eg_profiles_longs': os.environ.get('FILE_EG_LONGS', '1H01hgwiWPeIiBskgADZzA48d4yhvKSQV'),
    'eg_profiles_shorts': os.environ.get('FILE_EG_SHORTS', '1-QG7YSlClOT0VYZx2cIlZ7y6lOxpyVBm'),
    'ism_manufacturing': os.environ.get('FILE_ISM_MFG', '1o8eHxS_8V-tOgW_4lrOMCZ9FGCclGyrO'),
    'treasury_yields': os.environ.get('FILE_YIELDS', '1I3f36ghjh-NpI_EyhlZ9JTNUnGIWDkg4'),
    'portfolio_monitor': os.environ.get('FILE_PORTFOLIO_MON', 'UPDATE_ME'),
    'trading_stats': os.environ.get('FILE_TRADING_STATS', '1okQcj9021zMMY7NZ1xdNdDdUc3-natSX'),
    'beta_spreadsheet': os.environ.get('FILE_BETA', '1Zai1jl6cUri0uVbAa7vWnwYl-euMu5i8')
}

#═══════════════════════════════════════════════════════════════════════════════
# PORTFOLIO SETTINGS
#═══════════════════════════════════════════════════════════════════════════════

# Capital
PORTFOLIO_VALUE = float(os.environ.get('PORTFOLIO_VALUE', 100000))

# Position Limits
MAX_POSITION_PCT = float(os.environ.get('MAX_POSITION_PCT', 0.20))  # 20% max
MAX_SECTOR_PCT = float(os.environ.get('MAX_SECTOR_PCT', 0.30))      # 30% max

# Beta Targets (based on macro regime)
TARGET_BETA_BULLISH = (0.50, 0.70)    # Bullish: Higher beta OK
TARGET_BETA_NEUTRAL = (0.30, 0.50)    # Neutral: Moderate beta
TARGET_BETA_BEARISH = (0.00, 0.20)    # Bearish: Low/zero beta

# Exposure Limits
MAX_GROSS_EXPOSURE = float(os.environ.get('MAX_GROSS', 2.00))  # 200%
MAX_NET_EXPOSURE = float(os.environ.get('MAX_NET', 0.70))      # 70%

#═══════════════════════════════════════════════════════════════════════════════
# RISK MANAGEMENT SETTINGS
#═══════════════════════════════════════════════════════════════════════════════

# Position Sizing
RISK_PER_TRADE = float(os.environ.get('RISK_PER_TRADE', 0.015))  # 1.5%
KELLY_MULTIPLIER = float(os.environ.get('KELLY_MULT', 0.50))     # Half Kelly

# Stop Loss
ATRP_MULTIPLIER_STOP = float(os.environ.get('ATRP_STOP', 2.5))   # 2.5x ATRP
ATRP_MULTIPLIER_TARGET = float(os.environ.get('ATRP_TARGET', 7.5))  # 7.5x ATRP
MIN_RISK_REWARD = float(os.environ.get('MIN_RR', 3.0))           # 3:1 minimum

# Drawdown Triggers
ALERT_DRAWDOWN = float(os.environ.get('ALERT_DD', 0.05))         # 5% alert
WARNING_DRAWDOWN = float(os.environ.get('WARN_DD', 0.10))        # 10% warning
CRITICAL_DRAWDOWN = float(os.environ.get('CRIT_DD', 0.15))       # 15% critical

#═══════════════════════════════════════════════════════════════════════════════
# MACRO REGIME SETTINGS
#═══════════════════════════════════════════════════════════════════════════════

# ISM Thresholds
ISM_EXPANSION = 53.0
ISM_CONTRACTION = 48.0

# Regime Determination Weights
MACRO_WEIGHTS = {
    'ism_manufacturing': 0.25,
    'ism_services': 0.15,
    'yield_curve': 0.20,
    'credit_spreads': 0.15,
    'consumer_confidence': 0.10,
    'housing': 0.10,
    'international': 0.05
}

# Sector Recommendations by Regime
SECTOR_PREFERENCES = {
    'EXPANSION': {
        'longs': ['Financials', 'Industrials', 'Materials', 'Technology'],
        'shorts': ['Utilities', 'Consumer Staples']
    },
    'LATE_CYCLE': {
        'longs': ['Technology', 'Healthcare', 'Consumer Staples'],
        'shorts': ['Financials', 'Materials']
    },
    'RECESSION': {
        'longs': ['Utilities', 'Consumer Staples', 'Healthcare'],
        'shorts': ['Financials', 'Industrials', 'Materials', 'Consumer Discretionary']
    },
    'RECOVERY': {
        'longs': ['Industrials', 'Materials', 'Financials', 'Real Estate'],
        'shorts': ['Utilities']
    }
}

#═══════════════════════════════════════════════════════════════════════════════
# SCREENING CRITERIA
#═══════════════════════════════════════════════════════════════════════════════

# Fundamental Screens (Longs)
SCREEN_LONGS = {
    'min_market_cap': 1_000_000_000,      # $1B minimum
    'min_eps_growth': 20.0,                # 20% EPS growth
    'min_roe': 15.0,                       # 15% ROE
    'max_peg': 1.5,                        # PEG < 1.5
    'revenue_vs_sector': 'above',          # Above sector average
}

# Fundamental Screens (Shorts)
SCREEN_SHORTS = {
    'min_market_cap': 1_000_000_000,
    'max_eps_growth': 5.0,                 # Low/negative growth
    'eps_deceleration': True,              # Must be decelerating
    'revenue_vs_sector': 'below',          # Below sector average
    'pe_vs_sector': 1.5,                   # P/E > 1.5x sector = overvalued
}

# EG Profile Settings
EG_PROFILES = {
    'longs': [1, 2, 4, 5],     # Accelerating/Outperforming profiles
    'shorts': [1, 2, 3, 4]     # Decelerating/Underperforming profiles
}

# Technical Filters
TECHNICAL_FILTERS = {
    'longs': {
        'require_uptrend': True,           # Price > 50 EMA > 200 EMA
        'max_distance_from_ema': 0.05,     # Within 5% of 50-EMA for entry
        'min_atrp': 0.015,                 # Minimum 1.5% ATRP
        'max_atrp': 0.08                   # Maximum 8% ATRP
    },
    'shorts': {
        'require_downtrend': False,        # Can short overextended stocks
        'overextension_threshold': 1.20,   # >20% above 200-EMA
        'min_atrp': 0.015,
        'max_atrp': 0.08
    }
}

#═══════════════════════════════════════════════════════════════════════════════
# GROQ AI SETTINGS
#═══════════════════════════════════════════════════════════════════════════════

GROQ_MODEL = 'llama-3.1-70b-versatile'
GROQ_TEMPERATURE = 0.3  # Lower for more consistent analysis
GROQ_MAX_TOKENS = 1000

# Prompt templates
PROMPTS = {
    'macro_analysis': """Analyze these macro indicators and provide investment conclusion:

{indicators}

Provide in this exact format:
1. Overall Assessment (2-3 sentences)
2. Recommended Long Sectors (be specific)
3. Recommended Short Sectors (be specific)
4. Key Risks (bullet points)

Be concise and actionable.""",

    'stock_ranking': """Rank these stocks (1=best) for {direction} positions:

{stock_data}

Macro Context: {macro_context}

Provide:
- Rank (1-5)
- Brief reasoning for each (one sentence)
Format as: "1. TICKER - Reason" """,

    'trade_thesis': """Write a concise 2-3 sentence investment thesis:

Stock: {company} ({ticker})
Sector: {sector}
Fundamentals: P/E {pe}, ROE {roe}%, EPS Growth {eps_growth}%
Catalyst: {catalyst}
Key Factor: {key_factor}

Professional investment memo style. Be specific about catalysts and timing."""
}

#═══════════════════════════════════════════════════════════════════════════════
# SCHEDULE SETTINGS
#═══════════════════════════════════════════════════════════════════════════════

# Workflow schedules (cron format)
SCHEDULES = {
    'weekly_macro': '0 18 * * 0',          # Sunday 6pm
    'daily_monitor': '15 16 * * 1-5',      # Weekdays 4:15pm
    'monthly_report': '0 18 1 * *'         # First day of month 6pm
}

#═══════════════════════════════════════════════════════════════════════════════
# VALIDATION
#═══════════════════════════════════════════════════════════════════════════════

def validate_config():
    """Validate that all required configuration is present"""
    
    errors = []
    
    # Check required API keys
    required_keys = {
        'GROQ_API_KEY': GROQ_API_KEY,
        'ALPHA_VANTAGE_KEY': ALPHA_VANTAGE_KEY,
        'FRED_API_KEY': FRED_API_KEY
    }
    
    for key_name, key_value in required_keys.items():
        if not key_value:
            errors.append(f"Missing required environment variable: {key_name}")
    
    # Check Google service account
    if GOOGLE_SERVICE_ACCOUNT_BASE64:
        # Using base64 - validate it decodes properly
        try:
            decoded = base64.b64decode(GOOGLE_SERVICE_ACCOUNT_BASE64)
            json.loads(decoded)  # Ensure valid JSON
        except Exception as e:
            errors.append(f"Invalid GOOGLE_SERVICE_ACCOUNT_BASE64: {e}")
    else:
        # Using file path - check it exists
        if not os.path.exists(GOOGLE_SERVICE_ACCOUNT_FILE):
            errors.append(f"Google service account file not found: {GOOGLE_SERVICE_ACCOUNT_FILE}")
    
    # Warnings for folder IDs (not critical for initial setup)
    if all(v == 'UPDATE_ME' for v in DRIVE_FOLDERS.values()):
        print("WARNING: Google Drive folder IDs not configured yet. Update after Day 1.")
    
    # Raise errors if any
    if errors:
        raise ValueError(f"Configuration errors:\n" + "\n".join(f"  - {e}" for e in errors))
    
    return True

def get_beta_target(regime):
    """Get beta target range based on macro regime"""
    targets = {
        'EXPANSION': TARGET_BETA_BULLISH,
        'LATE_CYCLE': TARGET_BETA_NEUTRAL,
        'RECESSION': TARGET_BETA_BEARISH,
        'RECOVERY': TARGET_BETA_NEUTRAL
    }
    return targets.get(regime, TARGET_BETA_NEUTRAL)

def get_sector_preferences(regime):
    """Get sector preferences based on macro regime"""
    return SECTOR_PREFERENCES.get(regime, SECTOR_PREFERENCES['EXPANSION'])

#═══════════════════════════════════════════════════════════════════════════════
# ENVIRONMENT-SPECIFIC OVERRIDES
#═══════════════════════════════════════════════════════════════════════════════

if os.environ.get('FLASK_ENV') == 'development':
    # Development settings
    print("Running in DEVELOPMENT mode")
    GROQ_MAX_TOKENS = 500  # Faster for testing

elif os.environ.get('FLASK_ENV') == 'production':
    # Production settings
    print("Running in PRODUCTION mode")
    # Could add production-specific overrides here
