import json
import logging
from logging.handlers import RotatingFileHandler
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from ctrader_open_api import EndPoints

# Application credentials
APP_CLIENT_ID = "14154_kdUcmqKwgpIjkbSYljZn9ORIiHy9n0zHzyg7P3GvaEuPKLWmIv"
APP_CLIENT_SECRET = "iSj0GHcETjIsrHna91Pwe4r1L2k6Dt0ocq7r2u32muS6iEshSD"
ACCESS_TOKEN = "f2cG74o0Qh4Ty17zs6W0WaUXHSu5Ozg8Sf_OL9xXrwY"
REFRESH_TOKEN = "CSlI4r97y_LSL7lFJ_lBCH0MJ8fTlbfI2qbNUoqUMtU"

# Global trade flag for synchronized trading state management
GLOBAL_TRADE_OPEN = False  # True when trade is open, False when closed
GLOBAL_TRADE_LOCK = threading.Lock()  # Thread lock for synchronizing trade state

# cTrader API settings
HOST = EndPoints.PROTOBUF_DEMO_HOST
PORT = EndPoints.PROTOBUF_PORT

# Trading constants
PRICE_SCALE_FACTOR = 100000.0
VOLUME_CONVERSION_FACTOR = 100.0
WINDOW_SIZE = 100
DEFAULT_SPREAD = 0.00002  # 0.2 pips fallback
PIP_VALUE = 0.0001  # Standard pip value for 4-digit pairs
SOH = "\x01"  # Start of Header for FIX messages

# Commission settings
COMMISSION_PER_LOT_USD = 7.0  # IC Markets round-trip per lot
USD_GBP_CONVERSION_RATE = 0.76  # Default fallback rate

# Trading parameters
INITIAL_LOT_SIZE = 25  # This will be shown as 0.25 lots (25/100)
MAX_LOT_SIZE = 200  # This will be shown as 2.00 lots (200/100) - Conservative limit
LOT_SIZE_INCREMENT = 10  # Increment amount for lot size scaling (10 internal units = 0.10 lots)

# Fixed Stop Loss and Take Profit (simplified - no dynamic logic)
STOP_LOSS_PIPS = 10.0  # Fixed 10 pip stop loss
TAKE_PROFIT_PIPS = 20.0  # Fixed 20 pip take profit (2:1 R:R)

# Basic filter thresholds
MIN_SPREAD_THRESHOLD = 2.0  # Maximum spread in pips to allow trading
MAX_LATENCY_THRESHOLD = 150.0  # Maximum latency in milliseconds
MIN_CONFIDENCE_SCORE = 0.7  # Basic confidence threshold for trades

# Session management
SESSION_DURATION = 60  # Default session duration in minutes

# Load FIX TRADE session config
import os
config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config-trade.json")
with open(config_path) as f:
    CONFIG_TRADE = json.load(f)

# Simplified symbol configuration - start with major forex pairs
ACTIVE_SYMBOLS = {
    1: {"name": "EURUSD", "pip_value": 0.0001, "commission": 7.0, "contract_size": 100000},
    2: {"name": "GBPUSD", "pip_value": 0.0001, "commission": 7.0, "contract_size": 100000},
    4: {"name": "USDJPY", "pip_value": 0.01, "commission": 7.0, "contract_size": 100000},
    5: {"name": "AUDUSD", "pip_value": 0.0001, "commission": 7.0, "contract_size": 100000},
}

# Alias for compatibility
SYMBOLS = ACTIVE_SYMBOLS

# Thread pool for multi-symbol processing
THREAD_POOL_SIZE = len(ACTIVE_SYMBOLS)

# Performance monitoring
ENABLE_PERFORMANCE_MONITORING = True
LOG_LEVEL = logging.INFO

# Basic logging setup
def setup_logging():
    """Setup basic logging for the application"""
    
    # Create logs directory if it doesn't exist
    logs_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    if not os.path.exists(logs_path):
        os.makedirs(logs_path)
    
    # Configure root logger
    logging.basicConfig(
        level=LOG_LEVEL,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),  # Console output
            RotatingFileHandler(os.path.join(logs_path, 'hft_base_lite.log'), maxBytes=50*1024*1024, backupCount=5)
        ]
    )
    
    # Create symbol-specific loggers
    symbol_loggers = {}
    for symbol_id, symbol_info in ACTIVE_SYMBOLS.items():
        symbol_name = symbol_info['name']
        logger = logging.getLogger(f'Symbol_{symbol_name}')
        logger.setLevel(LOG_LEVEL)
        
        # Add file handler for this symbol
        handler = RotatingFileHandler(
            os.path.join(logs_path, f'{symbol_name.lower()}_trades.log'), 
            maxBytes=10*1024*1024, 
            backupCount=3
        )
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        symbol_loggers[symbol_id] = logger
    
    return symbol_loggers

# Initialize symbol loggers
symbol_loggers = setup_logging()

# Simple timing decorator
import time
import functools

def timing_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        try:
            result = func(*args, **kwargs)
            execution_time = time.perf_counter() - start_time
            
            # Log slow operations
            if execution_time > 0.1:  # Log operations taking more than 100ms
                logging.warning(f"Slow operation: {func.__name__} took {execution_time*1000:.2f}ms")
            
            return result
        except Exception as e:
            execution_time = time.perf_counter() - start_time
            logging.error(f"Error in {func.__name__}: {str(e)} (took {execution_time*1000:.2f}ms)")
            raise
    return wrapper 