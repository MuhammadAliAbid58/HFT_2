import time
import json
import logging
import threading
import os
from collections import deque
from typing import Dict, List, Optional
from datetime import datetime
from .globals import SYMBOLS, timing_decorator

class DataLogger:
    """
    Data logger for tick and DOM data with timestamp tracking and latency analysis.
    Logs data to separate files and provides latency statistics.
    """
    
    def __init__(self):
        self.logger = logging.getLogger('DataLogger')
        
        # Create data logs directory
        self.data_logs_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data_logs")
        if not os.path.exists(self.data_logs_path):
            os.makedirs(self.data_logs_path)
        
        # Thread safety
        self.tick_lock = threading.Lock()
        self.dom_lock = threading.Lock()
        
        # Latency tracking
        self.tick_latencies = {symbol_id: deque(maxlen=1000) for symbol_id in SYMBOLS.keys()}
        self.dom_latencies = {symbol_id: deque(maxlen=1000) for symbol_id in SYMBOLS.keys()}
        
        # Data counters
        self.tick_counts = {symbol_id: 0 for symbol_id in SYMBOLS.keys()}
        self.dom_counts = {symbol_id: 0 for symbol_id in SYMBOLS.keys()}
        
        # Log files for each symbol
        self.tick_log_files = {}
        self.dom_log_files = {}
        
        # Initialize log files
        self._initialize_log_files()
        
        self.logger.info("DataLogger initialized with tick and DOM logging")
    
    def _initialize_log_files(self):
        """Initialize log files for each symbol"""
        current_date = datetime.now().strftime("%Y%m%d")
        
        for symbol_id, symbol_info in SYMBOLS.items():
            symbol_name = symbol_info['name']
            
            # Tick data log file
            tick_filename = f"tick_data_{symbol_name.lower()}_{current_date}.log"
            tick_filepath = os.path.join(self.data_logs_path, tick_filename)
            
            # DOM data log file
            dom_filename = f"dom_data_{symbol_name.lower()}_{current_date}.log"
            dom_filepath = os.path.join(self.data_logs_path, dom_filename)
            
            # Open files in append mode
            try:
                self.tick_log_files[symbol_id] = open(tick_filepath, 'a', encoding='utf-8')
                self.dom_log_files[symbol_id] = open(dom_filepath, 'a', encoding='utf-8')
                
                # Write headers if files are new - UPDATED to include all three timestamps
                if os.path.getsize(tick_filepath) == 0:
                    self.tick_log_files[symbol_id].write("source_timestamp_ms,received_timestamp_ms,log_timestamp_ms,symbol,price,bid,ask,spread,latency_ms,direction,previous_price\n")
                    self.tick_log_files[symbol_id].flush()
                
                if os.path.getsize(dom_filepath) == 0:
                    self.dom_log_files[symbol_id].write("source_timestamp_ms,received_timestamp_ms,log_timestamp_ms,symbol,side,price,volume,latency_ms,total_quotes\n")
                    self.dom_log_files[symbol_id].flush()
                
                self.logger.info(f"Log files initialized for {symbol_name}: tick={tick_filename}, dom={dom_filename}")
                
            except Exception as e:
                self.logger.error(f"Error initializing log files for {symbol_name}: {e}")
    
    @timing_decorator
    def log_tick_data(self, symbol_id: int, tick_data: Dict, received_timestamp: float):
        """
        Log tick data with latency analysis
        
        Args:
            symbol_id: Symbol identifier
            tick_data: Tick data dictionary
            received_timestamp: When the data was received by the system
        """
        try:
            with self.tick_lock:
                current_time = time.time()
                current_time_ms = int(current_time * 1000)
                received_timestamp_ms = int(received_timestamp * 1000)
                
                # Extract source timestamp from tick data
                source_timestamp_ms = 0
                if 'timestamp' in tick_data:
                    source_timestamp = tick_data['timestamp']
                    
                    # Check if source timestamp is in milliseconds (typical for APIs)
                    if source_timestamp > 1e12:  # If > year 2001 in milliseconds
                        source_timestamp_ms = int(source_timestamp)
                        # Calculate latency in milliseconds
                        latency_ms = received_timestamp_ms - source_timestamp_ms
                    else:
                        # Source timestamp is already in seconds, convert to ms for logging
                        source_timestamp_ms = int(source_timestamp * 1000)
                        # Calculate latency in milliseconds
                        latency_ms = received_timestamp_ms - source_timestamp_ms
                else:
                    # No source timestamp available, use received timestamp as fallback
                    source_timestamp_ms = received_timestamp_ms
                    # Calculate latency as processing delay only (in milliseconds)
                    latency_ms = current_time_ms - received_timestamp_ms
                
                # Store latency for analysis
                self.tick_latencies[symbol_id].append(latency_ms)
                self.tick_counts[symbol_id] += 1
                
                # Format data for logging
                symbol_name = SYMBOLS[symbol_id]['name']
                price = tick_data.get('price', 0)
                bid = tick_data.get('bid', 0)
                ask = tick_data.get('ask', 0)
                spread = tick_data.get('spread', 0)
                direction = tick_data.get('direction', 0)
                previous_price = tick_data.get('previous_price', 0)
                
                # Create log entry with all three timestamps (all in milliseconds)
                log_entry = f"{source_timestamp_ms},{received_timestamp_ms},{current_time_ms},{symbol_name},{price:.5f},{bid:.5f},{ask:.5f},{spread:.5f},{latency_ms:.3f},{direction},{previous_price:.5f}\n"
                
                # Write to file
                if symbol_id in self.tick_log_files:
                    self.tick_log_files[symbol_id].write(log_entry)
                    self.tick_log_files[symbol_id].flush()
                
                # Log high latency warnings
                if latency_ms > 10:  # More than 10ms
                    self.logger.warning(f"High tick latency for {symbol_name}: {latency_ms:.2f}ms")
                
                self.logger.debug(f"Tick logged for {symbol_name}: price={price:.5f}, latency={latency_ms:.2f}ms, source_ts={source_timestamp_ms}, received_ts={received_timestamp_ms}, log_ts={current_time_ms}")
                
        except Exception as e:
            self.logger.error(f"Error logging tick data for symbol {symbol_id}: {e}")
    
    @timing_decorator
    def log_dom_data(self, symbol_id: int, quotes: List[Dict], received_timestamp: float):
        """
        Log DOM data with latency analysis
        
        Args:
            symbol_id: Symbol identifier  
            quotes: List of quote dictionaries
            received_timestamp: When the data was received by the system
        """
        try:
            with self.dom_lock:
                current_time = time.time()
                current_time_ms = int(current_time * 1000)
                received_timestamp_ms = int(received_timestamp * 1000)
                
                # For DOM data, we typically don't have individual source timestamps per quote
                # Use received timestamp as source timestamp for DOM data
                source_timestamp_ms = received_timestamp_ms
                
                # Calculate latency for DOM data (processing delay) in milliseconds
                latency_ms = current_time_ms - received_timestamp_ms
                
                # Store latency for analysis
                self.dom_latencies[symbol_id].append(latency_ms)
                self.dom_counts[symbol_id] += 1
                
                symbol_name = SYMBOLS[symbol_id]['name']
                
                # Log each quote in the DOM update
                for quote in quotes:
                    side = quote.get('side', '')
                    price = quote.get('price', 0)
                    volume = quote.get('volume', 0)
                    
                    # Create log entry with all three timestamps (all in milliseconds)
                    log_entry = f"{source_timestamp_ms},{received_timestamp_ms},{current_time_ms},{symbol_name},{side},{price:.5f},{volume},{latency_ms:.3f},{len(quotes)}\n"
                    
                    # Write to file
                    if symbol_id in self.dom_log_files:
                        self.dom_log_files[symbol_id].write(log_entry)
                
                # Flush after writing all quotes
                if symbol_id in self.dom_log_files:
                    self.dom_log_files[symbol_id].flush()
                
                # Log high latency warnings
                if latency_ms > 5:  # More than 5ms
                    self.logger.warning(f"High DOM latency for {symbol_name}: {latency_ms:.2f}ms")
                
                self.logger.debug(f"DOM logged for {symbol_name}: {len(quotes)} quotes, latency={latency_ms:.2f}ms, source_ts={source_timestamp_ms}, received_ts={received_timestamp_ms}, log_ts={current_time_ms}")
                
        except Exception as e:
            self.logger.error(f"Error logging DOM data for symbol {symbol_id}: {e}")

    def log_raw_payload(self, payload_type: str, payload_data: Dict, received_timestamp: float):
        """
        Log raw payload data from cTrader OpenAPI
        
        Args:
            payload_type: Type of payload (e.g., 'spot_event', 'depth_event')
            payload_data: Raw payload dictionary
            received_timestamp: When the payload was received
        """
        try:
            current_time = time.time()
            current_time_ms = int(current_time * 1000)
            received_timestamp_ms = int(received_timestamp * 1000)
            
            # Create raw payload log file if it doesn't exist
            if not hasattr(self, 'raw_payload_file'):
                current_date = datetime.now().strftime("%Y%m%d")
                raw_filename = f"raw_payloads_{current_date}.log"
                raw_filepath = os.path.join(self.data_logs_path, raw_filename)
                
                self.raw_payload_file = open(raw_filepath, 'a', encoding='utf-8')
                
                # Write header if file is new
                if os.path.getsize(raw_filepath) == 0:
                    self.raw_payload_file.write("received_timestamp_ms,log_timestamp_ms,payload_type,payload_json\n")
                    self.raw_payload_file.flush()
                
                self.logger.info(f"Raw payload logging initialized: {raw_filename}")
            
            # Convert payload to JSON string
            import json
            payload_json = json.dumps(payload_data, default=str)
            
            # Create log entry
            log_entry = f"{received_timestamp_ms},{current_time_ms},{payload_type},{payload_json}\n"
            
            # Write to file
            self.raw_payload_file.write(log_entry)
            self.raw_payload_file.flush()
            
            self.logger.debug(f"Raw payload logged: {payload_type}, received_ts={received_timestamp_ms}, log_ts={current_time_ms}")
            
        except Exception as e:
            self.logger.error(f"Error logging raw payload: {e}")
    
    def get_latency_statistics(self, symbol_id: Optional[int] = None) -> Dict:
        """Get latency statistics for one or all symbols"""
        stats = {}
        
        symbols_to_analyze = [symbol_id] if symbol_id else SYMBOLS.keys()
        
        for sid in symbols_to_analyze:
            symbol_name = SYMBOLS[sid]['name']
            
            tick_latencies = list(self.tick_latencies[sid])
            dom_latencies = list(self.dom_latencies[sid])
            
            stats[symbol_name] = {
                'tick_stats': self._calculate_latency_stats(tick_latencies),
                'dom_stats': self._calculate_latency_stats(dom_latencies),
                'tick_count': self.tick_counts[sid],
                'dom_count': self.dom_counts[sid]
            }
        
        return stats
    
    def _calculate_latency_stats(self, latencies: List[float]) -> Dict:
        """Calculate statistical measures for latencies"""
        if not latencies:
            return {
                'count': 0,
                'avg_ms': 0.0,
                'min_ms': 0.0,
                'max_ms': 0.0,
                'p50_ms': 0.0,
                'p95_ms': 0.0,
                'p99_ms': 0.0
            }
        
        sorted_latencies = sorted(latencies)
        count = len(sorted_latencies)
        
        # Calculate percentiles
        def percentile(data, p):
            index = int(p * (len(data) - 1))
            return data[index]
        
        return {
            'count': count,
            'avg_ms': sum(latencies) / count,
            'min_ms': min(latencies),
            'max_ms': max(latencies),
            'p50_ms': percentile(sorted_latencies, 0.50),
            'p95_ms': percentile(sorted_latencies, 0.95),
            'p99_ms': percentile(sorted_latencies, 0.99)
        }
    
    def log_latency_report(self):
        """Log comprehensive latency report"""
        stats = self.get_latency_statistics()
        
        self.logger.info("="*80)
        self.logger.info("DATA LATENCY ANALYSIS REPORT")
        self.logger.info("="*80)
        
        for symbol_name, symbol_stats in stats.items():
            tick_stats = symbol_stats['tick_stats']
            dom_stats = symbol_stats['dom_stats']
            
            self.logger.info(f"\n{symbol_name}:")
            self.logger.info(f"  TICK DATA ({symbol_stats['tick_count']} updates):")
            self.logger.info(f"    Average: {tick_stats['avg_ms']:.2f}ms")
            self.logger.info(f"    Min/Max: {tick_stats['min_ms']:.2f}ms / {tick_stats['max_ms']:.2f}ms")
            self.logger.info(f"    P50/P95/P99: {tick_stats['p50_ms']:.2f}ms / {tick_stats['p95_ms']:.2f}ms / {tick_stats['p99_ms']:.2f}ms")
            
            self.logger.info(f"  DOM DATA ({symbol_stats['dom_count']} updates):")
            self.logger.info(f"    Average: {dom_stats['avg_ms']:.2f}ms")
            self.logger.info(f"    Min/Max: {dom_stats['min_ms']:.2f}ms / {dom_stats['max_ms']:.2f}ms")
            self.logger.info(f"    P50/P95/P99: {dom_stats['p50_ms']:.2f}ms / {dom_stats['p95_ms']:.2f}ms / {dom_stats['p99_ms']:.2f}ms")
        
        self.logger.info("="*80)
    
    def get_data_rates(self) -> Dict:
        """Get data rates (updates per second) for each symbol"""
        current_time = time.time()
        rates = {}
        
        for symbol_id, symbol_info in SYMBOLS.items():
            symbol_name = symbol_info['name']
            
            # Calculate rates based on recent activity
            tick_count = self.tick_counts[symbol_id]
            dom_count = self.dom_counts[symbol_id]
            
            # Simple rate calculation (could be enhanced with time windows)
            session_time = current_time - getattr(self, 'start_time', current_time)
            
            if session_time > 0:
                tick_rate = tick_count / session_time
                dom_rate = dom_count / session_time
            else:
                tick_rate = dom_rate = 0
            
            rates[symbol_name] = {
                'tick_rate_per_sec': tick_rate,
                'dom_rate_per_sec': dom_rate,
                'total_tick_updates': tick_count,
                'total_dom_updates': dom_count
            }
        
        return rates
    
    def start_session(self):
        """Mark the start of a data logging session"""
        self.start_time = time.time()
        self.logger.info("Data logging session started")
    
    def close_log_files(self):
        """Close all log files"""
        try:
            for file_handle in self.tick_log_files.values():
                file_handle.close()
            
            for file_handle in self.dom_log_files.values():
                file_handle.close()
            
            # Close raw payload file if it exists
            if hasattr(self, 'raw_payload_file'):
                self.raw_payload_file.close()
            
            self.logger.info("All data log files closed")
            
        except Exception as e:
            self.logger.error(f"Error closing log files: {e}")
    
    def __del__(self):
        """Cleanup when object is destroyed"""
        self.close_log_files()


# Create global data logger instance
data_logger = DataLogger() 