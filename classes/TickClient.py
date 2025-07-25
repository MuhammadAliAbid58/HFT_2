import time
import logging
import threading
from collections import deque
from typing import List, Dict, Optional
from .globals import timing_decorator, SYMBOLS
from .PerformanceMonitor import performance_monitor
from .DataLogger import data_logger

class TickClient:
    """
    Simplified tick client for basic tick data collection and processing.
    Focuses on essential functionality without complex analysis.
    """
    
    def __init__(self, symbol_id: int):
        self.symbol_id = symbol_id
        self.symbol_name = SYMBOLS[symbol_id]['name']
        self.logger = logging.getLogger(f'TickClient_{self.symbol_name}')
        
        # Thread-safe tick storage
        self.lock = threading.Lock()
        self.spots = deque(maxlen=1000)  # Store last 1000 ticks
        
        # Basic tick analysis
        self.last_price = None
        self.price_changes = deque(maxlen=100)  # Track price movements
        self.tick_directions = deque(maxlen=50)  # Track tick directions (1, 0, -1)
        
        # Simple statistics
        self.total_ticks = 0
        self.up_ticks = 0
        self.down_ticks = 0
        self.neutral_ticks = 0
        
        # Performance tracking
        self.last_update_time = time.time()
        
        self.logger.info(f"TickClient initialized for {self.symbol_name}")
    
    @timing_decorator
    def update_spot(self, spot_data: Dict):
        """Update tick data with new spot price"""
        start_time = time.perf_counter()
        received_timestamp = time.time()  # Capture exact receive time for latency analysis
        
        try:
            with self.lock:
                current_price = spot_data.get('price')
                if current_price is None:
                    self.logger.warning("Received spot data without price")
                    return
                
                # Ensure timestamp is in milliseconds
                if 'timestamp' not in spot_data:
                    spot_data['timestamp'] = int(time.time() * 1000)  # Convert to milliseconds
                elif spot_data['timestamp'] < 1e12:  # If timestamp is in seconds
                    spot_data['timestamp'] = int(spot_data['timestamp'] * 1000)  # Convert to milliseconds
                
                # Calculate tick direction
                direction = 0  # neutral
                if self.last_price is not None:
                    if current_price > self.last_price:
                        direction = 1  # up tick
                        self.up_ticks += 1
                    elif current_price < self.last_price:
                        direction = -1  # down tick
                        self.down_ticks += 1
                    else:
                        self.neutral_ticks += 1
                
                # Add direction to spot data
                spot_data['direction'] = direction
                spot_data['previous_price'] = self.last_price
                
                # Store the tick
                self.spots.append(spot_data.copy())
                
                # Update tracking data
                if self.last_price is not None:
                    price_change = current_price - self.last_price
                    self.price_changes.append(price_change)
                
                self.tick_directions.append(direction)
                self.last_price = current_price
                self.total_ticks += 1
                self.last_update_time = time.time()
                
                # Log basic tick info
                self.logger.debug(f"Tick: {current_price:.5f} (dir: {direction}, spread: {spot_data.get('spread', 'N/A')})")
                
                # Log tick data to data logger (all timestamps in milliseconds)
                data_logger.log_tick_data(self.symbol_id, spot_data, received_timestamp)
                
        except Exception as e:
            self.logger.error(f"Error updating spot data: {str(e)}")
            raise
        finally:
            # Record processing time
            processing_time = time.perf_counter() - start_time
            performance_monitor.record_tick_processing_time(processing_time)
    
    @timing_decorator
    def get_latest_spots(self, count: int = 100) -> List[Dict]:
        """Get the latest tick data"""
        with self.lock:
            if count <= 0:
                return list(self.spots)
            else:
                return list(self.spots)[-count:] if len(self.spots) >= count else list(self.spots)
    
    @timing_decorator
    def get_latest_price(self) -> Optional[float]:
        """Get the most recent price"""
        with self.lock:
            if self.spots:
                return self.spots[-1].get('price')
            return None
    
    @timing_decorator
    def get_price_movement_stats(self) -> Dict:
        """Get basic price movement statistics"""
        with self.lock:
            if not self.spots or len(self.spots) < 2:
                return {
                    'total_ticks': 0,
                    'up_ticks': 0,
                    'down_ticks': 0,
                    'neutral_ticks': 0,
                    'up_percentage': 0.0,
                    'down_percentage': 0.0,
                    'latest_direction': 0
                }
            
            total = self.up_ticks + self.down_ticks + self.neutral_ticks
            
            return {
                'total_ticks': total,
                'up_ticks': self.up_ticks,
                'down_ticks': self.down_ticks,
                'neutral_ticks': self.neutral_ticks,
                'up_percentage': (self.up_ticks / total * 100) if total > 0 else 0.0,
                'down_percentage': (self.down_ticks / total * 100) if total > 0 else 0.0,
                'latest_direction': self.tick_directions[-1] if self.tick_directions else 0
            }
    
    @timing_decorator
    def get_recent_direction_bias(self, lookback: int = 10) -> float:
        """
        Get recent directional bias (positive = upward, negative = downward)
        Returns value between -1.0 and 1.0
        """
        with self.lock:
            if not self.tick_directions or len(self.tick_directions) < lookback:
                return 0.0
            
            recent_directions = list(self.tick_directions)[-lookback:]
            bias = sum(recent_directions) / len(recent_directions)
            
            return bias
    
    @timing_decorator
    def get_current_spread(self) -> Optional[float]:
        """Get the current spread from the latest tick"""
        with self.lock:
            if self.spots:
                return self.spots[-1].get('spread')
            return None
    
    @timing_decorator
    def calculate_volatility(self, period: int = 20) -> float:
        """Calculate simple price volatility over specified period"""
        with self.lock:
            if len(self.price_changes) < period:
                return 0.0
            
            recent_changes = list(self.price_changes)[-period:]
            
            # Calculate standard deviation of price changes
            if len(recent_changes) <= 1:
                return 0.0
            
            mean_change = sum(recent_changes) / len(recent_changes)
            variance = sum((change - mean_change) ** 2 for change in recent_changes) / len(recent_changes)
            volatility = variance ** 0.5
            
            return volatility
    
    @timing_decorator
    def has_sufficient_data(self, min_ticks: int = 10) -> bool:
        """Check if we have sufficient tick data for analysis"""
        with self.lock:
            return len(self.spots) >= min_ticks
    
    @timing_decorator
    def get_tick_summary(self) -> Dict:
        """Get a comprehensive summary of tick data"""
        with self.lock:
            latest_price = self.get_latest_price()
            spread = self.get_current_spread()
            movement_stats = self.get_price_movement_stats()
            volatility = self.calculate_volatility()
            direction_bias = self.get_recent_direction_bias()
            
            return {
                'symbol': self.symbol_name,
                'latest_price': latest_price,
                'spread': spread,
                'total_ticks': self.total_ticks,
                'data_points': len(self.spots),
                'movement_stats': movement_stats,
                'volatility': volatility,
                'direction_bias': direction_bias,
                'last_update': self.last_update_time,
                'has_sufficient_data': self.has_sufficient_data()
            }
    
    def log_status(self):
        """Log current status of the tick client"""
        summary = self.get_tick_summary()
        self.logger.info(f"TickClient Status for {self.symbol_name}:")
        self.logger.info(f"  Latest Price: {summary['latest_price']:.5f}")
        self.logger.info(f"  Spread: {summary['spread']:.5f}")
        self.logger.info(f"  Total Ticks: {summary['total_ticks']:,}")
        self.logger.info(f"  Direction Bias: {summary['direction_bias']:.3f}")
        self.logger.info(f"  Volatility: {summary['volatility']:.5f}")
    
    def reset_statistics(self):
        """Reset all statistics (useful for new trading sessions)"""
        with self.lock:
            self.total_ticks = 0
            self.up_ticks = 0
            self.down_ticks = 0
            self.neutral_ticks = 0
            self.price_changes.clear()
            self.tick_directions.clear()
            
            self.logger.info(f"Statistics reset for {self.symbol_name}") 