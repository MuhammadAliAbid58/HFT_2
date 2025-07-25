import time
import logging
import threading
from collections import deque
from typing import List, Dict, Optional, Tuple
from .globals import timing_decorator, SYMBOLS
from .PerformanceMonitor import performance_monitor
from .DataLogger import data_logger

class DepthClient:
    """
    Simplified depth client for basic DOM (Depth of Market) data collection and processing.
    Focuses on essential functionality without complex analysis.
    """
    
    def __init__(self, symbol_id: int):
        self.symbol_id = symbol_id
        self.symbol_name = SYMBOLS[symbol_id]['name']
        self.logger = logging.getLogger(f'DepthClient_{self.symbol_name}')
        
        # Thread-safe depth storage
        self.lock = threading.Lock()
        self.quotes = []  # Current depth quotes
        self.depth_history = deque(maxlen=100)  # Store recent depth snapshots
        
        # Basic DOM analysis
        self.last_update_time = 0
        self.total_updates = 0
        
        # Simple imbalance tracking
        self.bid_ask_imbalances = deque(maxlen=50)
        
        self.logger.info(f"DepthClient initialized for {self.symbol_name}")
    
    @timing_decorator
    def update_quotes(self, quotes: List[Dict]):
        """Update depth quotes with new DOM data"""
        start_time = time.perf_counter()
        received_timestamp = time.time()  # Capture exact receive time for latency analysis
        
        try:
            with self.lock:
                self.quotes = quotes.copy()
                self.last_update_time = time.time()
                self.total_updates += 1
                
                # Create depth snapshot for history
                snapshot = self._create_depth_snapshot()
                if snapshot:
                    self.depth_history.append(snapshot)
                
                # Calculate basic imbalance
                imbalance = self._calculate_bid_ask_imbalance()
                if imbalance is not None:
                    self.bid_ask_imbalances.append(imbalance)
                
                self.logger.debug(f"Updated DOM with {len(quotes)} quotes")
                
                # Log DOM data to data logger (all timestamps in milliseconds)
                data_logger.log_dom_data(self.symbol_id, quotes, received_timestamp)
                
        except Exception as e:
            self.logger.error(f"Error updating quotes: {str(e)}")
            raise
        finally:
            # Record processing time
            processing_time = time.perf_counter() - start_time
            performance_monitor.record_depth_processing_time(processing_time)
    
    @timing_decorator
    def get_current_quotes(self) -> List[Dict]:
        """Get current depth quotes"""
        with self.lock:
            return self.quotes.copy()
    
    @timing_decorator
    def get_best_bid_ask(self) -> Optional[Tuple[float, float]]:
        """Get best bid and ask prices"""
        with self.lock:
            if not self.quotes:
                return None
            
            best_bid = None
            best_ask = None
            
            for quote in self.quotes:
                price = quote.get('price', 0)
                side = quote.get('side', '')
                
                if side == 'bid':
                    if best_bid is None or price > best_bid:
                        best_bid = price
                elif side == 'ask':
                    if best_ask is None or price < best_ask:
                        best_ask = price
            
            if best_bid is not None and best_ask is not None:
                return (best_bid, best_ask)
            
            return None
    
    @timing_decorator
    def get_current_spread(self) -> Optional[float]:
        """Get current bid-ask spread"""
        bid_ask = self.get_best_bid_ask()
        if bid_ask:
            bid, ask = bid_ask
            return ask - bid
        return None
    
    @timing_decorator
    def _create_depth_snapshot(self) -> Optional[Dict]:
        """Create a snapshot of current depth data"""
        if not self.quotes:
            return None
        
        bids = []
        asks = []
        
        for quote in self.quotes:
            price = quote.get('price', 0)
            volume = quote.get('volume', 0)
            side = quote.get('side', '')
            
            if side == 'bid':
                bids.append((price, volume))
            elif side == 'ask':
                asks.append((price, volume))
        
        # Sort bids (highest first) and asks (lowest first)
        bids.sort(key=lambda x: x[0], reverse=True)
        asks.sort(key=lambda x: x[0])
        
        return {
            'timestamp': time.time(),
            'bids': bids[:10],  # Top 10 levels
            'asks': asks[:10],  # Top 10 levels
            'total_bid_volume': sum(vol for _, vol in bids),
            'total_ask_volume': sum(vol for _, vol in asks)
        }
    
    @timing_decorator
    def _calculate_bid_ask_imbalance(self) -> Optional[float]:
        """
        Calculate simple bid-ask volume imbalance
        Returns positive value for bid dominance, negative for ask dominance
        """
        snapshot = self._create_depth_snapshot()
        if not snapshot or not snapshot['bids'] or not snapshot['asks']:
            return None
        
        total_bid_volume = snapshot['total_bid_volume']
        total_ask_volume = snapshot['total_ask_volume']
        
        if total_bid_volume + total_ask_volume == 0:
            return 0.0
        
        # Calculate imbalance ratio
        imbalance = (total_bid_volume - total_ask_volume) / (total_bid_volume + total_ask_volume)
        
        return imbalance
    
    @timing_decorator
    def get_depth_levels(self, levels: int = 5) -> Dict:
        """Get specified number of depth levels for both sides"""
        with self.lock:
            if not self.quotes:
                return {'bids': [], 'asks': []}
            
            bids = []
            asks = []
            
            for quote in self.quotes:
                price = quote.get('price', 0)
                volume = quote.get('volume', 0)
                side = quote.get('side', '')
                
                if side == 'bid':
                    bids.append((price, volume))
                elif side == 'ask':
                    asks.append((price, volume))
            
            # Sort and limit to requested levels
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])
            
            return {
                'bids': bids[:levels],
                'asks': asks[:levels]
            }
    
    @timing_decorator
    def get_volume_at_price(self, target_price: float, tolerance: float = 0.00001) -> float:
        """Get volume available at or near a specific price level"""
        with self.lock:
            total_volume = 0.0
            
            for quote in self.quotes:
                price = quote.get('price', 0)
                volume = quote.get('volume', 0)
                
                if abs(price - target_price) <= tolerance:
                    total_volume += volume
            
            return total_volume
    
    @timing_decorator
    def get_imbalance_stats(self) -> Dict:
        """Get basic imbalance statistics"""
        with self.lock:
            if not self.bid_ask_imbalances:
                return {
                    'current_imbalance': 0.0,
                    'average_imbalance': 0.0,
                    'imbalance_trend': 'neutral'
                }
            
            current_imbalance = self.bid_ask_imbalances[-1]
            average_imbalance = sum(self.bid_ask_imbalances) / len(self.bid_ask_imbalances)
            
            # Determine trend
            if len(self.bid_ask_imbalances) >= 3:
                recent_avg = sum(list(self.bid_ask_imbalances)[-3:]) / 3
                if recent_avg > 0.1:
                    trend = 'bid_dominant'
                elif recent_avg < -0.1:
                    trend = 'ask_dominant'
                else:
                    trend = 'balanced'
            else:
                trend = 'insufficient_data'
            
            return {
                'current_imbalance': current_imbalance,
                'average_imbalance': average_imbalance,
                'imbalance_trend': trend,
                'data_points': len(self.bid_ask_imbalances)
            }
    
    @timing_decorator
    def has_sufficient_data(self, min_updates: int = 5) -> bool:
        """Check if we have sufficient DOM data for analysis"""
        with self.lock:
            return self.total_updates >= min_updates and len(self.quotes) > 0
    
    @timing_decorator
    def get_depth_summary(self) -> Dict:
        """Get a comprehensive summary of depth data"""
        with self.lock:
            best_bid_ask = self.get_best_bid_ask()
            spread = self.get_current_spread()
            imbalance_stats = self.get_imbalance_stats()
            depth_levels = self.get_depth_levels(5)
            
            return {
                'symbol': self.symbol_name,
                'best_bid': best_bid_ask[0] if best_bid_ask else None,
                'best_ask': best_bid_ask[1] if best_bid_ask else None,
                'spread': spread,
                'total_quotes': len(self.quotes),
                'total_updates': self.total_updates,
                'bid_levels': len(depth_levels['bids']),
                'ask_levels': len(depth_levels['asks']),
                'imbalance_stats': imbalance_stats,
                'last_update': self.last_update_time,
                'has_sufficient_data': self.has_sufficient_data()
            }
    
    def log_status(self):
        """Log current status of the depth client"""
        summary = self.get_depth_summary()
        self.logger.info(f"DepthClient Status for {self.symbol_name}:")
        if summary['best_bid'] and summary['best_ask']:
            self.logger.info(f"  Best Bid/Ask: {summary['best_bid']:.5f} / {summary['best_ask']:.5f}")
            self.logger.info(f"  Spread: {summary['spread']:.5f}")
        self.logger.info(f"  Total Quotes: {summary['total_quotes']}")
        self.logger.info(f"  Total Updates: {summary['total_updates']:,}")
        self.logger.info(f"  Imbalance: {summary['imbalance_stats']['current_imbalance']:.3f}")
    
    def reset_statistics(self):
        """Reset all statistics"""
        with self.lock:
            self.total_updates = 0
            self.depth_history.clear()
            self.bid_ask_imbalances.clear()
            
            self.logger.info(f"Statistics reset for {self.symbol_name}") 