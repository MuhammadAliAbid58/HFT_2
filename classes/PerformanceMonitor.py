import time
import logging
import threading
import statistics
from collections import deque
from typing import Dict, List
from .globals import timing_decorator

class PerformanceMonitor:
    """
    Simplified performance monitor for tracking latencies and basic metrics.
    Focuses on essential functionality without GPU/complex analysis.
    """
    
    def __init__(self):
        self.logger = logging.getLogger('PerformanceMonitor')
        
        # Thread-safe storage for different types of latencies
        self.lock = threading.Lock()
        
        # Basic latency tracking
        self.fix_latencies = deque(maxlen=1000)  # FIX order latencies
        self.tick_processing_times = deque(maxlen=1000)  # Tick processing latencies
        self.depth_processing_times = deque(maxlen=1000)  # DOM processing latencies
        self.order_to_fill_times = deque(maxlen=100)  # Order execution times
        
        # Function execution times
        self.function_times = {}
        
        # System metrics
        self.session_start_time = time.time()
        self.total_ticks_processed = 0
        self.total_orders_sent = 0
        self.total_orders_filled = 0
        
        self.logger.info("PerformanceMonitor initialized for basic latency tracking")
    
    @timing_decorator
    def record_fix_latency(self, latency_seconds: float):
        """Record FIX order round-trip latency"""
        with self.lock:
            latency_ms = latency_seconds * 1000
            self.fix_latencies.append(latency_ms)
            
            # Log high latency warnings
            if latency_ms > 100:  # > 100ms
                self.logger.warning(f"High FIX latency detected: {latency_ms:.2f}ms")
            
            self.logger.debug(f"FIX latency recorded: {latency_ms:.2f}ms")
    
    @timing_decorator  
    def record_tick_processing_time(self, processing_time_seconds: float):
        """Record tick data processing time"""
        with self.lock:
            processing_time_ms = processing_time_seconds * 1000
            self.tick_processing_times.append(processing_time_ms)
            self.total_ticks_processed += 1
            
            # Log slow processing
            if processing_time_ms > 50:  # > 50ms
                self.logger.warning(f"Slow tick processing: {processing_time_ms:.2f}ms")
    
    @timing_decorator
    def record_depth_processing_time(self, processing_time_seconds: float):
        """Record DOM/depth data processing time"""
        with self.lock:
            processing_time_ms = processing_time_seconds * 1000
            self.depth_processing_times.append(processing_time_ms)
            
            # Log slow processing
            if processing_time_ms > 50:  # > 50ms
                self.logger.warning(f"Slow depth processing: {processing_time_ms:.2f}ms")
    
    @timing_decorator
    def record_order_execution_time(self, execution_time_seconds: float):
        """Record order-to-fill execution time"""
        with self.lock:
            execution_time_ms = execution_time_seconds * 1000
            self.order_to_fill_times.append(execution_time_ms)
            self.total_orders_filled += 1
            
            # Log slow executions
            if execution_time_ms > 200:  # > 200ms
                self.logger.warning(f"Slow order execution: {execution_time_ms:.2f}ms")
    
    @timing_decorator
    def record_function_time(self, function_name: str, execution_time_seconds: float):
        """Record function execution time"""
        with self.lock:
            execution_time_ms = execution_time_seconds * 1000
            
            if function_name not in self.function_times:
                self.function_times[function_name] = deque(maxlen=100)
            
            self.function_times[function_name].append(execution_time_ms)
            
            # Log very slow functions
            if execution_time_ms > 100:  # > 100ms
                self.logger.warning(f"Slow function {function_name}: {execution_time_ms:.2f}ms")
    
    def get_latency_stats(self) -> Dict:
        """Get current latency statistics"""
        with self.lock:
            stats = {
                'session_duration_minutes': (time.time() - self.session_start_time) / 60,
                'total_ticks_processed': self.total_ticks_processed,
                'total_orders_sent': self.total_orders_sent,
                'total_orders_filled': self.total_orders_filled,
                'latencies': {}
            }
            
            # FIX latencies
            if self.fix_latencies:
                stats['latencies']['fix'] = {
                    'avg_ms': statistics.mean(self.fix_latencies),
                    'min_ms': min(self.fix_latencies),
                    'max_ms': max(self.fix_latencies),
                    'p95_ms': statistics.quantiles(self.fix_latencies, n=20)[18] if len(self.fix_latencies) >= 20 else statistics.mean(self.fix_latencies),
                    'count': len(self.fix_latencies)
                }
            
            # Tick processing times
            if self.tick_processing_times:
                stats['latencies']['tick_processing'] = {
                    'avg_ms': statistics.mean(self.tick_processing_times),
                    'min_ms': min(self.tick_processing_times),
                    'max_ms': max(self.tick_processing_times),
                    'p95_ms': statistics.quantiles(self.tick_processing_times, n=20)[18] if len(self.tick_processing_times) >= 20 else statistics.mean(self.tick_processing_times),
                    'count': len(self.tick_processing_times)
                }
            
            # Depth processing times
            if self.depth_processing_times:
                stats['latencies']['depth_processing'] = {
                    'avg_ms': statistics.mean(self.depth_processing_times),
                    'min_ms': min(self.depth_processing_times),
                    'max_ms': max(self.depth_processing_times),
                    'p95_ms': statistics.quantiles(self.depth_processing_times, n=20)[18] if len(self.depth_processing_times) >= 20 else statistics.mean(self.depth_processing_times),
                    'count': len(self.depth_processing_times)
                }
            
            # Order execution times
            if self.order_to_fill_times:
                stats['latencies']['order_execution'] = {
                    'avg_ms': statistics.mean(self.order_to_fill_times),
                    'min_ms': min(self.order_to_fill_times),
                    'max_ms': max(self.order_to_fill_times),
                    'p95_ms': statistics.quantiles(self.order_to_fill_times, n=20)[18] if len(self.order_to_fill_times) >= 20 else statistics.mean(self.order_to_fill_times),
                    'count': len(self.order_to_fill_times)
                }
            
            return stats
    
    def get_performance_summary(self) -> str:
        """Generate a human-readable performance summary"""
        stats = self.get_latency_stats()
        
        summary = [
            "="*60,
            "PERFORMANCE SUMMARY",
            "="*60,
            f"Session Duration: {stats['session_duration_minutes']:.1f} minutes",
            f"Ticks Processed: {stats['total_ticks_processed']:,}",
            f"Orders Sent: {stats['total_orders_sent']:,}",
            f"Orders Filled: {stats['total_orders_filled']:,}",
            ""
        ]
        
        # Add latency details
        for latency_type, data in stats['latencies'].items():
            summary.extend([
                f"{latency_type.replace('_', ' ').title()} Latencies:",
                f"  Average: {data['avg_ms']:.2f}ms",
                f"  95th Percentile: {data['p95_ms']:.2f}ms",
                f"  Min/Max: {data['min_ms']:.2f}ms / {data['max_ms']:.2f}ms",
                f"  Sample Count: {data['count']:,}",
                ""
            ])
        
        summary.append("="*60)
        
        return "\n".join(summary)
    
    def log_performance_report(self):
        """Log the current performance report"""
        self.logger.info("\n" + self.get_performance_summary())
    
    def get_average_latency(self, latency_type: str) -> float:
        """Get average latency for a specific type"""
        with self.lock:
            if latency_type == 'fix' and self.fix_latencies:
                return statistics.mean(self.fix_latencies)
            elif latency_type == 'tick' and self.tick_processing_times:
                return statistics.mean(self.tick_processing_times)
            elif latency_type == 'depth' and self.depth_processing_times:
                return statistics.mean(self.depth_processing_times)
            elif latency_type == 'execution' and self.order_to_fill_times:
                return statistics.mean(self.order_to_fill_times)
            else:
                return 0.0
    
    def increment_orders_sent(self):
        """Increment the orders sent counter"""
        with self.lock:
            self.total_orders_sent += 1


# Create global performance monitor instance
performance_monitor = PerformanceMonitor() 