#!/usr/bin/env python3
"""
HFT Base - Simplified High-Frequency Trading System

A streamlined version focused on:
- Core trading functions (open/close)
- Basic tick and DOM data processing 
- Simple latency checking
- Multi-symbol threading
- Fixed stop loss/take profit
- Performance metrics and logging

Usage: python main.py
"""

import logging
import signal
import sys
import time
from classes.globals import SYMBOLS, SESSION_DURATION
from classes.PerformanceMonitor import performance_monitor
from classes.DataCollector import DataCollector

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger = logging.getLogger('main')
    logger.info("Shutdown signal received. Stopping system...")
    
    if 'collector' in globals():
        collector.stop()
    
    sys.exit(0)

def main():
    """Main entry point for HFT Base Lite"""
    
    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Setup logging
    logger = logging.getLogger('main')
    logger.info("="*60)
    logger.info("HFT BASE LITE - STARTING")
    logger.info("="*60)
    logger.info(f"Active Symbols: {len(SYMBOLS)}")
    for symbol_id, symbol_info in SYMBOLS.items():
        logger.info(f"  {symbol_id}: {symbol_info['name']}")
    logger.info(f"Session Duration: {SESSION_DURATION} minutes")
    logger.info("="*60)
    
    try:
        # Initialize data collector
        logger.info("Initializing DataCollector...")
        global collector
        collector = DataCollector()
        
        # Start performance reporting
        collector.start_performance_reporting()
        
        # Log initial system status
        collector.log_system_status()
        
        # Start the system
        logger.info("Starting trading system...")
        logger.info("Press Ctrl+C to stop")
        
        # Start the main loop
        collector.start()
        
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        logger.error("", exc_info=True)
    finally:
        # Cleanup and final report
        try:
            logger.info("Generating final performance report...")
            
            # Get final statistics
            if 'collector' in globals():
                final_status = collector.get_system_status()
                logger.info("FINAL SYSTEM STATUS:")
                logger.info(f"  Performance: {final_status['performance']}")
                
                # Performance summary
                performance_monitor.log_performance_report()
                
                # Trading client summary
                if collector.trading_client:
                    collector.trading_client.log_status()
                
                # Stop the collector
                collector.stop()
            
            logger.info("="*60)
            logger.info("HFT BASE LITE - SHUTDOWN COMPLETE")
            logger.info("="*60)
            
        except Exception as e:
            logger.error(f"Error during shutdown: {str(e)}")

if __name__ == "__main__":
    main() 