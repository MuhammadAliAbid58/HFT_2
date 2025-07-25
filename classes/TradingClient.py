import time
import uuid
import logging
import threading
from typing import Dict, Optional
from collections import deque
from ctrader_fix import Client as FixClient, NewOrderSingle, LogonRequest, Heartbeat

from .globals import (
    timing_decorator, SYMBOLS, GLOBAL_TRADE_OPEN, GLOBAL_TRADE_LOCK,
    STOP_LOSS_PIPS, TAKE_PROFIT_PIPS, INITIAL_LOT_SIZE, 
    MIN_SPREAD_THRESHOLD, MAX_LATENCY_THRESHOLD
)
from .PerformanceMonitor import performance_monitor

class TradingClient:
    """
    Simplified trading client for core opening and closing functions.
    Based on the working FixTradingClient but without complex features.
    """
    
    def __init__(self, fix_client, config, tick_clients=None, depth_clients=None):
        self.fix_client = fix_client
        self.config = config
        self.tick_clients = tick_clients or {}
        self.depth_clients = depth_clients or {}
        
        self.logger = logging.getLogger('TradingClient')
        
        # Thread safety
        self.lock = threading.Lock()
        self._position_lock = threading.Lock()
        
        # Position tracking - one position per symbol
        self.active_positions = {symbol_id: None for symbol_id in SYMBOLS.keys()}
        
        # Trade statistics
        self.trade_count = 0
        self.wins = 0
        self.losses = 0
        self.total_pips_profit = 0.0
        self.order_history = []
        
        # Session management
        self.session_start_time = time.time()
        self.session_ready = threading.Event()
        self.session_ending = False
        
        # Performance tracking
        self.order_latencies = deque(maxlen=100)
        
        self.logger.info("TradingClient initialized")
    
    @timing_decorator
    def place_order(self, direction: str, symbol_id: int, 
                   take_profit: Optional[float] = None, stop_loss: Optional[float] = None) -> bool:
        """Place a market order for a specific symbol"""
        symbol_name = SYMBOLS[symbol_id]['name']
        symbol_logger = logging.getLogger(f'Symbol_{symbol_name}')
        
        # Check for existing position
        with self._position_lock:
            existing_position = self.active_positions.get(symbol_id)
            if existing_position:
                symbol_logger.warning(f"Order blocked: Position already exists for {symbol_name}")
                return False
        
        # Global trade lock (one trade at a time)
        global GLOBAL_TRADE_OPEN
        if GLOBAL_TRADE_OPEN:
            symbol_logger.warning(f"Order blocked: Global trade is already open")
            return False
        
        # Check session readiness
        if not self.session_ready.is_set():
            symbol_logger.warning(f"Order blocked: FIX session not ready")
            return False
        
        # Basic checks
        if direction.lower() not in ['buy', 'sell']:
            symbol_logger.error(f"Invalid direction: {direction}")
            return False
        
        # Check spread
        current_spread = self._get_current_spread(symbol_id)
        if current_spread and current_spread > MIN_SPREAD_THRESHOLD:
            symbol_logger.warning(f"Order blocked: Spread too wide ({current_spread:.5f})")
            return False
        
        try:
            # Create FIX order
            order_start_time = time.time()
            
            req = NewOrderSingle(self.config)
            req.ClOrdID = str(uuid.uuid4())
            req.Symbol = symbol_id
            req.Side = 1 if direction.lower() == "buy" else 2
            req.OrderQty = INITIAL_LOT_SIZE * 1000  # Convert to units
            req.OrdType = 1  # Market order
            req.PositionEffect = "O"  # Opening order
            
            # Create position record
            position = {
                "symbol_id": symbol_id,
                "side": direction.lower(),
                "qty": INITIAL_LOT_SIZE / 100.0,  # Convert to lots
                "status": "PENDING",
                "order_time": order_start_time,
                "client_order_id": req.ClOrdID,
                "stop_loss": stop_loss or STOP_LOSS_PIPS,
                "take_profit": take_profit or TAKE_PROFIT_PIPS
            }
            
            # Store position
            with self._position_lock:
                self.active_positions[symbol_id] = position
            
            # Send order
            self.fix_client.send(req)
            performance_monitor.increment_orders_sent()
            
            symbol_logger.info(f"Order sent: {direction.upper()} {symbol_name} @ market")
            symbol_logger.info(f"SL: {position['stop_loss']:.1f} pips, TP: {position['take_profit']:.1f} pips")
            
            return True
            
        except Exception as e:
            # Clean up on error
            with self._position_lock:
                if symbol_id in self.active_positions:
                    self.active_positions[symbol_id] = None
            
            symbol_logger.error(f"Error placing order: {str(e)}")
            return False
    
    @timing_decorator
    def close_position(self, symbol_id: int, reason: str = "Manual") -> bool:
        """Close an existing position"""
        symbol_name = SYMBOLS[symbol_id]['name']
        symbol_logger = logging.getLogger(f'Symbol_{symbol_name}')
        
        # Get position
        with self._position_lock:
            position = self.active_positions.get(symbol_id)
            if not position or position.get('status') != 'OPEN':
                symbol_logger.warning(f"No open position to close for {symbol_name}")
                return False
            
            # Mark as closing
            position['status'] = 'CLOSING'
            position['close_reason'] = reason
            position['close_time'] = time.time()
        
        try:
            # Create close order
            close_side = "sell" if position['side'] == "buy" else "buy"
            
            req = NewOrderSingle(self.config)
            req.ClOrdID = str(uuid.uuid4())
            req.Symbol = symbol_id
            req.Side = 1 if close_side == "buy" else 2
            req.OrderQty = position.get('qty', INITIAL_LOT_SIZE / 100.0) * 1000
            req.OrdType = 1  # Market order
            req.PosMaintRptID = position.get('pos_maint_rpt_id')
            req.PositionEffect = "C"  # Closing order
            
            # Send close order
            self.fix_client.send(req)
            
            symbol_logger.info(f"Close order sent for {symbol_name}: {reason}")
            
            return True
            
        except Exception as e:
            # Reset status on error
            with self._position_lock:
                if position:
                    position['status'] = 'OPEN'
            
            symbol_logger.error(f"Error closing position: {str(e)}")
            return False
    
    @timing_decorator
    def check_sl_tp(self, spot_data: Dict, symbol_id: int):
        """Check if current price has hit stop loss or take profit"""
        symbol_name = SYMBOLS[symbol_id]['name']
        
        # Get position
        with self._position_lock:
            position = self.active_positions.get(symbol_id)
            if not position or position.get('status') != 'OPEN':
                return
        
        current_price = spot_data.get('price')
        if not current_price:
            return
        
        entry_price = position.get('entry_price')
        if not entry_price:
            return
        
        position_side = position.get('side')
        symbol_info = SYMBOLS[symbol_id]
        pip_value = symbol_info['pip_value']
        
        # Calculate P&L in pips
        if position_side == "buy":
            pnl_pips = (current_price - entry_price) / pip_value
        else:
            pnl_pips = (entry_price - current_price) / pip_value
        
        # Get SL/TP levels
        stop_loss_pips = position.get('stop_loss', STOP_LOSS_PIPS)
        take_profit_pips = position.get('take_profit', TAKE_PROFIT_PIPS)
        
        # Check for SL/TP hit
        close_reason = None
        
        if pnl_pips <= -stop_loss_pips:
            close_reason = "Stop Loss"
        elif pnl_pips >= take_profit_pips:
            close_reason = "Take Profit"
        
        if close_reason:
            self.logger.info(f"{close_reason} hit for {symbol_name}: {pnl_pips:+.1f} pips")
            self.close_position(symbol_id, close_reason)
    
    def get_active_position(self, symbol_id: int) -> Optional[Dict]:
        """Get active position for a symbol"""
        with self._position_lock:
            return self.active_positions.get(symbol_id)
    
    def _get_current_spread(self, symbol_id: int) -> Optional[float]:
        """Get current spread for a symbol"""
        if symbol_id in self.tick_clients:
            return self.tick_clients[symbol_id].get_current_spread()
        return None
    
    def _get_current_price(self, symbol_id: int) -> Optional[float]:
        """Get current price for a symbol"""
        if symbol_id in self.tick_clients:
            return self.tick_clients[symbol_id].get_latest_price()
        return None
    
    @timing_decorator
    def process_execution_report(self, message):
        """Process execution reports from FIX"""
        try:
            # Extract order information
            client_order_id = getattr(message, 'ClOrdID', None)
            symbol_id = getattr(message, 'Symbol', None)
            exec_type = getattr(message, 'ExecType', None)
            order_status = getattr(message, 'OrdStatus', None)
            
            if not all([client_order_id, symbol_id]):
                return
            
            symbol_name = SYMBOLS.get(symbol_id, {}).get('name', f'ID_{symbol_id}')
            
            # Find matching position
            with self._position_lock:
                position = None
                for pos in self.active_positions.values():
                    if pos and pos.get('client_order_id') == client_order_id:
                        position = pos
                        break
                
                if not position:
                    self.logger.warning(f"No matching position found for order {client_order_id}")
                    return
                
                # Handle fill
                if exec_type == 'F':  # Fill
                    fill_price = getattr(message, 'AvgPx', getattr(message, 'LastPx', 0))
                    pos_maint_rpt_id = getattr(message, 'PosMaintRptID', None)
                    
                    if position['status'] == 'PENDING':
                        # Opening fill
                        position.update({
                            'status': 'OPEN',
                            'entry_price': fill_price,
                            'entry_time': time.time(),
                            'pos_maint_rpt_id': pos_maint_rpt_id
                        })
                        
                        # Set global trade flag
                        global GLOBAL_TRADE_OPEN
                        GLOBAL_TRADE_OPEN = True
                        
                        self.logger.info(f"Position opened: {symbol_name} {position['side'].upper()} @ {fill_price:.5f}")
                        
                    elif position['status'] == 'CLOSING':
                        # Closing fill
                        exit_price = fill_price
                        entry_price = position.get('entry_price', 0)
                        
                        # Calculate P&L
                        pip_value = SYMBOLS[symbol_id]['pip_value']
                        if position['side'] == 'buy':
                            pnl_pips = (exit_price - entry_price) / pip_value
                        else:
                            pnl_pips = (entry_price - exit_price) / pip_value
                        
                        # Update statistics
                        self.trade_count += 1
                        self.total_pips_profit += pnl_pips
                        
                        if pnl_pips > 0:
                            self.wins += 1
                        else:
                            self.losses += 1
                        
                        # Store completed trade
                        completed_trade = position.copy()
                        completed_trade.update({
                            'status': 'CLOSED',
                            'exit_price': exit_price,
                            'exit_time': time.time(),
                            'pnl_pips': pnl_pips
                        })
                        self.order_history.append(completed_trade)
                        
                        # Clear position
                        self.active_positions[symbol_id] = None
                        
                        # Reset global trade flag
                        GLOBAL_TRADE_OPEN = False
                        
                        self.logger.info(f"Position closed: {symbol_name} @ {exit_price:.5f}, P&L: {pnl_pips:+.1f} pips")
                
        except Exception as e:
            self.logger.error(f"Error processing execution report: {str(e)}")
    
    def get_performance_summary(self) -> str:
        """Get trading performance summary"""
        if self.trade_count == 0:
            return "No trades completed yet"
        
        win_rate = (self.wins / self.trade_count) * 100
        avg_pips = self.total_pips_profit / self.trade_count
        
        return (
            f"Trades: {self.trade_count}, "
            f"Wins: {self.wins}, Losses: {self.losses}, "
            f"Win Rate: {win_rate:.1f}%, "
            f"Total Pips: {self.total_pips_profit:+.1f}, "
            f"Avg Pips: {avg_pips:+.1f}"
        )
    
    def log_status(self):
        """Log current trading status"""
        active_count = sum(1 for pos in self.active_positions.values() if pos and pos.get('status') == 'OPEN')
        
        self.logger.info("TradingClient Status:")
        self.logger.info(f"  Active Positions: {active_count}")
        self.logger.info(f"  {self.get_performance_summary()}")
        self.logger.info(f"  Session Time: {(time.time() - self.session_start_time) / 60:.1f} minutes") 