import json
import logging
import time
from ctrader_fix import Client as FixClient, NewOrderSingle, LogonRequest, Heartbeat
from logging.handlers import RotatingFileHandler
import uuid
import threading
import os
from twisted.internet import reactor

# Configure logging
logging.basicConfig(level=logging.INFO)
fix_logger = logging.getLogger('ctrader_fix')
fix_logger.setLevel(logging.INFO)
fix_handler = RotatingFileHandler('logs/fix_messages.log', maxBytes=10*1024*1024, backupCount=5)
fix_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fix_handler.setFormatter(fix_formatter)
fix_logger.addHandler(fix_handler)

# Constants from globals.py
SYMBOLS = {
    1: {"name": "EURUSD", "pip_value": 0.0001},
    2: {"name": "GBPUSD", "pip_value": 0.0001},
    3: {"name": "USDJPY", "pip_value": 0.01},
    4: {"name": "AUDUSD", "pip_value": 0.0001}
}

INITIAL_LOT_SIZE = 100  # 1 lot = 100,000 units

class TestLiveTrading:
    def __init__(self):
        # Load configuration from config-trade.json
        config_path = os.path.join(os.path.dirname(__file__), 'config-trade.json')
        with open(config_path, 'r') as f:
            self.trade_config = json.load(f)
        
        # Using the config directly from the JSON file
        self.config = self.trade_config.copy()
        
        # Initialize components
        self.fix_client = None
        self.session_ready = threading.Event()
        self._position_lock = threading.Lock()
        self.active_positions = {}
        self.connected = False
        
    def connect(self):
        """Connect to FIX server"""
        try:
            # Initialize FIX client like in HFTDataCollector
            self.fix_client = FixClient(
                host=self.config["Host"],
                port=self.config["Port"],
                ssl=self.config.get("SSL", False)
            )
            
            # Setup callbacks using the method from HFTDataCollector
            self.fix_client.setConnectedCallback(self.on_fix_connected)
            self.fix_client.setDisconnectedCallback(self.on_fix_disconnected)
            self.fix_client.setMessageReceivedCallback(self.on_fix_message)
            
            # Start the FIX client service
            logging.info("About to start FIX client service...")
            self.fix_client.startService()
            self.connected = True
            logging.info("FIX client service started successfully")
            
            # Give the service a moment to establish connection
            time.sleep(2)
            
            # Start heartbeat thread
            self._start_heartbeat()
            
        except Exception as e:
            logging.error(f"Failed to connect to FIX server: {str(e)}")
            self.connected = False
    
    def _start_heartbeat(self):
        """Start sending heartbeats using reactor scheduler"""
        def send_heartbeat():
            if self.connected:
                try:
                    heartbeat = Heartbeat(self.config)
                    self.fix_client.send(heartbeat)
                    # Schedule next heartbeat
                    reactor.callLater(int(self.config["HeartBeat"]), send_heartbeat)
                except Exception as e:
                    logging.error(f"Heartbeat error: {str(e)}")
        
        # Schedule first heartbeat
        reactor.callLater(int(self.config["HeartBeat"]), send_heartbeat)
    
    def place_order(self, direction: str, symbol_id: int):
        """Place a market order for a specific symbol"""
        symbol_name = SYMBOLS[symbol_id]["name"]
        
        if not self.session_ready.is_set():
            logging.warning(f"[{symbol_name}] 🚫 ORDER BLOCKED: FIX session not ready")
            return False
            
        try:
            with self._position_lock:
                # Create order request with config
                order = NewOrderSingle(self.config)
                order.ClOrdID = str(uuid.uuid4())
                order.Symbol = symbol_id  # Use symbol ID (integer), not name
                order.Side = 1 if direction.lower() == "buy" else 2  # Use integers, not strings
                order.OrderQty = INITIAL_LOT_SIZE * 1000  # Convert lots to units (1 lot = 100,000 units)
                order.OrdType = 1  # Market order (integer, not string)
                
                try:
                    # Send order
                    order_send_time = time.perf_counter()
                    self.fix_client.send(order)
                    order_latency = time.perf_counter() - order_send_time
                    
                    logging.info(f"[{symbol_name}] ✅ ORDER SENT SUCCESSFULLY")
                    logging.info(f"[{symbol_name}] ClOrdID: {order.ClOrdID}")
                    logging.info(f"[{symbol_name}] Direction: {direction.upper()}")
                    logging.info(f"[{symbol_name}] Order Quantity: {INITIAL_LOT_SIZE/100:.2f} lots")
                    logging.info(f"[{symbol_name}] Send Latency: {order_latency*1000:.2f}ms")
                    
                    # Store position with order ID for hedging mode
                    self.active_positions[order.ClOrdID] = {
                        "symbol_id": symbol_id,
                        "side": direction.lower(),
                        "qty": INITIAL_LOT_SIZE,
                        "entry_time": time.time(),
                        "status": "PENDING",
                        "broker_order_id": None,  # Will be updated from execution report
                        "pos_maint_rpt_id": None  # Will be updated from execution report
                    }
                    
                    return True
                    
                except Exception as e:
                    logging.error(f"[{symbol_name}] ❌ FAILED TO SEND ORDER: {str(e)}")
                    return False
        
        except Exception as e:
            logging.error(f"[{symbol_name}] ❌ ERROR IN PLACE_ORDER: {str(e)}")
            return False

    def close_position_by_id(self, clord_id: str):
        """Close a specific position by its ClOrdID using PosMaintRptID"""
        if clord_id not in self.active_positions:
            logging.warning(f"Position {clord_id[:8]}... not found in active positions")
            return False
        
        position = self.active_positions[clord_id]
        symbol_id = position["symbol_id"]
        symbol_name = SYMBOLS[symbol_id]["name"]
        
        # Check if we have PosMaintRptID (required for closing)
        if not position.get("pos_maint_rpt_id"):
            logging.error(f"[{symbol_name}] ❌ No PosMaintRptID found for position {clord_id[:8]}...")
            return False
        
        try:
            # Create a closing order with PosMaintRptID
            order = NewOrderSingle(self.config)
            order.ClOrdID = str(uuid.uuid4())
            order.Symbol = symbol_id
            order.Side = 2 if position["side"] == "buy" else 1  # Opposite side
            order.OrderQty = position["qty"] * 1000  # Same quantity
            order.OrdType = 1  # Market order
            
            # THIS IS THE KEY: Set PosMaintRptID to specify which position to close
            order.PosMaintRptID = position["pos_maint_rpt_id"]
            
            # Send the closing order
            order_send_time = time.perf_counter()
            self.fix_client.send(order)
            order_latency = time.perf_counter() - order_send_time
            
            logging.info(f"[{symbol_name}] ✅ CLOSE ORDER SENT")
            logging.info(f"[{symbol_name}] Closing Position: {clord_id[:8]}...")
            logging.info(f"[{symbol_name}] PosMaintRptID: {position['pos_maint_rpt_id']}")
            logging.info(f"[{symbol_name}] Close ClOrdID: {order.ClOrdID}")
            logging.info(f"[{symbol_name}] Close Direction: {'SELL' if position['side'] == 'buy' else 'BUY'}")
            logging.info(f"[{symbol_name}] Send Latency: {order_latency*1000:.2f}ms")
            
            # Mark position as closing
            with self._position_lock:
                self.active_positions[clord_id]["status"] = "CLOSING"
            
            return True
            
        except Exception as e:
            logging.error(f"[{symbol_name}] ❌ FAILED TO SEND CLOSE ORDER: {str(e)}")
            return False

    def close_position(self, symbol_id: int):
        """Close the first open position for a symbol in hedging mode"""
        # Find first FILLED position for this symbol
        for clord_id, position in self.active_positions.items():
            if (position.get("symbol_id") == symbol_id and 
                position["status"] == "FILLED"):
                
                symbol_name = SYMBOLS[symbol_id]["name"]
                logging.info(f"[{symbol_name}] Attempting to close {position['side'].upper()} position (ClOrdID: {clord_id[:8]}...)")
                return self.close_position_by_id(clord_id)
        
        logging.warning(f"No active FILLED positions found for {SYMBOLS[symbol_id]['name']}")
        return False

    def on_fix_connected(self, client):
        """Callback when FIX client connects"""
        logging.info("FIX client connected. Sending logon request...")
        try:
            logon = LogonRequest(self.config)
            logon.ResetSeqNum = "Y"
            
            # Log the logon request details
            logging.info(f"Logon Config: SenderCompID={self.config.get('SenderCompID')}")
            logging.info(f"Logon Config: TargetCompID={self.config.get('TargetCompID')}")
            logging.info(f"Logon Config: Username={self.config.get('Username')}")
            
            self.fix_client.send(logon)
            logging.info("Logon request sent successfully")
        except Exception as e:
            logging.error(f"Failed to send logon request: {str(e)}")
            import traceback
            traceback.print_exc()

    def on_fix_disconnected(self, client, reason):
        """Callback when FIX client disconnects"""
        logging.warning(f"FIX disconnected: {reason}. Attempting reconnect in 5 seconds...")
        self.session_ready.clear()
        self.connected = False

    def on_fix_message(self, client, msg):
        """Callback for FIX messages"""
        try:
            text = msg.getMessage().replace("\x01", "|")
            logging.info(f"FIX Received: {text}")
            
            msg_type = msg.getFieldValue(35)
            logging.info(f"FIX Message Type: {msg_type}")
            
            # Helper function to safely get fields
            def safe_get_field(msg_obj, field_id):
                try:
                    return msg_obj.getFieldValue(field_id)
                except Exception:
                    return None
            
            if msg_type == "A":  # Logon
                self.session_ready.set()
                logging.info("FIX SESSION READY - Trading session authenticated")
                
            elif msg_type == "8":  # ExecutionReport
                exec_type = safe_get_field(msg, 150)
                ord_status = safe_get_field(msg, 39)
                symbol = safe_get_field(msg, 55)
                side = safe_get_field(msg, 54)
                order_qty = safe_get_field(msg, 38)
                price = safe_get_field(msg, 44)
                clord_id = safe_get_field(msg, 11)
                
                status_map = {
                    '0': 'New',
                    '1': 'Partially filled',
                    '2': 'Filled',
                    '4': 'Cancelled',
                    '8': 'Rejected',
                }
                
                exec_type_map = {
                    '0': 'New',
                    '1': 'Partial fill',
                    '2': 'Fill',
                    '4': 'Cancelled',
                    '8': 'Rejected',
                    'F': 'Trade',  # Add this mapping for exec type F
                }
                
                status = status_map.get(ord_status, f"Unknown status: {ord_status}")
                exec_status = exec_type_map.get(exec_type, f"Unknown exec type: {exec_type}")
                
                logging.info(f"\n=== Execution Report ===")
                logging.info(f"Symbol: {symbol}")
                logging.info(f"Side: {'BUY' if side == '1' else 'SELL'}")
                logging.info(f"ClOrdID: {clord_id}")
                logging.info(f"Status: {status}")
                logging.info(f"ExecType: {exec_status}")
                logging.info(f"Quantity: {order_qty}")
                if price:
                    logging.info(f"Price: {price}")
                
                # Handle position tracking based on execution reports
                if clord_id and clord_id in self.active_positions:
                    with self._position_lock:
                        position = self.active_positions[clord_id]
                        
                        if ord_status == '2':  # Filled
                            filled_qty = safe_get_field(msg, 14)  # CumQty
                            avg_px = safe_get_field(msg, 6)      # AvgPx
                            broker_order_id = safe_get_field(msg, 37)  # OrderID from broker
                            pos_maint_rpt_id = safe_get_field(msg, 721)  # PosMaintRptID
                            
                            # Log fill details
                            fill_info = []
                            if filled_qty is not None:
                                fill_info.append(f"Quantity: {filled_qty}")
                            if avg_px is not None:
                                fill_info.append(f"Price: {avg_px}")
                            if broker_order_id is not None:
                                fill_info.append(f"Order ID: {broker_order_id}")
                            if pos_maint_rpt_id:
                                fill_info.append(f"PosMaintRptID: {pos_maint_rpt_id}")
                            
                            if fill_info:
                                logging.info(f"Fill Details: {' | '.join(fill_info)}")
                            
                            # Update position with fill details
                            position.update({
                                "status": "FILLED",
                                "filled_qty": filled_qty,
                                "avg_price": avg_px,
                                "broker_order_id": broker_order_id,
                                "pos_maint_rpt_id": pos_maint_rpt_id
                            })
                            
                            symbol_name = SYMBOLS[position["symbol_id"]]["name"]
                            logging.info(f"[{symbol_name}] Position opened and filled (ClOrdID: {clord_id[:8]}...)")
                        
                        elif ord_status in ['8', '4']:  # Rejected or Cancelled
                            reason = safe_get_field(msg, 58) if ord_status == '8' else "Cancelled"
                            if ord_status == '8':
                                logging.error(f"❌ Order Rejected: {reason if reason else 'No reason provided'}")
                            
                            # Remove rejected/cancelled orders from tracking
                            del self.active_positions[clord_id]
                            symbol_name = SYMBOLS[position["symbol_id"]]["name"]
                            status_text = "Rejected" if ord_status == '8' else "Cancelled"
                            logging.info(f"[{symbol_name}] {status_text} order removed from tracking")
                
                # Handle closing orders - look for orders that reference a PosMaintRptID
                else:
                    pos_maint_rpt_id = safe_get_field(msg, 721)
                    
                    if pos_maint_rpt_id and ord_status == '2':  # Closing order filled
                        # Find the position being closed by PosMaintRptID
                        pos_clord_id_to_close = None
                        
                        for pos_clord_id, position in self.active_positions.items():
                            if (position.get("pos_maint_rpt_id") == pos_maint_rpt_id and 
                                position["status"] == "CLOSING"):
                                pos_clord_id_to_close = pos_clord_id
                                break
                        
                        if pos_clord_id_to_close:
                            position = self.active_positions[pos_clord_id_to_close]
                            symbol_name = SYMBOLS[position["symbol_id"]]["name"]
                            
                            logging.info(f"[{symbol_name}] ✅ POSITION CLOSED SUCCESSFULLY")
                            logging.info(f"[{symbol_name}] Closed Position: {pos_clord_id_to_close[:8]}...")
                            logging.info(f"[{symbol_name}] Close Order: {clord_id[:8]}...")
                            logging.info(f"[{symbol_name}] PosMaintRptID: {pos_maint_rpt_id}")
                            
                            # Remove the closed position from tracking
                            with self._position_lock:
                                del self.active_positions[pos_clord_id_to_close]
                            
                            logging.info(f"[{symbol_name}] Position removed from active tracking")
                        else:
                            logging.warning(f"Could not find position to close with PosMaintRptID: {pos_maint_rpt_id}")
                    
                    elif ord_status == "8":  # Rejected (when not in active positions)
                        reason = safe_get_field(msg, 58)
                        logging.error(f"❌ Order Rejected: {reason if reason else 'No reason provided'}")
                    
            elif msg_type == "3":  # Reject
                reason = safe_get_field(msg, 58)
                logging.error(f"❌ Message Rejected: {reason if reason else 'No reason provided'}")
                
        except Exception as e:
            logging.error(f"Error processing FIX message: {str(e)}")

    def get_active_positions_summary(self):
        """Get summary of active positions"""
        with self._position_lock:
            if not self.active_positions:
                logging.info("✅ No active positions tracked locally")
                return {}
            
            logging.info(f"📊 Active Positions Summary ({len(self.active_positions)} positions):")
            for clord_id, position in self.active_positions.items():
                symbol_name = SYMBOLS[position["symbol_id"]]["name"]
                logging.info(f"  - {symbol_name}: {position['side'].upper()} | Status: {position['status']} | Entry: {position.get('avg_price', 'N/A')} | ClOrdID: {clord_id[:8]}...")
            
            return self.active_positions.copy()

    def force_close_all_positions(self):
        """Force close all remaining positions"""
        logging.info("🔄 Force closing all remaining positions...")
        
        positions_to_close = list(self.active_positions.keys())
        if not positions_to_close:
            logging.info("✅ No positions to close")
            return True
        
        success_count = 0
        for clord_id in positions_to_close:
            position = self.active_positions.get(clord_id)
            if not position or position["status"] == "CLOSING":
                continue
                
            symbol_id = position["symbol_id"]
            symbol_name = SYMBOLS[symbol_id]["name"]
            
            logging.info(f"🔄 Force closing {symbol_name} {position['side'].upper()} position (ClOrdID: {clord_id[:8]}...)...")
            if self.close_position_by_id(clord_id):
                success_count += 1
                logging.info(f"✅ {symbol_name} close order sent")
            else:
                logging.error(f"❌ Failed to close {symbol_name}")
        
        logging.info(f"📊 Close orders sent: {success_count}/{len(positions_to_close)}")
        return success_count == len(positions_to_close)

    def wait_for_positions_to_close(self, timeout_seconds=10):
        """Wait for all positions to be closed with timeout"""
        logging.info(f"⏳ Waiting up to {timeout_seconds}s for positions to close...")
        
        start_time = time.time()
        while time.time() - start_time < timeout_seconds:
            with self._position_lock:
                if not self.active_positions:
                    logging.info("✅ All positions closed successfully")
                    return True
                
                # Log remaining positions
                remaining = [f"{SYMBOLS[pos['symbol_id']]['name']}({pos['status']})" 
                           for clord_id, pos in self.active_positions.items()]
                logging.info(f"⏳ Waiting for: {', '.join(remaining)}")
            
            time.sleep(1)
        
        # Timeout reached
        with self._position_lock:
            if self.active_positions:
                remaining = [f"{SYMBOLS[pos['symbol_id']]['name']}({pos['status']})" 
                           for clord_id, pos in self.active_positions.items()]
                logging.warning(f"⚠️ Timeout! Positions still open: {', '.join(remaining)}")
                return False
            else:
                logging.info("✅ All positions closed during timeout period")
                return True

    def disconnect(self):
        """Disconnect from FIX server"""
        if self.connected and self.fix_client:
            try:
                self.connected = False
                self.fix_client.stopService()
                logging.info("Disconnected from FIX server")
            except Exception as e:
                logging.error(f"Error disconnecting: {str(e)}")

def test_live_trading():
    trader = None
    
    def run_trading_test():
        """Run the trading test inside reactor"""
        nonlocal trader
        try:
            # Create trader instance
            trader = TestLiveTrading()
            
            # Connect to FIX server
            trader.connect()
            
            # Schedule session check after connection attempt
            reactor.callLater(5, check_session_and_trade)
            
        except Exception as e:
            logging.error(f"❌ Error in run_trading_test: {str(e)}")
            reactor.stop()
    
    def check_session_and_trade():
        """Check if session is ready and run trades"""
        nonlocal trader
        try:
            if trader.session_ready.is_set():
                logging.info("✅ FIX session is ready! Starting trading tests...")
                reactor.callLater(1, run_buy_test)
            else:
                logging.error("❌ FIX session not ready yet")
                if hasattr(trader, 'fix_client') and trader.fix_client:
                    logging.info("Debug: FIX client exists, but session not authenticated")
                reactor.stop()
        except Exception as e:
            logging.error(f"❌ Error in check_session_and_trade: {str(e)}")
            reactor.stop()
    
    def run_buy_test():
        """Run buy order test"""
        nonlocal trader
        try:
            logging.info("\n=== Testing Buy Order ===")
            success = trader.place_order("buy", 1)  # 1 = EURUSD
            if success:
                logging.info("✅ Buy order placed successfully")
                # Schedule position close after 5 seconds
                reactor.callLater(5, close_buy_position)
            else:
                logging.error("❌ Failed to place buy order")
                reactor.callLater(1, run_sell_test)
        except Exception as e:
            logging.error(f"❌ Error in run_buy_test: {str(e)}")
            reactor.stop()
    
    def close_buy_position():
        """Close buy position"""
        nonlocal trader
        try:
            logging.info("\n=== Closing Buy Position ===")
            if trader.close_position(1):
                logging.info("✅ Buy position closed successfully")
            else:
                logging.error("❌ Failed to close buy position")
            # Schedule sell test
            reactor.callLater(2, run_sell_test)
        except Exception as e:
            logging.error(f"❌ Error in close_buy_position: {str(e)}")
            reactor.stop()
    
    def run_sell_test():
        """Run sell order test"""
        nonlocal trader
        try:
            logging.info("\n=== Testing Sell Order ===")
            success = trader.place_order("sell", 1)  # 1 = EURUSD
            if success:
                logging.info("✅ Sell order placed successfully")
                # Schedule position close after 5 seconds
                reactor.callLater(5, close_sell_position)
            else:
                logging.error("❌ Failed to place sell order")
                reactor.callLater(1, cleanup_and_stop)
        except Exception as e:
            logging.error(f"❌ Error in run_sell_test: {str(e)}")
            reactor.stop()
    
    def close_sell_position():
        """Close sell position"""
        nonlocal trader
        try:
            logging.info("\n=== Closing Sell Position ===")
            if trader.close_position(1):
                logging.info("✅ Sell position closed successfully")
            else:
                logging.error("❌ Failed to close sell position")
            # Schedule cleanup
            reactor.callLater(2, cleanup_and_stop)
        except Exception as e:
            logging.error(f"❌ Error in close_sell_position: {str(e)}")
            reactor.stop()
    
    def cleanup_and_stop():
        """Clean up and stop reactor"""
        nonlocal trader
        try:
            logging.info("\n=== Starting Cleanup Process ===")
            
            if trader and trader.connected:
                # Check current positions
                trader.get_active_positions_summary()
                
                # Force close any remaining positions
                if trader.active_positions:
                    logging.info("🔄 Closing all remaining positions...")
                    trader.force_close_all_positions()
                    
                    # Wait for positions to close
                    if not trader.wait_for_positions_to_close(timeout_seconds=15):
                        logging.warning("⚠️ Some positions may still be open on broker side!")
                        logging.warning("⚠️ Check your broker platform to manually close any remaining positions")
                        
                        # Log final position summary
                        trader.get_active_positions_summary()
                else:
                    logging.info("✅ No local positions to clean up")
                
                # Give a moment for final messages
                time.sleep(2)
                
                # Disconnect
                trader.disconnect()
            
            logging.info("✅ Test completed, stopping reactor...")
            reactor.stop()
        except Exception as e:
            logging.error(f"❌ Error in cleanup_and_stop: {str(e)}")
            reactor.stop()
    
    # Start the test
    reactor.callWhenRunning(run_trading_test)
    
    # Add timeout to prevent hanging
    reactor.callLater(60, lambda: reactor.stop())  # 60 second timeout
    
    logging.info("Starting Twisted reactor...")
    reactor.run()

if __name__ == "__main__":
    test_live_trading() 