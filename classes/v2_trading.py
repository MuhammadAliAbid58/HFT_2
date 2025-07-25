import json
import logging
import time
from ctrader_fix import Client as FixClient, NewOrderSingle, LogonRequest, Heartbeat
import uuid
import threading
import os
from twisted.internet import reactor

# Simple logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
SYMBOLS = {1: {"name": "EURUSD"}, 2: {"name": "GBPUSD"}, 3: {"name": "USDJPY"}, 4: {"name": "AUDUSD"}}
INITIAL_LOT_SIZE = 100

class TestLiveTrading:
    def __init__(self):
        config_path = os.path.join(os.path.dirname(__file__), 'config-trade.json')
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.fix_client = None
        self.session_ready = threading.Event()
        self._position_lock = threading.Lock()
        self.active_positions = {}
        self.connected = False
        
    def connect(self):
        """Connect to FIX server"""
        try:
            self.fix_client = FixClient(
                host=self.config["Host"],
                port=self.config["Port"],
                ssl=self.config.get("SSL", False)
            )
            
            self.fix_client.setConnectedCallback(self.on_fix_connected)
            self.fix_client.setDisconnectedCallback(self.on_fix_disconnected)
            self.fix_client.setMessageReceivedCallback(self.on_fix_message)
            
            logging.info("Starting FIX client...")
            self.fix_client.startService()
            self.connected = True
            time.sleep(2)
            self._start_heartbeat()
            
        except Exception as e:
            logging.error(f"Failed to connect: {str(e)}")
            self.connected = False
    
    def _start_heartbeat(self):
        """Start heartbeats"""
        def send_heartbeat():
            if self.connected:
                try:
                    self.fix_client.send(Heartbeat(self.config))
                    reactor.callLater(int(self.config["HeartBeat"]), send_heartbeat)
                except Exception as e:
                    logging.error(f"Heartbeat error: {str(e)}")
        
        reactor.callLater(int(self.config["HeartBeat"]), send_heartbeat)
    
    def place_order(self, direction: str, symbol_id: int):
        """Place market order"""
        if not self.session_ready.is_set():
            logging.warning(f"FIX session not ready")
            return False
            
        try:
            with self._position_lock:
                order = NewOrderSingle(self.config)
                order.ClOrdID = str(uuid.uuid4())
                order.Symbol = symbol_id
                order.Side = 1 if direction.lower() == "buy" else 2
                order.OrderQty = INITIAL_LOT_SIZE * 1000
                order.OrdType = 1
                
                self.fix_client.send(order)
                logging.info(f"Order sent: {direction.upper()} {SYMBOLS[symbol_id]['name']}")
                
                self.active_positions[order.ClOrdID] = {
                    "symbol_id": symbol_id,
                    "side": direction.lower(),
                    "qty": INITIAL_LOT_SIZE,
                    "status": "PENDING",
                    "pos_maint_rpt_id": None
                }
                return True
                    
        except Exception as e:
            logging.error(f"Order failed: {str(e)}")
            return False

    def close_position(self, symbol_id=None, clord_id=None):
        """Close position by symbol_id or clord_id"""
        if not symbol_id and not clord_id:
            return False
        
        # Find position to close
        if clord_id:
            if clord_id not in self.active_positions:
                return False
            position = self.active_positions[clord_id]
            symbol_id = position["symbol_id"]
        else:
            clord_id = None
            for pos_id, position in self.active_positions.items():
                if position.get("symbol_id") == symbol_id and position["status"] == "FILLED":
                    clord_id = pos_id
                    break
            if not clord_id:
                return False
            position = self.active_positions[clord_id]
        
        if not position.get("pos_maint_rpt_id"):
            logging.error(f"No PosMaintRptID for position {clord_id[:8]}...")
            return False
        
        try:
            order = NewOrderSingle(self.config)
            order.ClOrdID = str(uuid.uuid4())
            order.Symbol = symbol_id
            order.Side = 2 if position["side"] == "buy" else 1
            order.OrderQty = position["qty"] * 1000
            order.OrdType = 1
            order.PosMaintRptID = position["pos_maint_rpt_id"]
            
            self.fix_client.send(order)
            logging.info(f"Close order sent: {SYMBOLS[symbol_id]['name']}")
            
            with self._position_lock:
                self.active_positions[clord_id]["status"] = "CLOSING"
            
            return True
        except Exception as e:
            logging.error(f"Close failed: {str(e)}")
            return False

    def on_fix_connected(self, client):
        """FIX connected callback"""
        logging.info("FIX connected, sending logon...")
        logon = LogonRequest(self.config)
        logon.ResetSeqNum = "Y"
        self.fix_client.send(logon)

    def on_fix_disconnected(self, client, reason):
        """FIX disconnected callback"""
        logging.warning(f"FIX disconnected: {reason}")
        self.session_ready.clear()
        self.connected = False

    def on_fix_message(self, client, msg):
        """Handle FIX messages"""
        try:
            msg_type = msg.getFieldValue(35)
            
            def safe_get_field(msg_obj, field_id):
                try:
                    return msg_obj.getFieldValue(field_id)
                except:
                    return None
            
            if msg_type == "A":  # Logon
                self.session_ready.set()
                logging.info("FIX session ready")
                
            elif msg_type == "8":  # ExecutionReport
                ord_status = safe_get_field(msg, 39)
                clord_id = safe_get_field(msg, 11)
                
                if clord_id and clord_id in self.active_positions:
                    with self._position_lock:
                        position = self.active_positions[clord_id]
                        
                        if ord_status == '2':  # Filled
                            pos_maint_rpt_id = safe_get_field(msg, 721)
                            position.update({
                                "status": "FILLED",
                                "avg_price": safe_get_field(msg, 6),
                                "pos_maint_rpt_id": pos_maint_rpt_id
                            })
                            logging.info(f"Position filled: {SYMBOLS[position['symbol_id']]['name']}")
                        
                        elif ord_status in ['8', '4']:  # Rejected/Cancelled
                            del self.active_positions[clord_id]
                            logging.info(f"Order removed: {'rejected' if ord_status == '8' else 'cancelled'}")
                
                else:  # Closing order
                    pos_maint_rpt_id = safe_get_field(msg, 721)
                    if pos_maint_rpt_id and ord_status == '2':
                        for pos_clord_id, position in self.active_positions.items():
                            if (position.get("pos_maint_rpt_id") == pos_maint_rpt_id and 
                                position["status"] == "CLOSING"):
                                with self._position_lock:
                                    del self.active_positions[pos_clord_id]
                                logging.info(f"Position closed: {SYMBOLS[position['symbol_id']]['name']}")
                                break
                    
        except Exception as e:
            logging.error(f"Message processing error: {str(e)}")

    def get_active_positions_summary(self):
        """Show active positions"""
        with self._position_lock:
            if not self.active_positions:
                logging.info("No active positions")
                return {}
            
            logging.info(f"Active positions: {len(self.active_positions)}")
            for clord_id, pos in self.active_positions.items():
                logging.info(f"  {SYMBOLS[pos['symbol_id']]['name']}: {pos['side'].upper()} - {pos['status']}")
            return self.active_positions.copy()

    def close_all_positions(self, timeout_seconds=10):
        """Close all positions and wait"""
        logging.info("Closing all positions...")
        
        positions_to_close = list(self.active_positions.keys())
        if not positions_to_close:
            logging.info("No positions to close")
            return True
        
        # Send close orders
        success_count = 0
        for clord_id in positions_to_close:
            position = self.active_positions.get(clord_id)
            if position and position["status"] != "CLOSING":
                if self.close_position(clord_id=clord_id):
                    success_count += 1
        
        # Wait for closure
        if success_count > 0:
            start_time = time.time()
            while time.time() - start_time < timeout_seconds:
                with self._position_lock:
                    if not self.active_positions:
                        logging.info("All positions closed")
                        return True
                time.sleep(1)
            
            logging.warning("Timeout waiting for positions to close")
            return False
        
        return True

    def disconnect(self):
        """Disconnect FIX"""
        if self.connected and self.fix_client:
            try:
                self.connected = False
                self.fix_client.stopService()
                logging.info("Disconnected")
            except Exception as e:
                logging.error(f"Disconnect error: {str(e)}")

def test_live_trading():
    trader = None
    
    def run_test():
        nonlocal trader
        trader = TestLiveTrading()
        trader.connect()
        reactor.callLater(5, check_and_trade)
    
    def check_and_trade():
        nonlocal trader
        if trader.session_ready.is_set():
            logging.info("Session ready, starting tests...")
            reactor.callLater(1, buy_test)
        else:
            logging.error("Session not ready")
            reactor.stop()
    
    def buy_test():
        nonlocal trader
        logging.info("Testing buy order...")
        if trader.place_order("buy", 1):
            reactor.callLater(5, close_buy)
        else:
            reactor.callLater(1, sell_test)
    
    def close_buy():
        nonlocal trader
        logging.info("Closing buy position...")
        trader.close_position(1)
        reactor.callLater(2, sell_test)
    
    def sell_test():
        nonlocal trader
        logging.info("Testing sell order...")
        if trader.place_order("sell", 1):
            reactor.callLater(5, close_sell)
        else:
            reactor.callLater(1, cleanup)
    
    def close_sell():
        nonlocal trader
        logging.info("Closing sell position...")
        trader.close_position(1)
        reactor.callLater(2, cleanup)
    
    def cleanup():
        nonlocal trader
        logging.info("Cleanup...")
        if trader and trader.connected:
            trader.get_active_positions_summary()
            if trader.active_positions:
                trader.close_all_positions(15)
            trader.disconnect()
        logging.info("Test completed")
        reactor.stop()
    
    reactor.callWhenRunning(run_test)
    reactor.callLater(60, lambda: reactor.stop())  # Timeout
    logging.info("Starting reactor...")
    reactor.run()

if __name__ == "__main__":
    test_live_trading() 