import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional
from twisted.internet import reactor
from ctrader_open_api import Client as OpenApiClient, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import ProtoHeartbeatEvent
from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAApplicationAuthReq, ProtoOAApplicationAuthRes,
    ProtoOAGetAccountListByAccessTokenReq, ProtoOAGetAccountListByAccessTokenRes,
    ProtoOAAccountAuthReq, ProtoOAAccountAuthRes,
    ProtoOASubscribeSpotsReq, ProtoOASubscribeSpotsRes, ProtoOASpotEvent,
    ProtoOASubscribeDepthQuotesReq, ProtoOASubscribeDepthQuotesRes, ProtoOADepthEvent,
    ProtoOAErrorRes
)
from ctrader_fix import Client as FixClient

from .globals import (
    APP_CLIENT_ID, APP_CLIENT_SECRET, ACCESS_TOKEN, REFRESH_TOKEN, HOST, PORT,
    SYMBOLS, CONFIG_TRADE, timing_decorator, GLOBAL_TRADE_OPEN, GLOBAL_TRADE_LOCK,
    MIN_CONFIDENCE_SCORE, PRICE_SCALE_FACTOR, VOLUME_CONVERSION_FACTOR
)
from .TickClient import TickClient
from .DepthClient import DepthClient
from .TradingClient import TradingClient
from .PerformanceMonitor import performance_monitor
from .DataLogger import data_logger

class DataCollector:
    """
    Simplified data collector for coordinating multi-symbol trading.
    Handles tick/DOM data collection and basic trading decisions.
    """
    
    def __init__(self):
        self.logger = logging.getLogger('DataCollector')
        
        # OpenAPI client for market data
        self.client_openapi = OpenApiClient(HOST, PORT, TcpProtocol)
        
        # FIX client for trading
        self.client_fix = FixClient(
            host=CONFIG_TRADE["Host"],
            port=CONFIG_TRADE["Port"],
            ssl=CONFIG_TRADE.get("SSL", False)
        )
        
        # Initialize clients for each symbol
        self.tick_clients = {}
        self.depth_clients = {}
        self.trade_locks = {}
        self.symbol_trading_state = {}
        
        for symbol_id in SYMBOLS.keys():
            symbol_name = SYMBOLS[symbol_id]['name']
            self.tick_clients[symbol_id] = TickClient(symbol_id)  # Pass symbol_id
            self.depth_clients[symbol_id] = DepthClient(symbol_id)  # Pass symbol_id
            self.trade_locks[symbol_id] = threading.Lock()
            self.symbol_trading_state[symbol_id] = {"is_trading": False, "last_trade_time": 0}
        
        # Initialize trading client
        self.trading_client = TradingClient(
            self.client_fix, 
            CONFIG_TRADE, 
            self.tick_clients,
            self.depth_clients
        )
        
        # Thread pool for parallel processing
        self.thread_pool = ThreadPoolExecutor(max_workers=len(SYMBOLS))
        
        # Market data subscription tracking
        self.spot_subscriptions = {sid: False for sid in SYMBOLS.keys()}
        self.depth_subscriptions = {sid: False for sid in SYMBOLS.keys()}
        
        # Connection state
        self.openapi_connected = False
        self.fix_connected = False
        self.account_id = None
        self.ctidTraderAccountId = None
        
        # Setup callbacks
        self._setup_callbacks()
        
        # Start data logging session
        data_logger.start_session()
        
        self.logger.info("DataCollector initialized for simplified HFT trading")
    
    def _setup_callbacks(self):
        """Setup client callbacks"""
        # OpenAPI callbacks
        self.client_openapi.setConnectedCallback(self.on_openapi_connected)
        self.client_openapi.setDisconnectedCallback(self.on_openapi_disconnected)
        self.client_openapi.setMessageReceivedCallback(self.on_openapi_message)
        
        # FIX callbacks
        self.client_fix.setConnectedCallback(self.on_fix_connected)
        self.client_fix.setDisconnectedCallback(self.on_fix_disconnected)
        self.client_fix.setMessageReceivedCallback(self.on_fix_message)
    
    # ================= CONNECTION HANDLERS =================
    
    def on_openapi_connected(self, client):
        """Handle OpenAPI connection"""
        self.logger.info("OpenAPI connected - authenticating...")
        self.openapi_connected = True
        
        # Authenticate application
        auth_req = ProtoOAApplicationAuthReq()
        auth_req.clientId = APP_CLIENT_ID
        auth_req.clientSecret = APP_CLIENT_SECRET
        client.send(auth_req)
    
    def on_openapi_disconnected(self, client, reason=None):
        """Handle OpenAPI disconnection"""
        self.logger.warning(f"OpenAPI disconnected: {reason}")
        self.openapi_connected = False
        # Schedule reactor stop after brief delay
        reactor.callLater(1, reactor.stop)
    
    def on_fix_connected(self, client):
        """Handle FIX connection"""
        self.logger.info("FIX connected")
        self.fix_connected = True
        self.trading_client.session_ready.set()
    
    def on_fix_disconnected(self, client, reason=None):
        """Handle FIX disconnection"""
        self.logger.warning(f"FIX disconnected: {reason}")
        self.fix_connected = False
        self.trading_client.session_ready.clear()
    
    # ================= MESSAGE HANDLERS =================
    
    @timing_decorator
    def on_openapi_message(self, client, message):
        """Handle OpenAPI messages"""
        try:
            # Capture receive timestamp immediately
            received_timestamp = time.time()
            
            # Skip heartbeat messages
            if hasattr(message, 'payloadType') and message.payloadType == ProtoHeartbeatEvent().payloadType:
                return
            
            # Extract payload from message
            payload = Protobuf.extract(message)
            
            # Log raw payload for all messages
            if payload:
                # Convert payload to dictionary for logging
                payload_dict = self._protobuf_to_dict(payload)
                data_logger.log_raw_payload(f"openapi_message_{payload.__class__.__name__}", payload_dict, received_timestamp)
            
            if isinstance(payload, ProtoOAErrorRes):
                self.logger.error(f"OpenAPI Error {payload.errorCode}: {payload.description}")
                return
                
            elif isinstance(payload, ProtoOAApplicationAuthRes):
                self.logger.info("Application authenticated - getting accounts...")
                
                # Get account list
                account_req = ProtoOAGetAccountListByAccessTokenReq()
                account_req.accessToken = ACCESS_TOKEN
                client.send(account_req)
                
            elif isinstance(payload, ProtoOAGetAccountListByAccessTokenRes):
                # Use any available account (prioritize working account if available)
                selected_account = None
                if payload.ctidTraderAccount:
                    self.logger.info("Available accounts:")
                    for account in payload.ctidTraderAccount:
                        account_id = account.ctidTraderAccountId
                        self.logger.info(f"  - Account ID: {account_id}")
                        
                        # Prioritize the working account if available
                        if account_id == 43035165:
                            selected_account = account_id
                            self.logger.info(f"✅ Found working account: {account_id}")
                            break
                        elif selected_account is None:
                            # Use first available account as fallback
                            selected_account = account_id
                    
                    if selected_account:
                        self.logger.info(f"Using account: {selected_account}")
                        self.account_id = selected_account
                        self.ctidTraderAccountId = selected_account
                        
                        # Authenticate account
                        auth_req = ProtoOAAccountAuthReq()
                        auth_req.ctidTraderAccountId = selected_account
                        auth_req.accessToken = ACCESS_TOKEN
                        client.send(auth_req)
                    else:
                        self.logger.error("❌ No accounts found!")
                        return
                
            elif isinstance(payload, ProtoOAAccountAuthRes):
                account_id = payload.ctidTraderAccountId
                self.logger.info(f"Account {account_id} authenticated - subscribing to market data...")
                self._subscribe_to_market_data()
                
            elif isinstance(payload, ProtoOASpotEvent):
                self._handle_spot_event(payload, received_timestamp)
                
            elif isinstance(payload, ProtoOADepthEvent):
                self._handle_depth_event(payload, received_timestamp)
                
            elif isinstance(payload, ProtoOASubscribeSpotsRes):
                self.logger.info(f"Spot subscription confirmed for account {payload.ctidTraderAccountId}")
                # Set subscription status for ALL symbols (like working system)
                for symbol_id in SYMBOLS.keys():
                    self.spot_subscriptions[symbol_id] = True
                
            elif isinstance(payload, ProtoOASubscribeDepthQuotesRes):
                self.logger.info(f"Depth subscription confirmed for account {payload.ctidTraderAccountId}")
                # Set subscription status for ALL symbols (like working system)
                for symbol_id in SYMBOLS.keys():
                    self.depth_subscriptions[symbol_id] = True
                
        except Exception as e:
            self.logger.error(f"Error handling OpenAPI message: {str(e)}")
            import traceback
            self.logger.error(f"Stack trace: {traceback.format_exc()}")
    
    @timing_decorator
    def on_fix_message(self, client, message):
        """Handle FIX messages"""
        try:
            # Delegate to trading client for execution reports
            if hasattr(message, 'MsgType'):
                msg_type = getattr(message, 'MsgType', '')
                if msg_type == '8':  # Execution Report
                    self.trading_client.process_execution_report(message)
                    
        except Exception as e:
            self.logger.error(f"Error handling FIX message: {str(e)}")
    
    # ================= MARKET DATA HANDLERS =================
    
    @timing_decorator
    def _handle_spot_event(self, payload, received_timestamp: float):
        """Handle spot price updates"""
        try:
            symbol_id = payload.symbolId
            if symbol_id not in SYMBOLS:
                return
            
            symbol_name = SYMBOLS[symbol_id]['name']
            
            # Extract and scale spot data (like working system)
            spot_data = {}
            bid = payload.bid / PRICE_SCALE_FACTOR if hasattr(payload, 'bid') and payload.bid else None
            ask = payload.ask / PRICE_SCALE_FACTOR if hasattr(payload, 'ask') and payload.ask else None
            
            if bid and ask:
                spot_data['price'] = bid  # Use bid as main price
                spot_data['bid'] = bid
                spot_data['ask'] = ask
                spot_data['spread'] = ask - bid
            elif bid:
                spot_data['price'] = bid
                spot_data['bid'] = bid
            elif ask:
                spot_data['price'] = ask
                spot_data['ask'] = ask
            
            # Add timestamp (ensure it's in milliseconds)
            if hasattr(payload, 'timestamp'):
                spot_data['timestamp'] = payload.timestamp  # API provides in milliseconds
            else:
                spot_data['timestamp'] = int(time.time() * 1000)  # Convert to milliseconds
            
            if 'price' in spot_data:
                # Add previous price
                if self.tick_clients[symbol_id].spots:
                    spot_data['previous_price'] = self.tick_clients[symbol_id].spots[-1].get('price', spot_data['price'])
                
                # Update tick client
                self.tick_clients[symbol_id].update_spot(spot_data)
                
                self.logger.debug(f"Spot update for {symbol_name}: price={spot_data['price']:.5f}, spread={spot_data.get('spread', 0):.5f}")
                
                # Process for trading if data is ready
                if self._is_market_data_ready(symbol_id):
                    # Check SL/TP first
                    self.trading_client.check_sl_tp(spot_data, symbol_id)
                    
                    # Check for new trading opportunities
                    self._process_trading_opportunity(symbol_id, spot_data)
                else:
                    self.logger.debug(f"Waiting for market data initialization for {symbol_name}")
                
        except Exception as e:
            self.logger.error(f"Error handling spot event: {str(e)}")
            import traceback
            self.logger.error(f"Stack trace: {traceback.format_exc()}")
    
    @timing_decorator
    def _handle_depth_event(self, payload, received_timestamp: float):
        """Handle depth/DOM updates"""
        try:
            symbol_id = payload.symbolId
            if symbol_id not in SYMBOLS:
                return
            
            symbol_name = SYMBOLS[symbol_id]['name']
            
            # Extract new quotes (like working system)
            new_quotes = []
            for q in payload.newQuotes:
                if q.bid or q.ask:
                    side = "bid" if q.bid else "ask"
                    price = (q.bid or q.ask) / PRICE_SCALE_FACTOR
                    volume = q.size / VOLUME_CONVERSION_FACTOR
                    entry = {"id": q.id, "side": side, "price": price, "volume": volume}
                    new_quotes.append(entry)
            
            # Get current quotes and remove deleted ones
            current_quotes = [q for q in self.depth_clients[symbol_id].get_current_quotes() 
                            if q.get("id") not in payload.deletedQuotes]
            
            # Add new quotes
            current_quotes.extend(new_quotes)
            
            # Update depth client
            self.depth_clients[symbol_id].update_quotes(current_quotes)
            
            self.logger.debug(f"DOM update for {symbol_name}: {len(new_quotes)} new quotes, {len(payload.deletedQuotes)} deleted")
            
        except Exception as e:
            self.logger.error(f"Error handling depth event: {str(e)}")
            import traceback
            self.logger.error(f"Stack trace: {traceback.format_exc()}")
    
    # ================= TRADING LOGIC =================
    
    @timing_decorator
    def _process_trading_opportunity(self, symbol_id: int, spot_data: Dict):
        """Process potential trading opportunity for a symbol"""
        global GLOBAL_TRADE_OPEN
        
        symbol_name = SYMBOLS[symbol_id]['name']
        
        try:
            # Check if trading is allowed
            with GLOBAL_TRADE_LOCK:
                if GLOBAL_TRADE_OPEN:
                    return  # Another trade is already open
            
            # Check if symbol is already trading
            with self.trade_locks[symbol_id]:
                if self.symbol_trading_state[symbol_id]["is_trading"]:
                    return
                
                # Check for existing position
                position = self.trading_client.get_active_position(symbol_id)
                if position:
                    return
            
            # Basic trading decision logic
            trade_decision = self._make_trading_decision(symbol_id, spot_data)
            
            if trade_decision["action"] == "BUY":
                success = self.trading_client.place_order("buy", symbol_id)
                if success:
                    with self.trade_locks[symbol_id]:
                        self.symbol_trading_state[symbol_id]["is_trading"] = True
                        self.symbol_trading_state[symbol_id]["last_trade_time"] = time.time()
                    
            elif trade_decision["action"] == "SELL":
                success = self.trading_client.place_order("sell", symbol_id)
                if success:
                    with self.trade_locks[symbol_id]:
                        self.symbol_trading_state[symbol_id]["is_trading"] = True
                        self.symbol_trading_state[symbol_id]["last_trade_time"] = time.time()
                        
        except Exception as e:
            self.logger.error(f"Error processing trading opportunity for {symbol_name}: {str(e)}")
    
    @timing_decorator
    def _make_trading_decision(self, symbol_id: int, spot_data: Dict) -> Dict:
        """
        Basic trading decision logic.
        Replace this with your own strategy logic.
        """
        
        # Get market data
        tick_stats = self.tick_clients[symbol_id].get_price_movement_stats()
        depth_stats = self.depth_clients[symbol_id].get_imbalance_stats()
        
        # Basic filters
        current_spread = spot_data.get('spread', 0)
        if current_spread > 0.0002:  # 2 pips max spread
            return {"action": "HOLD", "reason": "spread_too_wide"}
        
        # Simple momentum-based decision
        direction_bias = self.tick_clients[symbol_id].get_recent_direction_bias(10)
        imbalance = depth_stats.get('current_imbalance', 0)
        
        # Confidence calculation (basic)
        confidence = abs(direction_bias) + abs(imbalance) * 0.5
        
        if confidence < MIN_CONFIDENCE_SCORE:
            return {"action": "HOLD", "reason": "low_confidence"}
        
        # Decision logic
        if direction_bias > 0.3 and imbalance > 0.2:  # Strong upward bias
            return {"action": "BUY", "confidence": confidence, "bias": direction_bias}
        elif direction_bias < -0.3 and imbalance < -0.2:  # Strong downward bias
            return {"action": "SELL", "confidence": confidence, "bias": direction_bias}
        else:
            return {"action": "HOLD", "reason": "insufficient_signal"}
    
    # ================= UTILITY METHODS =================
    
    def _subscribe_to_market_data(self):
        """Subscribe to spot and depth data for all symbols (like working system)"""
        try:
            # Subscribe to spots for ALL symbols in one request
            spot_req = ProtoOASubscribeSpotsReq()
            spot_req.ctidTraderAccountId = self.account_id
            spot_req.subscribeToSpotTimestamp = True
            
            for symbol_id in SYMBOLS.keys():
                spot_req.symbolId.append(symbol_id)
            
            self.client_openapi.send(spot_req)
            self.logger.info(f"Subscribed to spots for all symbols: {list(SYMBOLS.keys())}")
            
            # Subscribe to depth for ALL symbols
            for symbol_id in SYMBOLS.keys():
                depth_req = ProtoOASubscribeDepthQuotesReq()
                depth_req.ctidTraderAccountId = self.account_id
                depth_req.symbolId.append(symbol_id)
                self.client_openapi.send(depth_req)
            
            self.logger.info(f"Subscribed to depth for all symbols: {list(SYMBOLS.keys())}")
                
        except Exception as e:
            self.logger.error(f"Error subscribing to market data: {str(e)}")
            import traceback
            self.logger.error(f"Stack trace: {traceback.format_exc()}")
    
    def _is_market_data_ready(self, symbol_id: int) -> bool:
        """Check if market data is ready for analysis"""
        tick_ready = (self.spot_subscriptions.get(symbol_id, False) and 
                     self.tick_clients[symbol_id].has_sufficient_data())
        depth_ready = (self.depth_subscriptions.get(symbol_id, False) and 
                      self.depth_clients[symbol_id].has_sufficient_data())
        
        if not tick_ready:
            self.logger.debug(f"Tick data not ready for {SYMBOLS[symbol_id]['name']}: spot_sub={self.spot_subscriptions.get(symbol_id, False)}, sufficient_data={self.tick_clients[symbol_id].has_sufficient_data()}")
        if not depth_ready:
            self.logger.debug(f"Depth data not ready for {SYMBOLS[symbol_id]['name']}: depth_sub={self.depth_subscriptions.get(symbol_id, False)}, sufficient_data={self.depth_clients[symbol_id].has_sufficient_data()}")
        
        return tick_ready and depth_ready
    
    def start(self):
        """Start the data collector"""
        try:
            self.logger.info("Starting HFT Base Lite data collector...")
            
            # Start OpenAPI service
            self.client_openapi.startService()
            
            # Start FIX service  
            self.client_fix.startService()
            
            # Start reactor
            reactor.run()
            
        except Exception as e:
            self.logger.error(f"Error starting data collector: {str(e)}")
            raise
    
    def stop(self):
        """Stop the data collector"""
        try:
            self.logger.info("Stopping data collector...")
            
            # Generate final latency report
            data_logger.log_latency_report()
            
            # Close data log files
            data_logger.close_log_files()
            
            # Shutdown thread pool
            self.thread_pool.shutdown(wait=True)
            
            # Stop reactor (clients will be stopped automatically)
            if reactor.running:
                reactor.stop()
                
        except Exception as e:
            self.logger.error(f"Error stopping data collector: {str(e)}")
    
    def get_system_status(self) -> Dict:
        """Get current system status"""
        active_positions = sum(
            1 for pos in self.trading_client.active_positions.values() 
            if pos and pos.get('status') == 'OPEN'
        )
        
        ready_symbols = sum(
            1 for symbol_id in SYMBOLS.keys() 
            if self._is_market_data_ready(symbol_id)
        )
        
        return {
            'openapi_connected': self.openapi_connected,
            'fix_connected': self.fix_connected,
            'account_id': self.account_id,
            'active_positions': active_positions,
            'ready_symbols': ready_symbols,
            'total_symbols': len(SYMBOLS),
            'performance': self.trading_client.get_performance_summary()
        }
    
    def log_system_status(self):
        """Log current system status"""
        status = self.get_system_status()
        
        self.logger.info("=== System Status ===")
        self.logger.info(f"OpenAPI: {'Connected' if status['openapi_connected'] else 'Disconnected'}")
        self.logger.info(f"FIX: {'Connected' if status['fix_connected'] else 'Disconnected'}")
        self.logger.info(f"Active Positions: {status['active_positions']}")
        self.logger.info(f"Ready Symbols: {status['ready_symbols']}/{status['total_symbols']}")
        self.logger.info(f"Performance: {status['performance']}")
        self.logger.info("====================")
    
    def start_performance_reporting(self):
        """Start periodic performance reporting"""
        def report_performance():
            try:
                self.log_system_status()
                performance_monitor.log_performance_report()
                
                # Log data latency analysis
                data_logger.log_latency_report()
                
                # Log data rates
                rates = data_logger.get_data_rates()
                self.logger.info("DATA RATES:")
                for symbol, rate_info in rates.items():
                    self.logger.info(f"  {symbol}: Tick={rate_info['tick_rate_per_sec']:.1f}/s, DOM={rate_info['dom_rate_per_sec']:.1f}/s")
                
                # Schedule next report
                reactor.callLater(30, report_performance)  # Every 30 seconds
                
            except Exception as e:
                self.logger.error(f"Error in performance reporting: {str(e)}")
        
        # Start first report after 10 seconds
        reactor.callLater(10, report_performance) 

    def _protobuf_to_dict(self, protobuf_obj):
        """Convert protobuf object to dictionary for logging"""
        try:
            result = {}
            for field in protobuf_obj.DESCRIPTOR.fields:
                field_name = field.name
                field_value = getattr(protobuf_obj, field_name)
                
                # Handle different field types
                if field.type == field.TYPE_MESSAGE:
                    if field.label == field.LABEL_REPEATED:
                        # Repeated message field
                        result[field_name] = [self._protobuf_to_dict(item) for item in field_value]
                    else:
                        # Single message field
                        if field_value:
                            result[field_name] = self._protobuf_to_dict(field_value)
                        else:
                            result[field_name] = None
                elif field.label == field.LABEL_REPEATED:
                    # Repeated primitive field
                    result[field_name] = list(field_value)
                else:
                    # Single primitive field
                    result[field_name] = field_value
            
            return result
        except Exception as e:
            return {"error": f"Failed to convert protobuf: {str(e)}"} 