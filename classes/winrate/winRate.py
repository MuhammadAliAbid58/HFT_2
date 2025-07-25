#!/usr/bin/env python3
"""
Streamlined cTrader Deal Data Viewer
"""

import time
import logging
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

try:
    from twisted.internet import reactor
    from ctrader_open_api import Client as OpenApiClient, Protobuf, TcpProtocol
    from ctrader_open_api.messages.OpenApiMessages_pb2 import *
    from globals import APP_CLIENT_ID, APP_CLIENT_SECRET, ACCESS_TOKEN, HOST, PORT
except ImportError as e:
    print(f"Import error: {e}")
    sys.exit(1)


class DealDataViewer:
    """Simple deal data viewer for cTrader API"""
    
    def __init__(self, target_account_id=None):
        self.client = OpenApiClient(HOST, PORT, TcpProtocol)
        self.account_id = None
        self.target_account_id = target_account_id
        self.deals = []
        
        self.client.setConnectedCallback(self.on_connected)
        self.client.setDisconnectedCallback(lambda c, r=None: reactor.running and reactor.stop())
        self.client.setMessageReceivedCallback(self.on_message)
    
    def on_connected(self, client):
        print("Connected to cTrader API")
        auth_req = ProtoOAApplicationAuthReq()
        auth_req.clientId = APP_CLIENT_ID
        auth_req.clientSecret = APP_CLIENT_SECRET
        client.send(auth_req)
        print("Application auth request sent")
    
    def on_message(self, client, message):
        try:
            payload = Protobuf.extract(message)
            
            if isinstance(payload, ProtoOAErrorRes):
                print(f"Error: {payload.description}")
                reactor.running and reactor.stop()
                
            elif isinstance(payload, ProtoOAApplicationAuthRes):
                req = ProtoOAGetAccountListByAccessTokenReq()
                req.accessToken = ACCESS_TOKEN
                client.send(req)
                
            elif isinstance(payload, ProtoOAGetAccountListByAccessTokenRes):
                if not payload.ctidTraderAccount:
                    print("No accounts found!")
                    return reactor.running and reactor.stop()
                
                account = payload.ctidTraderAccount[0] if not self.target_account_id else next(
                    (acc for acc in payload.ctidTraderAccount if acc.ctidTraderAccountId == self.target_account_id), None)
                
                if not account:
                    print(f"Account {self.target_account_id} not found!")
                    return reactor.running and reactor.stop()
                
                self.account_id = account.ctidTraderAccountId
                auth_req = ProtoOAAccountAuthReq()
                auth_req.ctidTraderAccountId = self.account_id
                auth_req.accessToken = ACCESS_TOKEN
                client.send(auth_req)
                
            elif isinstance(payload, ProtoOAAccountAuthRes):
                print(f"Account authenticated: {self.account_id}")
                self.request_deals()
                
            elif isinstance(payload, ProtoOADealListRes):
                print(f"Received deals response")
                self.display_deals(payload.deal)
                reactor.running and reactor.stop()  # Only stop after processing deals
                
        except Exception as e:
            print(f"Error in on_message: {e}")
            reactor.running and reactor.stop()
    
    def request_deals(self):
        current_time = int(time.time() * 1000)
        req = ProtoOADealListReq()
        req.ctidTraderAccountId = self.account_id
        req.fromTimestamp = current_time - (2 * 24 * 60 * 60 * 1000)  # Last 48 hours
        req.toTimestamp = current_time
        req.maxRows = 1000
        self.client.send(req)
    
    def display_deals(self, deals):
        if not deals:
            return print("No deals found")
        
        symbols = {1: "EURUSD", 2: "GBPUSD", 4: "USDJPY", 5: "AUDUSD", 13: "GBPJPY"}
        
        # Filter deals with valid closing data (ignore empty closePositionDetail)
        valid_closing_deals = []
        ignored_deals = []
        
        for deal in deals:
            if hasattr(deal, 'closePositionDetail') and deal.closePositionDetail:
                close_detail = deal.closePositionDetail
                # Check if closePositionDetail has actual data (not all zeros)
                entry_price = getattr(close_detail, 'entryPrice', 0.0)
                gross_profit = getattr(close_detail, 'grossProfit', 0)
                
                if entry_price != 0.0 or gross_profit != 0:
                    valid_closing_deals.append(deal)
                else:
                    ignored_deals.append(deal)
            else:
                ignored_deals.append(deal)
        
        print(f"\n{'='*170}")
        print("VALID CLOSING DEALS WITH PROFIT/LOSS CALCULATION")
        print(f"{'='*170}")
        print(f"{'#':<4}{'Deal ID':<12}{'Symbol':<8}{'Side':<5}{'Volume':<10}{'Entry Price':<12}{'Exit Price':<12}{'Gross P&L':<12}{'Commission':<12}{'Net P&L':<12}{'Result':<8}{'Date/Time':<20}")
        print("-"*170)
        
        total_net_profit = 0
        winning_deals = 0
        losing_deals = 0
        
        for i, deal in enumerate(valid_closing_deals, 1):
            # Basic deal info
            symbol = symbols.get(getattr(deal, 'symbolId', 0), f"SYM{getattr(deal, 'symbolId', 0)}")
            side = 'BUY' if getattr(deal, 'tradeSide', 2) == 1 else 'SELL'
            volume = getattr(deal, 'volume', 0) / 100000000  # Convert from cents to lots
            exit_price = getattr(deal, 'executionPrice', 0.0)
            
            # Extract close position details
            close_detail = deal.closePositionDetail
            entry_price = getattr(close_detail, 'entryPrice', 0.0)
            gross_profit_raw = getattr(close_detail, 'grossProfit', 0)
            commission_raw = getattr(close_detail, 'commission', 0)  # Use commission from closePositionDetail
            money_digits = getattr(deal, 'moneyDigits', 2)
            
            # Calculate actual profit using moneyDigits
            gross_profit = gross_profit_raw / (10 ** money_digits)
            commission = commission_raw / (10 ** money_digits)
            net_profit = gross_profit + commission  # Commission is already negative
            
            # Determine win/loss
            result = "WIN" if net_profit > 0 else "LOSS"
            
            # Track statistics
            total_net_profit += net_profit
            if net_profit > 0:
                winning_deals += 1
            else:
                losing_deals += 1
            
            # Format timestamp
            timestamp = getattr(deal, 'executionTimestamp', 0) + (60 * 60 * 1000)
            date_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp/1000))
            
            print(f"{i:<4}{getattr(deal, 'dealId', 0):<12}{symbol:<8}{side:<5}{volume:<10.2f}{entry_price:<12.5f}{exit_price:<12.5f}{gross_profit:<12.2f}{commission:<12.2f}{net_profit:<12.2f}{result:<8}{date_time:<20}")
        
        print("="*170)
        
        # Calculate win rate and display statistics
        total_valid_deals = len(valid_closing_deals)
        if total_valid_deals > 0:
            win_rate = (winning_deals / total_valid_deals) * 100
            
            # Calculate average win and loss
            winning_profits = []
            losing_profits = []
            
            for deal in valid_closing_deals:
                close_detail = deal.closePositionDetail
                gross_profit_raw = getattr(close_detail, 'grossProfit', 0)
                commission_raw = getattr(close_detail, 'commission', 0)  # Use commission from closePositionDetail
                money_digits = getattr(deal, 'moneyDigits', 2)
                
                net_profit = (gross_profit_raw / (10 ** money_digits)) + (commission_raw / (10 ** money_digits))
                
                if net_profit > 0:
                    winning_profits.append(net_profit)
                else:
                    losing_profits.append(abs(net_profit))
            
            avg_win = sum(winning_profits) / len(winning_profits) if winning_profits else 0
            avg_loss = sum(losing_profits) / len(losing_profits) if losing_profits else 0
            
            print(f"\n{'='*60}")
            print("TRADING PERFORMANCE ANALYSIS")
            print(f"{'='*60}")
            print(f"Total Valid Closing Deals: {total_valid_deals}")
            print(f"Ignored Deals (empty/opening): {len(ignored_deals)}")
            print(f"Winning Deals: {winning_deals}")
            print(f"Losing Deals: {losing_deals}")
            print(f"Win Rate: {win_rate:.2f}%")
            print(f"Total Net Profit: {total_net_profit:.2f}")
            print(f"Average Win: {avg_win:.2f}")
            print(f"Average Loss: {avg_loss:.2f}")
            if losing_deals > 0 and avg_loss != 0:
                profit_factor = (avg_win * winning_deals) / (avg_loss * losing_deals)
                print(f"Profit Factor: {profit_factor:.2f}")
            print(f"{'='*60}")
        else:
            print("\nNo valid closing deals found for analysis.")
        
        # Show ignored deals summary
        print(f"\n{'='*100}")
        print("IGNORED DEALS SUMMARY")
        print(f"{'='*100}")
        print(f"Total ignored deals: {len(ignored_deals)}")
        
        empty_close_deals = 0
        opening_deals = 0
        
        for deal in ignored_deals:
            if hasattr(deal, 'closePositionDetail') and deal.closePositionDetail:
                # This is a deal with empty closePositionDetail
                empty_close_deals += 1
            else:
                # This is an opening deal
                opening_deals += 1
        
        print(f"- Empty closePositionDetail: {empty_close_deals}")
        print(f"- Opening deals: {opening_deals}")
        print("="*100)
        
        # Show all deals table (original format)
        print(f"\n{'='*140}")
        print("ALL RAW DEALS (OPENING + CLOSING)")
        print(f"{'='*140}")
        print(f"{'#':<4}{'Deal ID':<10}{'Symbol':<8}{'Side':<5}{'Volume':<10}{'Price':<12}{'Type':<12}{'Date/Time':<20}")
        print("-"*140)
        
        for i, deal in enumerate(deals, 1):
            symbol = symbols.get(getattr(deal, 'symbolId', 0), f"SYM{getattr(deal, 'symbolId', 0)}")
            side = 'BUY' if getattr(deal, 'tradeSide', 2) == 1 else 'SELL'
            volume = getattr(deal, 'volume', 0) / 100000000
            price = getattr(deal, 'executionPrice', 0.0)
            deal_type = "CLOSING" if hasattr(deal, 'closePositionDetail') and deal.closePositionDetail else "OPENING"
            timestamp = getattr(deal, 'executionTimestamp', 0) + (60 * 60 * 1000)
            date_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp/1000))
            
            print(f"{i:<4}{getattr(deal, 'dealId', 0):<10}{symbol:<8}{side:<5}{volume:<10.2f}{price:<12.5f}{deal_type:<12}{date_time:<20}")
        
        print("="*140)
        
        # Simple stats
        total = len(deals)
        buys = sum(1 for d in deals if getattr(d, 'tradeSide', 2) == 1)
        sells = total - buys
        
        print(f"\nStats: {total} total deals ({buys} buys, {sells} sells)")
        print(f"Valid closing deals with P&L: {len(valid_closing_deals)}")
        print(f"Ignored deals (empty/opening): {len(ignored_deals)}")
        
        # Symbol distribution
        symbol_counts = {}
        for deal in deals:
            symbol = symbols.get(getattr(deal, 'symbolId', 0), f"SYM{getattr(deal, 'symbolId', 0)}")
            symbol_counts[symbol] = symbol_counts.get(symbol, 0) + 1
        
        if symbol_counts:
            print("Symbol distribution:", ", ".join(f"{k}: {v}" for k, v in symbol_counts.items()))
        
        # Print completely raw data without any mapping (first 5 deals only)
        print(f"\n{'='*160}")
        print("SAMPLE RAW API DATA (FIRST 5 DEALS)")
        print(f"{'='*160}")
        print("This is exactly what the cTrader API returns:\n")
        
        for i, deal in enumerate(deals[:5], 1):
            print(f"Deal #{i}:")
            print(f"  dealId: {getattr(deal, 'dealId', 'NOT_SET')}")
            print(f"  symbolId: {getattr(deal, 'symbolId', 'NOT_SET')}")
            print(f"  tradeSide: {getattr(deal, 'tradeSide', 'NOT_SET')}")
            print(f"  volume: {getattr(deal, 'volume', 'NOT_SET')}")
            print(f"  executionPrice: {getattr(deal, 'executionPrice', 'NOT_SET')}")
            print(f"  executionTimestamp: {getattr(deal, 'executionTimestamp', 'NOT_SET')}")
            print(f"  commission: {getattr(deal, 'commission', 'NOT_SET')}")
            print(f"  moneyDigits: {getattr(deal, 'moneyDigits', 'NOT_SET')}")
            print(f"  dealStatus: {getattr(deal, 'dealStatus', 'NOT_SET')}")
            
            if hasattr(deal, 'closePositionDetail') and deal.closePositionDetail:
                close_detail = deal.closePositionDetail
                print(f"  closePositionDetail:")
                print(f"    entryPrice: {getattr(close_detail, 'entryPrice', 'NOT_SET')}")
                print(f"    grossProfit: {getattr(close_detail, 'grossProfit', 'NOT_SET')}")
                print(f"    commission: {getattr(close_detail, 'commission', 'NOT_SET')}")
                print(f"    balance: {getattr(close_detail, 'balance', 'NOT_SET')}")
                print(f"    moneyDigits: {getattr(close_detail, 'moneyDigits', 'NOT_SET')}")
            else:
                print(f"  closePositionDetail: NOT_SET (Opening deal)")
            
            print("-" * 80)
        
        print(f"\nNOTE: Profit calculations use actual cTrader data:")
        print(f"- grossProfit from closePositionDetail divided by 10^moneyDigits")
        print(f"- commission from closePositionDetail divided by 10^moneyDigits (total trade commission)") 
        print(f"- Net P&L = Gross Profit + Commission (commission is negative)")
        print(f"- Only closing deals with valid data are used for win rate calculation")
        print("="*160)
    
    def start(self):
        print("Starting cTrader Deal Data Viewer...")
        print(f"Connecting to {HOST}:{PORT}")
        try:
            self.client.startService()
            print("Client service started, waiting for connection...")
            reactor.callLater(30, lambda: reactor.running and reactor.stop())
            reactor.run()
            print("Reactor finished")
        except Exception as e:
            print(f"Error in start: {e}")
            import traceback
            traceback.print_exc()


def main():
    logging.basicConfig(level=logging.WARNING)
    target_id = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else None
    DealDataViewer(target_id).start()


if __name__ == "__main__":
    main()