import json
from ctrader_open_api import EndPoints

# Open API Configuration
APP_CLIENT_ID = "14154_kdUcmqKwgpIjkbSYljZn9ORIiHy9n0zHzyg7P3GvaEuPKLWmIv"
APP_CLIENT_SECRET = "iSj0GHcETjIsrHna91Pwe4r1L2k6Dt0ocq7r2u32muS6iEshSD"
ACCESS_TOKEN = "f2cG74o0Qh4Ty17zs6W0WaUXHSu5Ozg8Sf_OL9xXrwY"
REFRESH_TOKEN = "CSlI4r97y_LSL7lFJ_lBCH0MJ8fTlbfI2qbNUoqUMtU"
HOST = EndPoints.PROTOBUF_LIVE_HOST
PORT = EndPoints.PROTOBUF_PORT
SYMBOL_ID = 1
PRICE_SCALE_FACTOR = 100000.0
VOLUME_CONVERSION_FACTOR = 100.0

# FIX Configuration
with open("config-trade.json") as f:
    CONFIG_TRADE = json.load(f)
with open("config-quote.json") as f:
    CONFIG_QUOTE = json.load(f) 