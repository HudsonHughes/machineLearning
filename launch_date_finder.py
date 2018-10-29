'''
Binance First Candle Finders
Creslin

Get list of all IDs on binance
Returns the first candle / launch timestamp to the minute for each
'''
import urllib.request
import json
import ccxt
from cachier import cachier
from datetime import datetime, timedelta

def all_ids():
    # load all markets from binance into a list
    id = 'binance'
    exchange_found = id in ccxt.exchanges
    if exchange_found:
        exchange = getattr(ccxt, id)({})
        markets = exchange.load_markets()
        tuples = list(ccxt.Exchange.keysort(markets).items())

        ids = []
        for (k, v) in tuples:
            ids.append(v['id'])

        return ids

def give_first_kline_open_stamp(interval, symbol, start_ts=1499990400000):
        '''
        Returns the first kline from an interval and start timestamp and symbol
        :param interval:  1w, 1d, 1m etc - the bar length to query
        :param symbol:    BTCUSDT or LTCBTC etc
        :param start_ts:  Timestamp in miliseconds to start the query from
        :return:          The first open candle timestamp
        '''

        url_stub = "http://api.binance.com/api/v1/klines?interval="

        #/api/v1/klines?interval=1m&startTime=1536349500000&symbol=ETCBNB
        addInterval   = url_stub     + str(interval) + "&"
        addStarttime  = addInterval   + "startTime="  + str(start_ts) + "&"
        addSymbol     = addStarttime + "symbol="     + str(symbol)
        url_to_get = addSymbol

        kline_data = urllib.request.urlopen(url_to_get).read().decode("utf-8")
        kline_data = json.loads(kline_data)

        return kline_data[0][0]

@cachier(stale_after=timedelta(days=3))
def coin_start_map():
    ids = all_ids()
    result = dict()
    for this_id in ids:
        if "BTC" not in this_id:
            continue
        '''
        Find launch Week of symbol, start at Binance launch date 2017-07-14 (1499990400000)
        Find launch Day of symbol in week
        Find launch minute of symbol in day
        '''

        symbol_launch_week_stamp = give_first_kline_open_stamp('1w', this_id, 1499990400000)
        symbol_launch_day_stamp = give_first_kline_open_stamp('1d', this_id, symbol_launch_week_stamp)
        symbol_launch_minute_stamp = give_first_kline_open_stamp('1m', this_id, symbol_launch_day_stamp)
        if "BTC" in this_id:
            result[this_id.replace("BTC", "").lower()] = symbol_launch_week_stamp
            print(this_id + " processed")
    return result
