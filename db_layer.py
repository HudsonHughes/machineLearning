from binance.client import Client
from launch_date_finder import *
import os
import json
from datetime import datetime
import time
import requests
import psycopg2
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import Column, Integer, String, DateTime, Float
from datetime import datetime, timedelta
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import func
from sqlalchemy import or_
import sys, traceback
import pandas
import numpy as np
from pathlib import Path
from cachier import cachier
dbname = "coins"
key = os.environ['API_KEY']
secret = os.environ['API_SECRET']
Base = automap_base()
engine = create_engine('postgresql://postgres:1hudson1@localhost:5432/' + dbname, echo=False)
connection = engine.connect()
Base.prepare(engine, reflect=True)
Price = Base.classes.price
Pump = Base.classes.pump
Name = Base.classes.name
session = Session(engine)
client = Client(key, secret)
symbols = ['xrp', 'eth', 'bcc', 'eos', 'xlm', 'ltc', 'bnb', 'ada', 'data', 'nano', 'trx', 'bcpt', 'neo', 'iota', 'mth',
           'cmt', 'ont', 'enj', 'pivx', 'gas', 'zrx', 'arn', 'qkc', 'tusd', 'etc', 'icx', 'hot', 'vet', 'dash', 'xmr',
           'mtl', 'qtum', 'xvg', 'nuls', 'agi', 'wpr', 'poa', 'fuel', 'omg', 'zec', 'eng', 'iost', 'gto', 'cvc', 'evx',
           'loom', 'zil', 'cdt', 'dock', 'ncash', 'storm', 'elf', 'xem', 'lun', 'gvt', 'theta', 'wabi', 'req', 'vibe',
           'key', 'wtc', 'ardr', 'sc', 'rcn', 'wan', 'poe', 'adx', 'bqx', 'ost', 'vib', 'tnb', 'dnt', 'rep', 'npxs',
           'bcn', 'bcd', 'btg', 'trig', 'bat', 'storj', 'gnt', 'wings', 'chat', 'nas', 'ins', 'appc', 'snm', 'kmd',
           'aion', 'tnt', 'xzc', 'mft', 'nxs', 'go', 'dent', 'phx', 'strat', 'ae', 'lsk', 'lend', 'amb', 'icn', 'waves',
           'gxs', 'mana', 'dlt', 'iotx', 'mco', 'ppt', 'mod', 'snt', 'cnd', 'salt', 'nebl', 'brd', 'zen', 'link',
           'yoyo', 'fun', 'sub', 'nav', 'lrc', 'oax', 'via', 'rdn', 'powr', 'ast', 'mda', 'bts', 'sngls', 'steem',
           'knc', 'sky', 'bnt', 'blz', 'qlc', 'poly', 'ark', 'rlc', 'qsp', 'dgd', 'edo', 'hc', 'cloak', 'sys', 'grs']
# symbols = list(set([s['baseAsset'] for s in client.get_exchange_info()['symbols']]))
dbname = 'coins'

def find(f, seq):
    """Return first item in sequence where f(item) == True."""
    for item in seq:
        if f(item):
            return item


# @cachier(stale_after=timedelta(days=7))
def add_coin_names():
    start_map = coin_start_map()
    print("Filling coin name table")
    listings = requests.get(url="https://api.coinmarketcap.com/v2/listings/").json()["data"]
    missing = []
    for sym in symbols:
        point = find(lambda spot: spot["symbol"] == sym.upper(), listings)
        if point == None:
            missing.append(sym)
        else:
            print(point["symbol"], point["name"], point["website_slug"], datetime.fromtimestamp(start_map[sym.lower()] / 1000))
            session.add(Name(name=point["name"].lower(), symbol=sym.lower(), cmc=point["website_slug"].lower(), start_date=datetime.fromtimestamp(start_map[sym.lower()] / 1000)))
    session.add(Name(name="bitcbnnect", symbol="bcc", cmc="bitconnect", start_date=datetime.fromtimestamp(start_map["bcc"] / 1000)))
    session.add(Name(name="iota", symbol="iota", cmc="iota", start_date=datetime.fromtimestamp(start_map["iota"] / 1000)))
    session.add(Name(name="ethos", symbol="bqx", cmc="ethos", start_date=datetime.fromtimestamp(start_map["bqx"] / 1000)))
    session.add(Name(name="yoyow", symbol="yoyo", cmc="yoyow", start_date=datetime.fromtimestamp(start_map["yoyo"] / 1000)))
    print(missing, len(missing))
    try:
        session.commit()
    except Exception:
        session.rollback()
    return True


timeshift = 6 * 1000 * 60 * 60


# @cachier(stale_after=timedelta(days=3))
def get_slug(coin):
    return session.query(Name).filter(or_(Name.name==coin, Name.symbol==coin, Name.cmc==coin)).one().cmc


# @cachier(stale_after=timedelta(days=3))
def get_name(coin):
    return session.query(Name).filter(or_(Name.name==coin, Name.symbol==coin, Name.cmc==coin)).one().name


# @cachier(stale_after=timedelta(days=3))
def get_symbol(coin):
    return session.query(Name).filter(or_(Name.name==coin, Name.symbol==coin, Name.cmc==coin)).one().symbol

def RateLimited(maxPerSecond):
    minInterval = 1.0 / float(maxPerSecond)
    def decorate(func):
        lastTimeCalled = [0.0]
        def rateLimitedFunction(*args,**kargs):
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait>0:
                time.sleep(leftToWait)
            ret = func(*args,**kargs)
            lastTimeCalled[0] = time.clock()
            return ret
        return rateLimitedFunction
    return decorate

@RateLimited(0.5)
def cmc_request(url):
    return requests.get(url=url)

def generate_price_from_cmc_as_file(coin):
    coin = get_slug(coin)
    file = open("temp.csv", "w")
    print("Scrapping " + coin)
    start_time = 0
    if session.query(Price).filter(Price.name==coin).first():
        start_time = int(int(session.query(Price).filter(Price.name == coin).order_by(Price.id.desc()).first().time.timestamp()) * 1000 + 2 * 1000 * 60)
        print("Coin already here at " + datetime.utcfromtimestamp(start_time / 1000).strftime('%m/%d/%Y %X'))
    else:
        start_time = int(session.query(Name).filter(Name.name == get_name(coin)).first().start_date.timestamp() * 1000 + 604800000)
        if start_time > int(round(time.time() * 1000)):
            print("This coin is too new: " + datetime.utcfromtimestamp(start_time / 1000).strftime('%m/%d/%Y'))
            return
        print("Calculating coin from " + datetime.utcfromtimestamp(start_time / 1000).strftime('%m/%d/%Y'))
    tick = 1
    count = 0
    start = time.time()
    last_time = 0
    first_packet = True
    while start_time < time.time() * 1000:
        try:
            resp = cmc_request(url="https://graphs2.coinmarketcap.com/currencies/" + coin + "/" + str(start_time) + "/" + str(start_time + 86400000) + "/").json()
            for packet in zip(resp['price_usd'], resp['price_btc'], resp['volume_usd'], resp['market_cap_by_available_supply']):
                if last_time < packet[0][0] / 1000:
                    if first_packet:
                        print("First packet at " + str(datetime.utcfromtimestamp(packet[0][0] / 1000).strftime("%Y-%m-%d %X")))
                        first_packet = False
                    file.write(coin + "," + datetime.utcfromtimestamp(packet[0][0] / 1000).strftime("%Y-%m-%d %X") + "," + str(packet[0][1]) + "," + str(packet[1][1]) + "," + str(packet[2][1]) + "," + str(packet[3][1]) + "\n")
                last_time = packet[0][0] / 1000
            start_time = start_time + 86400000
            count += 1
            tick = tick - 1
            if tick == 0:
                tick = 30
                print("Scrapping " + datetime.utcfromtimestamp(start_time / 1000).strftime('%m/%d/%Y'))
        except Exception:
            print(count)
            end = time.time()
            print("Elapsed time was %g seconds" % (end - start))
            print("Problem at " + "https://graphs2.coinmarketcap.com/currencies/" + coin + "/" + str(start_time) + "/" + str(start_time + 86400000) + "/")
            print(str(requests.get(url="https://graphs2.coinmarketcap.com/currencies/" + coin + "/" + str(start_time) + "/" + str(start_time + 86400000) + "/")))
            traceback.print_exc(file=sys.stdout)
            exit(0)
    return file


def fill_price_table():
    print("Filling price table")
    first = False
    for symbol in symbols:
        start_time = time.time()
        slug = get_slug(symbol)
        print("retrieving " + slug)
        if not first:
            f = generate_price_from_cmc_as_file(slug)
            f.close()
        else:
            f = open("temp.csv", "r")
            f.close
        first = False
        working_directory = os.getcwd() + "\\"
        trans = connection.begin()
        connection.execute(text("copy price (name, time, btc, usd, vol, cap) from '" + working_directory + f.name +"' with (FORMAT csv);"))
        trans.commit()
        end_time = time.time()
        print("Elapsed time was %g seconds" % (end_time - start_time))


def get_prices_as_numpy(coin):
    None


cache_path = "C:\\Users\\" + os.getlogin() + "\\coin-cache-for-hudson-at-work\\"


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def get_prices_as_pandas(coin):
    # my_file = Path(cache_path + get_name(coin))
    # if my_file.is_file():
    #     return pandas.read_csv(cache_path + get_name(coin))
    # df = pandas.read_sql_query("SELECT time, usd, btc, vol, cap FROM price WHERE name = '" + get_name(coin) + "'", engine, parse_dates=["time"])
    # if not Path(cache_path).is_dir():
    #     Path(cache_path).mkdir()
    # df.to_csv(cache_path + get_name(coin), sep='\t')
    # return df
    # print("SELECT time, usd, btc, vol, cap FROM price WHERE name = '" + get_name(coin).lower() +
    #                              "'")
    return pandas.read_sql_query("SELECT time, usd, btc, vol, cap FROM price WHERE name = '" + get_slug(coin).lower() +
                                 "'", engine, parse_dates=["time"], index_col="time")

def get_prices_as_pandas_no_index(coin):
    # my_file = Path(cache_path + get_name(coin))
    # if my_file.is_file():
    #     return pandas.read_csv(cache_path + get_name(coin))
    # df = pandas.read_sql_query("SELECT time, usd, btc, vol, cap FROM price WHERE name = '" + get_name(coin) + "'", engine, parse_dates=["time"])
    # if not Path(cache_path).is_dir():
    #     Path(cache_path).mkdir()
    # df.to_csv(cache_path + get_name(coin), sep='\t')
    # return df
    # print("SELECT time, usd, btc, vol, cap FROM price WHERE name = '" + get_name(coin).lower() +
    #                              "'")
    return pandas.read_sql_query("SELECT time, usd, btc, vol, cap FROM price WHERE name = '" + get_slug(coin).lower() +
                                 "'", engine, parse_dates=["time"])
