from binance.client import Client
import os
import json
from datetime import datetime
import time
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import Column, Integer, String, DateTime, Float
from datetime import datetime, timedelta
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import func
from sqlalchemy import or_
import pandas
import numpy as np
from pathlib import Path
from cachier import cachier
dbname = "coins"
key = os.environ['API_KEY']
secret = os.environ['API_SECRET']
Base = automap_base()
engine = create_engine('postgresql://postgres:1hudson1@localhost:5432/' + dbname, echo=False)
Base.prepare(engine, reflect=True)
Price = Base.classes.price
Pump = Base.classes.pump
Name = Base.classes.name
session = Session(engine)
client = Client(key, secret)
symbols = ['XRP', 'ETH', 'BCC', 'EOS', 'XLM', 'LTC', 'BNB', 'ADA', 'DATA', 'NANO', 'TRX', 'BCPT', 'NEO', 'IOTA', 'MTH',
           'CMT', 'ONT', 'ENJ', 'PIVX', 'GAS', 'ZRX', 'ARN', 'QKC', 'TUSD', 'ETC', 'ICX', 'HOT', 'VET', 'DASH', 'XMR',
           'MTL', 'QTUM', 'XVG', 'NULS', 'AGI', 'WPR', 'POA', 'FUEL', 'OMG', 'ZEC', 'ENG', 'IOST', 'GTO', 'CVC', 'EVX',
           'LOOM', 'ZIL', 'CDT', 'DOCK', 'NCASH', 'STORM', 'ELF', 'XEM', 'LUN', 'GVT', 'THETA', 'WABI', 'REQ', 'VIBE',
           'KEY', 'WTC', 'ARDR', 'SC', 'RCN', 'WAN', 'POE', 'ADX', 'BQX', 'OST', 'VIB', 'TNB', 'DNT', 'REP', 'NPXS',
           'BCN', 'BCD', 'BTG', 'TRIG', 'BAT', 'STORJ', 'GNT', 'WINGS', 'CHAT', 'NAS', 'INS', 'APPC', 'SNM', 'KMD',
           'AION', 'TNT', 'XZC', 'MFT', 'NXS', 'GO', 'DENT', 'PHX', 'STRAT', 'AE', 'LSK', 'LEND', 'AMB', 'ICN', 'WAVES',
           'GXS', 'MANA', 'DLT', 'IOTX', 'MCO', 'PPT', 'MOD', 'SNT', 'CND', 'SALT', 'NEBL', 'BRD', 'ZEN', 'LINK',
           'YOYO', 'FUN', 'SUB', 'NAV', 'LRC', 'OAX', 'VIA', 'RDN', 'POWR', 'AST', 'MDA', 'BTS', 'SNGLS', 'STEEM',
           'KNC', 'SKY', 'BNT', 'BLZ', 'QLC', 'POLY', 'ARK', 'RLC', 'QSP', 'DGD', 'EDO', 'HC', 'CLOAK', 'SYS', 'GRS']
# symbols = list(set([s['baseAsset'] for s in client.get_exchange_info()['symbols']]))
dbname = 'coins'

def find(f, seq):
    """Return first item in sequence where f(item) == True."""
    for item in seq:
        if f(item):
            return item


@cachier(stale_after=timedelta(days=7))
def add_coin_names():
    print("Filling coin name table")
    listings = requests.get(url="https://api.coinmarketcap.com/v2/listings/").json()["data"]
    missing = []
    for sym in symbols:
        point = find(lambda spot: spot["symbol"] == sym, listings)
        if point == None:
            missing.append(sym)
        else:
            print(point["symbol"], point["name"], point["website_slug"])
            session.add(Name(name=point["name"], symbol=sym, cmc=point["website_slug"]))
    session.add(Name(name="BitConnect", symbol="BCC", cmc="bitconnect"))
    session.add(Name(name="Iota", symbol="IOTA", cmc="iota"))
    session.add(Name(name="Ethos", symbol="BQX", cmc="ethos"))
    session.add(Name(name="Yoyow", symbol="YOYO", cmc="yoyow"))
    print(missing, len(missing))
    session.commit()
    return True


timeshift = 6 * 1000 * 60 * 60


def generate_price_from_cmc(coin):
    print("Scrapping " + coin)
    start_time = 0
    if session.query(Price).filter(Price.name==coin).first():
        start_time = int(session.query(Price).filter(Price.name == coin).order_by(Price.id.desc()).first().time.timestamp()) * 1000
        print("Coin already here at " + datetime.utcfromtimestamp(start_time / 1000).strftime('%m/%d/%Y'))

    else:
        start_time = max(1483254000000, requests.get(url="https://graphs2.coinmarketcap.com/currencies/" + coin).json()["price_btc"][0][0])
    tick = 1
    while start_time < time.time() * 1000:
        try:
            resp = requests.get(url="https://graphs2.coinmarketcap.com/currencies/" + coin + "/" + str(start_time) + "/" + str(start_time + 86400000) + "/").json()
            days_packets = []
            for packet in zip(resp['price_usd'], resp['price_btc'], resp['volume_usd'], resp['market_cap_by_available_supply']):
                days_packets.append((datetime.utcfromtimestamp(packet[0][0] / 1000), packet[0][1], packet[1][1], packet[2][1], packet[3][1]))
            yield days_packets
            start_time = start_time + 86400000
            tick = tick - 1
            if tick == 0:
                tick = 30
                print("Scrapping " + datetime.utcfromtimestamp(start_time / 1000).strftime('%m/%d/%Y'))
        except Exception:
            print("Fuck up at " + "https://graphs2.coinmarketcap.com/currencies/" + coin + "/" + str(start_time) + "/" + str(start_time + 86400000) + "/")


# @cachier(stale_after=timedelta(days=3))
def get_slug(coin):
    return session.query(Name).filter(or_(Name.name==coin, Name.symbol==coin, Name.cmc==coin)).one().cmc


# @cachier(stale_after=timedelta(days=3))
def get_name(coin):
    return session.query(Name).filter(or_(Name.name==coin, Name.symbol==coin, Name.cmc==coin)).one().name


# @cachier(stale_after=timedelta(days=3))
def get_symbol(coin):
    return session.query(Name).filter(or_(Name.name==coin, Name.symbol==coin, Name.cmc==coin)).one().symbol


def fill_price_table():
    print("Filling price table")
    for symbol in symbols:
        slug = get_slug(symbol)
        print("slug " + slug)
        first = True
        for days_packet in generate_price_from_cmc(slug):
            if first:
                for packet in days_packet:
                    session.add(
                        Price(name=slug, time=packet[0], usd=packet[1], btc=packet[2], vol=packet[3], cap=packet[4]))
            else:
                try:
                    session.bulk_save_objects(
                        [Price(name=slug, time=packet[0], usd=packet[1], btc=packet[2], vol=packet[3], cap=packet[4])
                         for packet in days_packet])
                    session.commit()
                except IntegrityError:
                    session.rollback()
            first = False


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
