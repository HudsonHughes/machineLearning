from db_layer import *
import launch_date_finder
import pandas as pd
import matplotlib
# import tensorflow as tf
from ratelimit import limits

add_coin_names()
fill_price_table()
exit(0)

def days_to_samples(n):
    return int(n * 12 * 24)


def hours_to_samples(n):
    return int(n * 12)


past_amount = hours_to_samples(12)
future_amount = hours_to_samples(6)


@limits(calls=60, period=60)
def binance_trade_call(coin, time):
    print(time, int(time))
    return client.get_aggregate_trades(symbol=get_symbol(coin).upper() + 'BTC', startTime=time, endTime=(time + 1000 * 3600))


from datetime import timedelta, date

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

start_date = date(2018, 9, 29)
end_date = date(2018, 10, 27)
for single_date in daterange(start_date, end_date):
    print(get_slug("MDA"))
    a = binance_trade_call("MDA", int(time.mktime(single_date.timetuple()) * 1000))
    if len(a) > 0:
        print(a)
        print( time.mktime(single_date.timetuple()) * 1000 + 1000)
        print(single_date)


def get_trades_from_pump(pump):
    start_time = pump[2].timestamp() * 1000
    print(binance_trade_call(pump[-1], int(start_time)))


def get_pumps(coin, threshold, time_limit):
    time_series = get_prices_as_pandas_no_index(coin)
    time_series_val = time_series.values
    print(time_series_val)
    result = []
    last_peak_time = time_series_val[0][0]
    for index, row in enumerate(time_series_val):
        current_time = row[0]
        end_time = row[0] + pd.Timedelta(minutes=time_limit)
        index += 1
        if index > len(time_series_val):
            break
        peak_price = 0
        peak_index = None
        while current_time <= end_time and index < len(time_series_val) - 1:
            if time_series_val[index][1] > peak_price:
                peak_price, peak_index = time_series_val[index][1], index
            index += 1
            current_time = time_series_val[index][0]
        if peak_price / row[1] > threshold:
            if last_peak_time < row[0]:
                result.append((row[1], peak_price, row[0], time_series_val[peak_index][0], coin))
            else:
                result[-1] = (row[1], peak_price, row[0], time_series_val[peak_index][0], coin)
    return result


pumps = get_pumps("MDA", 1.07, 300)

for i in pumps:
    print(i)
    get_trades_from_pump(i)

exit(0)
for n, i in enumerate(symbols):
    arr = get_prices_as_pandas(i)
    arr = arr
    total_length = arr.size
    start_idx = 0
    while True:
        past_points = arr[start_idx: start_idx + past_amount]
        future_points = arr[start_idx + past_amount: min(start_idx + past_amount + future_amount, total_length - 1)]
        if future_points.empty:
            break
        past_price = past_points["btc"][-1]
        future_price = future_points["btc"].mean()
        if future_price / past_price > 1.02:
            print("point " + str(past_price), future_price, future_price / past_price)


        start_idx += past_amount
    # chunks = chunker(arr, hours_to_samples(6))
    # while True:
    #     try:
    #         print(next(chunks))
    #     except StopIteration:
    #         break
    break
