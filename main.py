from db_layer import *
import pandas
import matplotlib
import tensorflow as tf


def days_to_samples(n):
    return int(n * 12 * 24)


def hours_to_samples(n):
    return int(n * 12)

past_amount = hours_to_samples(12)
future_amount = hours_to_samples(6)

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
