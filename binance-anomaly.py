import argparse

import logging
import threading
import time
import os
import numpy as np
import pandas as pd
from prettytable import PrettyTable

from binance.client import Client
from unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager import BinanceWebSocketApiManager


logging.basicConfig(
    level=logging.ERROR,
    filename=os.path.basename(__file__) +
    '.log',
    format='{asctime} [{levelname:8}] {process} {thread} {module}: {message}',
    style='{')

parser = argparse.ArgumentParser(description='Binance Anomalies')
parser.add_argument(
    '--symbols',
    '-s',
    nargs='?',
    const=1,
    type=list,
    default=['ADAUSDT', 'DOGEUSDT'])
parser.add_argument(
    '--thresholds',
    '-t',
    nargs='?',
    const=1,
    type=list,
    default=[18, 18])

args = parser.parse_args()


def get_anomaly(client, symbol, threshold):
    binance_manager = BinanceWebSocketApiManager(
        exchange='binance.com', output_default='dict')
    binance_manager.create_stream('kline_1h', symbol)

    cols = {
        't': 'start_time',
        'T': 'end_time',
        's': 'symbol',
        'i': 'interval',
        'f': 'first_trade_id',
        'L': 'last_trade_id',
        'o': 'open',
        'c': 'close',
        'h': 'high',
        'l': 'low',
        'v': 'volume',
        'n': 'number_of_trades',
        'x': 'final_bar',
        'q': 'quote_volume',
        'V': 'volume_of_active_buy',
        'Q': 'quote_volume_of_active_buy'
    }

    result = PrettyTable()
    result.field_names = [
        'symbol',
        'datetime',
        'final_bar',
        'open_5',
        'low',
        'pct_change_lowest_low',
        'anomaly']

    lowest_low = 0

    while True:
        if binance_manager.is_manager_stopping():
            exit(0)
        stream_data = binance_manager.pop_stream_data_from_stream_buffer()
        if stream_data is False:
            time.sleep(0.01)
        else:
            if stream_data is not None:
                if 'data' in stream_data.keys():
                    df = pd.DataFrame([stream_data['data']['k']])
                    df['datetime'] = pd.Timestamp('today')

                    df.rename(columns=cols, inplace=True)
                    df.set_index('datetime', drop=True, inplace=True)
                    df = df.iloc[0]

                    open_5 = float(
                        client.get_historical_klines(
                            symbol,
                            Client.KLINE_INTERVAL_1HOUR,
                            '6 hour ago UTC')[0][1])
                    df['pct_change_low'] = (
                        ((float(df['low']) - open_5) / open_5) * 100)

                    if df['pct_change_low'] < lowest_low:
                        lowest_low = df['pct_change_low']
                        anomaly = (lowest_low <= -threshold)

                        df['pct_change_lowest_low'] = lowest_low

                        result.add_row([symbol,
                                        df.name,
                                        df['final_bar'],
                                        round(open_5, 4),
                                        round(float(df['low']), 4),
                                        round(df['pct_change_lowest_low'], 4),
                                        anomaly])
                        print(result)

                    if df['final_bar']:
                        lowest_low = 0


if args.symbols:
    api_key = os.environ.get('binance_api')
    api_secret = os.environ.get('binance_secret')

    client = Client(api_key, api_secret)

    threads = list()
    for symbol in args.symbols:
        thread = threading.Thread(target=get_anomaly, args=(
            client,
            symbol,
            dict(zip(args.symbols, args.thresholds))[symbol]))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
else:
    print('Missing symbols.')
