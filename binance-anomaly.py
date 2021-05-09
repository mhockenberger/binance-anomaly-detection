import argparse

import logging
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
    '--symbol',
    '-s',
    nargs='?',
    const=1,
    type=str,
    default='ADAUSDT')
parser.add_argument(
    '--threshold',
    '-t',
    nargs='?',
    const=1,
    type=float,
    default=18)

args = parser.parse_args()

if args.symbol:
    symbol = args.symbol.upper()
    threshold = args.threshold

    api_key = os.environ.get('binance_api')
    api_secret = os.environ.get('binance_secret')

    client = Client(api_key, api_secret)

    binance_manager = BinanceWebSocketApiManager(
        exchange='binance.com', output_default='dict')

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

    binance_manager.create_stream('kline_1h', symbol)

    result = PrettyTable()
    result.field_names = [
        'datetime',
        'final_bar',
        'open_bar1',
        'lowest_low',
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

                    open_bar1 = float(
                        client.get_historical_klines(
                            args.symbol,
                            Client.KLINE_INTERVAL_1HOUR,
                            '2 hour ago UTC')[0][1])
                    df['pct_change_low'] = (
                        ((float(df['low']) - open_bar1) / open_bar1) * 100)

                    if df['pct_change_low'] < lowest_low:
                        lowest_low = df['pct_change_low']
                        anomaly = np.where(lowest_low <= -threshold,
                                           True, False)

                        df['pct_change_lowest_low'] = lowest_low

                        result.add_row([df.name,
                                        df['final_bar'],
                                        round(open_bar1, 4),
                                        round(lowest_low, 4),
                                        round(df['pct_change_lowest_low'], 4),
                                        anomaly])
                        print(result)

                    if df['final_bar']:
                        lowest_low = 0
else:
    print('Missing symbol.')
