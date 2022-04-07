import os
from pathlib import Path
import ccxt
import sys
import csv

market = 'future'
timeframe = '1d'

# press run button to start


# -----------------------------------------------------------------------------

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(''))))
sys.path.append(root + '/python')


# -----------------------------------------------------------------------------

# def fetchTickers(market) -> dict:
#         exchange = ccxt.binance({
#            'timeout': 10000,
#            'enableRateLimit': True,
#            'options': {
#                'defaultType': market,}
#        })
#         exchange.loadMarkets()

#         # Data container
#         ticker_list = []

#         #Fetch OHLCV data
#         ticker_list =  exchange.fetchTickers()

#         for key in list(filter(lambda x: '_' in x, ticker_list.keys())): del ticker_list[key]

#         # Return OHLCV data
#         return ticker_list


def retry_fetch_ohlcv(exchange, max_retries, symbol, timeframe, since, limit):
    num_retries = 0
    try:
        num_retries += 1
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since, limit)
        # print('Fetched', len(ohlcv), symbol, 'candles from', exchange.iso8601 (ohlcv[0][0]), 'to', exchange.iso8601 (ohlcv[-1][0]))
        return ohlcv
    except Exception:
        if num_retries > max_retries:
            raise  # Exception('Failed to fetch', timeframe, symbol, 'OHLCV in', max_retries, 'attempts')


def scrape_ohlcv(exchange, max_retries, symbol, timeframe, since, limit):
    earliest_timestamp = exchange.milliseconds()
    timeframe_duration_in_seconds = exchange.parse_timeframe(timeframe)
    timeframe_duration_in_ms = timeframe_duration_in_seconds * 1000
    timedelta = limit * timeframe_duration_in_ms
    all_ohlcv = []
    while True:
        fetch_since = earliest_timestamp - timedelta
        ohlcv = retry_fetch_ohlcv(exchange, max_retries, symbol, timeframe, fetch_since, limit)
        # if we have reached the beginning of history
        if ohlcv[0][0] >= earliest_timestamp:
            break
        earliest_timestamp = ohlcv[0][0]
        all_ohlcv = ohlcv + all_ohlcv
        print(len(all_ohlcv), symbol, 'candles in total from', exchange.iso8601(all_ohlcv[0][0]), 'to', exchange.iso8601(all_ohlcv[-1][0]))
        # if we have reached the checkpoint
        if fetch_since < since:
            break
    return all_ohlcv


def write_to_csv(filename, data):
    p = Path("./data/" + market + '/' + timeframe )
    p.mkdir(parents=True, exist_ok=True)
    full_path = p / str(filename + '.csv')
    with Path(full_path).open('w+', newline='') as output_file:
        csv_writer = csv.writer(output_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerows(data)

# will need to increase limit if ever want to look at timeframe under 1h

def scrape_candles_to_csv(max_retries, timeframe, since, market, limit=100000):
    # instantiate the exchange by id
    exchange = ccxt.binance({
        'enableRateLimit': True,
        'options': {
        'defaultType': market,  }
    })
    # convert since from string to milliseconds integer if needed
    if isinstance(since, str):
        since = exchange.parse8601(since)
    # preload all markets from the exchange
    exchange.load_markets()
    ticker_list =  exchange.fetchTickers()
    # delete some funny tickers for future expiration
    for key in list(filter(lambda x: '_' in x, ticker_list.keys())): del ticker_list[key]
    # delete any key that doesn't have USDT
    for key in list(filter(lambda x: 'USDT' not in x, ticker_list.keys())): del ticker_list[key]
    # fetch all candles
    for key in ticker_list.keys():
        ohlcv = scrape_ohlcv(exchange, max_retries, key, timeframe, since, limit)
        # save them to csv file
        key = key.replace('/', '_')
        # add column names
        ohlcv.insert(0, ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
        write_to_csv(key, ohlcv)
        print('Saved', len(ohlcv), 'candles from', exchange.iso8601(ohlcv[0][0]), 'to', exchange.iso8601(ohlcv[-1][0]), 'to', key+'.csv')

    print(len(ticker_list), 'tickers saved') 
# -----------------------------------------------------------------------------

if __name__ == '__main__':

    # My first __name__ == '__main__' code.  31 March 2022.

    scrape_candles_to_csv(max_retries=5, timeframe=timeframe, since='2017-01-0100:00:00Z', market=market)