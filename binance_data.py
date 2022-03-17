import ccxt
import pandas as pd

timeframe = '1d'    # 1d 1h 5m etc
since = 10         # how many days do you want data for 
# Depending on your operating system and setup, this may take some trial and error.
# With nothing below it outputs in the current directory.
# Include a / at the end if you are naming the folder to use.
filepath = f'data{timeframe}/'
exchange_type = 'future'


def get_tickers(exchange_type: str):
    exchange = ccxt.binance({
    'timeout': 10000,
    'enableRateLimit': True,
    'rateLimit': 250,            # don't lower this... you can get IP banned.
    'options': {
        'defaultType': exchange_type,
    }})
    
    tickers = list()
    for ticker in exchange.fetch_tickers():
        tickers.append(ticker) if ticker.endswith('USDT') else None

    return tickers

tickers = get_tickers(exchange_type)

# remove all UP/DOWN BEAR/BULL 
remove_strings = ['UP', 'DOWN', 'BEAR', 'BULL']
for s in remove_strings:
    tickers = [t for t in tickers if s not in t]
# tickers.append('SUPER/USDT') # add this to spot list, not in futures


# exchange = ccxt.binance({
#     'timeout': 10000,
#     'enableRateLimit': True,
#     'rateLimit': 250,            # don't lower this... you can get IP banned.
#     'options': {
#         'defaultType': exchange_type,
#     }})


    # Fetch OHLCV data 
    #
    #  @staticmethod
def fetchOHLCVData(exchange_type, symbol: str, timeframe: str, days: int = 30,) -> list:
        exchange = ccxt.binance({
           'timeout': 10000,
           'enableRateLimit': True,
           'rateLimit': 250,            # don't lower this... you can get IP banned.
           'options': {
               'defaultType': exchange_type,
            
           }
       })
        exchange.loadMarkets()

        # Get all OHLCV data since
        since = exchange.milliseconds() - days * 24 * 60 * 60 * 1000

        # Data container
        ohlcv_data = []

        # Paginate through data
        while since < exchange.milliseconds():

            #Fetch OHLCV data
            try:
                ohlcv =  exchange.fetchOHLCV(symbol, timeframe=timeframe, since=since)
            except:
                print(f'Error fetching {symbol}')
                break

            # Did we received enough valid data
            if ohlcv and len(ohlcv) > 1:
                since = ohlcv[-1][0] + (ohlcv[-1][0] - ohlcv[-2][0])

                # Append OHLCV data
                ohlcv_data += ohlcv
            else:
                break

        # Return OHLCV data
        return ohlcv_data

def data_to_df(data):
    df = pd.DataFrame(data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
    df['Timestamp'] = pd.to_datetime(df.Timestamp, unit='ms')
    df = df.set_index('Timestamp')
    return df

counter = 0
for coin in tickers:
    data = fetchOHLCVData(exchange_type, coin, timeframe, since)
    df = data_to_df(data)
    coin = coin.replace("/","")
    df.to_csv(f'{filepath}{coin}.CSV')
    counter +=1
    print(f'{coin} updated.')
    # print(df.tail(2))

print(f'{len(tickers)} tickers chosen, {counter} updated.')