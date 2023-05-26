import os

from yahoofinancials import YahooFinancials
from datetime import datetime
from datetime import date
from datetime import timedelta
import pandas as pd
from csv import reader

ticker_file = r'c:\invest\ticker.txt'
days_back = 60
out_file = r'c:\temp\stock_price_volume.xlsx'
no_price_stock_file = r'c:\temp\no_price_stock.txt'
ticker_file = open(ticker_file, 'r')
reader = reader(ticker_file)
ticker_list = list(reader)
ticker_file.close()
ticker = set([x[0] for x in ticker_list])

end_date = date.today().strftime('%Y-%m-%d')
start_date = (date.today() + timedelta(days=-60)).strftime('%Y-%m-%d')

columns = ['ticker', 'trxn_date', 'current_price', 'current_volume', 'avg_price', 'avg_volume', 'buy_price', 'buy_volume', 'buy_flag', 'volume_flag']
stock_data = []
stock_buy_data = []
no_price_stock = []

# loop through tickers and get stock data
for x in ticker:
    ticker_symbol = x
    yahoo_financials = YahooFinancials(ticker_symbol)

    data = yahoo_financials.get_historical_price_data(start_date=start_date,
                                                      end_date=end_date,
                                                      time_interval='daily')
    if 'prices' in data[ticker_symbol]:
        stock_df = pd.DataFrame(data[ticker_symbol]['prices'])
        trxn_date = stock_df['formatted_date'].iloc[-1]
        stock_df = stock_df.drop('date', axis=1).set_index('formatted_date')
        avg_price = stock_df['adjclose'].mean()
        min_price = stock_df['adjclose'].min()
        max_price = stock_df['adjclose'].max()
        avg_volume = stock_df['volume'].mean()
        current_price = stock_df['adjclose'].iloc[-1]
        current_volume = stock_df['volume'].iloc[-1]
        buy_price = avg_price * .97
        buy_volume = avg_volume * 1.02
        if current_price <= buy_price:
            buy_flag = 1
        else:
            buy_flag = 0
        if current_volume >= buy_volume:
            volume_flag = 1
        else:
            volume_flag = 0
        stock_data.append([ticker_symbol, trxn_date, current_price, current_volume, avg_price, avg_volume, buy_price, buy_volume, buy_flag, volume_flag])
    else:
        no_price_stock.append(ticker_symbol)

stock_data_df = pd.DataFrame(stock_data, columns=columns)
stock_data_df['current_volume'] = stock_data_df['current_volume'].astype('int')
stock_data_df['avg_volume'] = stock_data_df['avg_volume'].astype('int')
stock_data_df['buy_volume'] = stock_data_df['buy_volume'].astype('int')
stock_data_df = stock_data_df.sort_values(by=['buy_flag', 'volume_flag'], ascending=(False, False))

stock_data_df.to_excel(excel_writer=out_file, index=False, header=True)
os.startfile(out_file)

if len(no_price_stock) > 0:
    with open(no_price_stock_file, 'w') as f:
        for x in no_price_stock:
            f.write(x + '\n')

    os.startfile(no_price_stock_file)
