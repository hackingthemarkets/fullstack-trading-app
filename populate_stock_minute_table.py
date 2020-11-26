import config
import sqlite3
import pandas
import csv
import alpaca_trade_api as tradeapi
from datetime import datetime, timedelta

connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row

cursor = connection.cursor()

api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, config.API_URL)
symbols = []
stock_ids = {}

with open('qqq.csv') as f:
    reader = csv.reader(f)

    for line in reader:
        symbols.append(line[1])

cursor.execute("""
    SELECT * FROM stock
""")

stocks = cursor.fetchall()

for stock in stocks:
    symbol = stock['symbol']
    stock_ids[symbol] = stock['id']
    
for symbol in symbols:
    start_date = datetime(2020, 1, 6).date()
    end_date_range = datetime(2020, 11, 20).date()

    while start_date < end_date_range:
        end_date = start_date + timedelta(days=4)

        print(f"== Fetching minute bars for {symbol} {start_date} - {end_date} ==")
        minutes = api.polygon.historic_agg_v2(symbol, 1, 'minute', _from=start_date, to=end_date).df
        minutes = minutes.resample('1min').ffill()

        for index, row in minutes.iterrows():
            cursor.execute("""
                INSERT INTO stock_price_minute (stock_id, datetime, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (stock_ids[symbol], index.tz_localize(None).isoformat(), row['open'], row['high'], row['low'], row['close'], row['volume']))

        start_date = start_date + timedelta(days=7)

connection.commit()
