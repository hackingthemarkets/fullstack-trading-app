import con_config as conf
import backtrader
import pandas as pd
import sqlite3
from datetime import date, datetime, time, timedelta
import matplotlib
import psycopg2


class OpeningRangeBreakout(backtrader.Strategy):
    params = dict(
        num_opening_bars=3
    )

    def __init__(self):
        self.opening_range_low = 0
        self.opening_range_high = 0
        self.opening_range = 0
        self.bought_today = False
        self.order = None

    def log(self, txt, dt=None):
        if dt is None:
            dt = self.datas[0].datetime.datetime()

        print('%s, %s' % (dt, txt))

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        if order.status in [order.Completed]:
            order_details = f"{order.executed.price}, Cost: {order.executed.value}, Comm {order.executed.comm}"

            if order.isbuy():
                self.log(f"BUY EXECUTED, Price: {order_details}")
            else:  # Sell
                self.log(f"SELL EXECUTED, Price: {order_details}")

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        self.order = None

    def next(self):
        current_bar_datetime = self.data.num2date(self.data.datetime[0])
        previous_bar_datetime = self.data.num2date(self.data.datetime[-1])

        if current_bar_datetime.date() != previous_bar_datetime.date():
            self.opening_range_low = self.data.low[0]
            self.opening_range_high = self.data.high[0]
            self.bought_today = False

        opening_range_start_time = time(9, 30, 0)
        dt = datetime.combine(date.today(), opening_range_start_time) + \
            timedelta(minutes=self.p.num_opening_bars * 5)
        opening_range_end_time = dt.time()

        if (current_bar_datetime.time() >= opening_range_start_time) \
            and (current_bar_datetime.time() < opening_range_end_time):
                self.opening_range_high = max(self.data.high[0], self.opening_range_high)
                self.opening_range_low = min(self.data.low[0], self.opening_range_low)
                self.opening_range = self.opening_range_high - self.opening_range_low
        else:

            if self.order:
                return

            if self.position and (self.data.close[0] > (self.opening_range_high + self.opening_range)):
                self.close()

            if self.data.close[0] > self.opening_range_high and not self.position and not self.bought_today:
                self.order = self.buy()
                self.bought_today = True

            if self.position and (self.data.close[0] < (self.opening_range_high - self.opening_range)):
                self.order = self.close()

            if self.position and current_bar_datetime.time() >= time(15, 45, 0):
                self.log("RUNNING OUT OF TIME - LIQUIDATING POSITION")
                self.close()

    def stop(self):
        self.roi = (self.broker.get_value() / 100000) - 1.0
        
        self.log('(Num Opening Bars %2d) Ending Value %.2f ROI %.2f' %
                 (self.params.num_opening_bars,self.broker.getvalue(),self.roi))

        if self.broker.getvalue() > 130000:
            self.log("*** BIG WINNER ***")

        if self.broker.getvalue() < 70000:
            self.log("*** MAJOR LOSER ***")


if __name__ == '__main__':
    conn = psycopg2.connect(**conf.con_dict)
    query = """--sql
            select  b.tradingsymbol
            from ( 
            select a.tradingsymbol, 
            rank() over( order by a.volume desc) rank1
            from (
            select tradingsymbol, avg(Volume) as volume
            from equities.candlestick
            where candle_date_time >= '01-01-2020'
            and cast(candle_date_time as time(0)) > '09:30:00'
            and cast(candle_date_time as time(0)) < '15:30:00'
            and candle_length = '5minute'
            group by tradingsymbol) as a) as b
            where b.rank1 <50;
            """
    stocks = pd.read_sql(query, con=conn)
    stocks_list = stocks['tradingsymbol']


    for stock in stocks_list:
        print(f"== Testing {stock} ==")
        cerebro = backtrader.Cerebro()
        cerebro.broker.setcash(100000.0)
        cerebro.broker.setcommission(commission=0.001)  # 0.1% of the operation value
        cerebro.addsizer(backtrader.sizers.PercentSizer, percents=95)
        cerebro.addwriter(backtrader.WriterFile, csv=True)
        query = f"""--sql
                select *
                from equities.candlestick
                where candle_date_time >= '01-03-2020'
                and cast(candle_date_time as time(0)) > '09:30:00'
                and cast(candle_date_time as time(0)) <= '15:30:00'
                and tradingsymbol = '{stock}'
                and candle_length = '5minute';
                """

        dataframe = pd.read_sql(query,
                                con=conn,
                                index_col='candle_date_time',
                                parse_dates=['datetime'])
        data = backtrader.feeds.PandasData(dataname=dataframe)

        cerebro.adddata(data)
        cerebro.addstrategy(OpeningRangeBreakout)

        # strats = cerebro.optstrategy(OpeningRangeBreakout, num_opening_bars=\
        # [15, 30, 60])

        cerebro.run()
        # cerebro.plot()
