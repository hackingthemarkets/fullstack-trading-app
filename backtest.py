import con_config as conf
import backtrader as bt
import pandas as pd
import sqlite3
from datetime import date, datetime, time, timedelta
import matplotlib
import psycopg2
import os
import rootpath
from analyzer_functions import printTradeAnalysis, printSQN, printDrawDown, printSharpeR


class OpeningRangeBreakout(bt.Strategy):
    params = dict(
        num_opening_bars=3,
        fast_length= 5*12*6,
        slow_length= 25*12*6
    )

    def __init__(self):
        self.opening_range_low = 0
        self.opening_range_high = 0
        self.opening_range = 0
        self.bought_today = False
        self.order = None
        self.crossovers = []
        
        # for d in self.datas: 
        #     ma_fast = bt.ind.SMA(d, period = self.params.fast_length)
        #     ma_slow = bt.ind.SMA(d, period = self.params.slow_length)
            
        #     self.crossovers.append(bt.ind.CrossOver(ma_fast, ma_slow))
           
    def log(self, txt, dt=None):
        if dt is None:
            dt = self.datas[0].datetime.datetime()

        # print('%s, %s' % (dt, txt))

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
        self.opening_range_low = {}
        self.opening_range_high = {}
        self.bought_today = {}
        self.opening_range = {}
        for i, d in enumerate(self.datas):
            current_bar_datetime = d.num2date(d.datetime[0])
            previous_bar_datetime = d.num2date(d.datetime[-1])

            if current_bar_datetime.date() != previous_bar_datetime.date():
                self.opening_range_low[i] = d.low[0]
                self.opening_range_high[i] = d.high[0]
                self.bought_today[i] = False

            opening_range_start_time = time(9, 30, 0)
            dt = datetime.combine(date.today(), opening_range_start_time) + \
                timedelta(minutes=self.p.num_opening_bars * 5)
            opening_range_end_time = dt.time()

            if (current_bar_datetime.time() >= opening_range_start_time) \
                    and (current_bar_datetime.time() < opening_range_end_time):
                self.opening_range_high[i] = max(
                    d.high[0], self.opening_range_high[i])
                self.opening_range_low[i] = min(
                    d.low[0], self.opening_range_low[i])
                self.opening_range[i] = self.opening_range_high[i] - self.opening_range_low[i]
            else:

                if self.order:
                    return

                if self.getposition(d).size and (d.close[0] > (self.opening_range_high[i] + self.opening_range[i])):
                    self.close()

                if d.close[0] > self.opening_range_high[i] and not self.getposition(d).size and not self.bought_today[i]:
                    self.order = self.buy()
                    self.bought_today[i] = True

                if self.getposition(d).size and (self.data.close[0] < (self.opening_range_high[i] - self.opening_range[i])):
                    self.order = self.close()

                if self.getposition(d).size and current_bar_datetime.time() >= time(15, 45, 0):
                    self.log("RUNNING OUT OF TIME - LIQUIDATING POSITION")
                    self.close()

    def stop(self):
        self.roi = (self.broker.get_value() / 100000) - 1.0

        # self.log('(Num Opening Bars %2d) Ending Value %.2f ROI %.2f' %
        #          (self.params.num_opening_bars, self.broker.getvalue(), self.roi))

        # if self.broker.getvalue() > 130000:
        #     # self.log("*** BIG WINNER ***")

        # if self.broker.getvalue() < 70000:
        #     # self.log("*** MAJOR LOSER ***")


if __name__ == '__main__':

    # Reading in the csv
    # Getting the rootpath of the working directory
    # working_dir = rootpath.detect(__file__)
    working_dir = "C:\\Users\\40100147\\Abhishek\\Projects\\fullstack-trading-app"
    nifty50_path = os.path.join(working_dir, "data\\nifty50.csv")
    outputpath = os.path.join(working_dir, "data\\result_df_2.csv")
    nifty50 = pd.read_csv(nifty50_path, header=0, sep='\t')
    nifty50_list = tuple(nifty50['Symbol'].tolist())
    conn = psycopg2.connect(**conf.con_dict)

    query = f"""--sql
            select  a.tradingsymbol
            from ( 
            select tradingsymbol, avg(Volume) as volume
            from equities.candlestick
            where candle_date_time >= '01-01-2020'
            and cast(candle_date_time as time(0)) > '09:30:00'
            and cast(candle_date_time as time(0)) < '15:30:00'
            and candle_length = '5minute'
            and tradingsymbol in {nifty50_list}
            group by tradingsymbol) as a;
            """
    stocks = pd.read_sql(query, con=conn)
    stocks_list = stocks['tradingsymbol']
    result_df = []
    cerebro = bt.Cerebro()

    for stock in stocks_list:
        query = f"""--sql
                select *
                from equities.candlestick
                where candle_date_time >= '01-10-2020'
                and cast(candle_date_time as time(0)) > '09:30:00'
                and cast(candle_date_time as time(0)) <= '15:30:00'
                and tradingsymbol = '{stock}'
                and candle_length = '5minute';
                """
        dataframe = pd.read_sql(query,
                                con=conn,
                                index_col='candle_date_time',
                                parse_dates=['datetime'])
        data = bt.feeds.PandasData(dataname=dataframe)

        cerebro.adddata(data, name = stock)

    temp_list = []
    initial_cash = 100000.0
    cerebro.broker.setcash(initial_cash)
    # 0.01% of the operation value
    cerebro.broker.setcommission(commission=0.0001)
    cerebro.addsizer(bt.sizers.PercentSizer, percents=95)
    cerebro.addwriter(bt.WriterFile, csv=True)
    cerebro.addstrategy(OpeningRangeBreakout)

    # strats = cerebro.optstrategy(OpeningRangeBreakout, num_opening_bars=\
    # [15, 30, 60])
    # Add the analyzers we are interested in
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="ta")
    cerebro.addanalyzer(bt.analyzers.SQN, _name="sqn")
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="ddown")
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharper", riskfreerate=0.04, annualize=True, 
                        timeframe=bt.TimeFrame.Days, compression=1)

    # Run over everything
    strategies = cerebro.run()
    firstStrat = strategies[0]

    # print the analyzers
    print_list = printTradeAnalysis(firstStrat.analyzers.ta.get_analysis())
    printsqn = printSQN(firstStrat.analyzers.sqn.get_analysis())
    printdrawdown = printDrawDown(firstStrat.analyzers.ddown.get_analysis())
    printsharpe = printSharpeR(firstStrat.analyzers.sharper.get_analysis())
    temp_list = [stock] + [initial_cash] + print_list[1] + print_list[3] + \
                [printsqn] + printdrawdown[1] + [printsharpe]
    # Get final portfolio Value
    portvalue = cerebro.broker.getvalue()

    # Print out the final result
    print('Final Portfolio Value: ${}'.format(portvalue))
    # cerebro.run()
    # cerebro.plot()
    result_df.append(temp_list)
    resut_df = pd.DataFrame(result_df, columns = ['Stock','Initial_Portfolio','Total Open', 'Total Closed', 'Total Won', 'Total Lost',
                                                'Strike Rate','Win Streak', 'Losing Streak', 'PnL Net','SQN',
                                                'drawdown', 'moneydown', 'len_drawdown', 'max_drawdown','max_moneydown','max_len_drawdown',
                                                'sharper'])    
    resut_df.to_csv(outputpath,index=False)
