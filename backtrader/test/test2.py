from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime  # For datetime objects
import os.path  # To manage paths
import sys  # To find out the script name (in argv[0])
import akshare as ak  # 升级到最新版
# Import the backtrader platform
import backtrader as bt
import pandas as pd


# 增加 notify_order


# Create a Stratey
class TestStrategy(bt.Strategy):

    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close
        self.order = None

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY EXECUTED, %.2f' % order.executed.price)
            elif order.issell():
                self.log('SELL EXECUTED, %.2f' % order.executed.price)

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None

    def next(self):
        if self.order:
            return
        # Simply log the closing price of the series from the reference
        self.log('Close, %.2f' % self.dataclose[0])
        if not self.position:
            if self.dataclose[0] < self.dataclose[-1]:
                # current close less than previous close
                # self.log('BUY CREATE, %.2f' % self.dataclose[0])
                # self.buy()
                if self.dataclose[-1] < self.dataclose[-2]:
                    if self.dataclose[-2] < self.dataclose[-3]:
                        if self.dataclose[-3] < self.dataclose[-4]:
                            self.log('BUY CREATE, %.2f' % self.dataclose[0])
                            self.order = self.buy()

        else:
            if self.dataclose[0] > self.dataclose[-1]:
                if self.dataclose[-1] > self.dataclose[-2]:
                    if self.dataclose[-2] > self.dataclose[-3]:
                        if self.dataclose[-3] > self.dataclose[-4]:
                            if self.dataclose[-4] > self.dataclose[-5]:
                                if self.dataclose[-5] > self.dataclose[-6]:
                                    self.log('sell CREATE, %.2f' % self.dataclose[0])
                                    self.order = self.sell()


if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Add a strategy
    cerebro.addstrategy(TestStrategy)

    # Datas are in a subfolder of the samples. Need to find where the script is
    # because it could have been called from anywhere
    modpath = os.path.dirname(os.path.abspath(sys.argv[0]))
    stock_hfq_df = ak.stock_zh_a_hist(symbol="300085", adjust="hfq").iloc[:, :6]
    # 处理字段命名，以符合 Backtrader 的要求
    stock_hfq_df.columns = [
        'date',
        'open',
        'close',
        'high',
        'low',
        'volume',
    ]
    # 把 date 作为日期索引，以符合 Backtrader 的要求
    stock_hfq_df.index = pd.to_datetime(stock_hfq_df['date'])

    # Create a Data Feed
    data = bt.feeds.PandasData(
        dataname=stock_hfq_df,
        # Do not pass values before this date
        fromdate=datetime.datetime(2014, 4, 3),
        # Do not pass values before this date
        todate=datetime.datetime(2016, 6, 16))

    # Add the Data Feed to Cerebro
    cerebro.adddata(data)
    # Add a FixedSize sizer according to the stake
    cerebro.addsizer(bt.sizers.FixedSize, stake=2000)

    cerebro.broker.setcommission(commission=0.002)
    # Set our desired cash start
    cerebro.broker.setcash(10000000.0)

    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Run over everything
    cerebro.run()

    # Print out the final result
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    value = cerebro.broker.getvalue() - 10000000.0
    print('收益率:{:.2%}'.format(value / 10000000.0))
    cerebro.plot()  # 画图
