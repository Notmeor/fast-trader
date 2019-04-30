import pandas as pd
import datetime as dt
import re
from fast_trader.dtp_quote import TickFeed
from fast_trader.feed_store import FeedStore
from fast_trader.rest_api import query_capital, Order, place_batch_order, cancel_all, query_positions
from fast_trader.dtp import type_pb2 as dtp_type
from smartbeta.smartfactor import SmartFactor
from data_provider.datafeed.quote_feed import QuoteFeed
from data_provider.datafeed.universe import Universe
from data_provider.nestlib.trading_cal import TradeCal
import time
import traceback

tc_handle = TradeCal()
uni_handle = Universe()


class OrderGenerator(object):
    """
    根据信号生成订单列表，包括买单和卖单
    """
    def __init__(self, factor_name):
        self.factor_name = factor_name
        self.factor_direction = -1  # -1代表因子值越大越好
        self.today = dt.date.today().strftime('%Y%m%d')
        self.pre_trading_day = tc_handle.shift_date(self.today, 1)

        self.trade_amount_threshold = 0.9  # 取成交量前90%的股票（相当于剔除成交量最小的10%）
        self.holds_num = 400   # 通过成交量筛选后，取Top 400个股票作为初始持仓
        self.high_open_percentage = 0.3  # 将高开的前30%剔除
        self.low_open_ret = -0.08        # 将低开超过8%的剔除

        self.sell_price_scale = 0.99   # 将要卖出的股票价格乘以0.99缩放，提高成交概率
        self.buy_price_scale = 1.01    # 将要买入的股票价格乘以1.01缩放，提高成交概率
        self.sell_order_initial_id = 200110  # 卖单的初始id
        self.buy_order_initial_id = 300000   # 买单的初始id

        self.stock_tick_store = FeedStore(TickFeed)

        self.pre_holds_df = pd.DataFrame()  # 上个交易日的持仓
        self.pre_close_df = pd.DataFrame()  # 上个交易日持仓对应的昨收
        self.pre_holds_tickers = []

        self.today_holds_df = pd.DataFrame()        # 今日过滤后的股票
        self.today_un_filtered_holds_tickers = []   # 今日没有进过高低开过滤的股票列表
        self.common_tickers = []                    # 今日目标持仓和历史持仓的股票列表的交集
        self.is_prepared = False
        self.record_dict = {}    # 记录下单过程中产生的中间变量

    def get_pre_holds(self, path='2019-04-25_Position.csv'):
        pre_holds_df = pd.read_csv(path)
        pre_holds_df.rename(columns={'Code': 'ticker', 'Balance': 'quantity'}, inplace=True)
        pre_holds_df = pre_holds_df[['ticker', 'quantity']]
        pre_holds_df['ticker'] = pre_holds_df['ticker'].apply(self.add_suffix)

        return pre_holds_df

    def filter_by_jump_ret(self, stock_quotes, real_time_price_df):
        """根据跳空价格对持仓进行过滤
        剔除涨幅靠近前high_open的股票
        剔除跌幅超过low_open的股票
        """
        merged_df = stock_quotes.merge(real_time_price_df, on='ticker', how='inner')
        merged_df[['pre_close', 'real_time_price']] = merged_df[['pre_close', 'real_time_price']].astype(float)
        merged_df['jump_ret'] = (merged_df['real_time_price'] - merged_df['pre_close']) / merged_df['pre_close']

        merged_df['jump_ret_rank'] = merged_df['jump_ret'].rank(pct=True)
        ret_df = merged_df[merged_df.loc[:, 'jump_ret_rank'] < self.high_open_percentage]

        ret_df = ret_df[ret_df['jump_ret'] > self.low_open_ret]  # 当跌幅超过8%，不进行买入
        return ret_df

    def get_pre_close(self, pre_trading_day):
        """获取昨收"""
        qfq_quote = QuoteFeed(
            universe_ticker='000902.SH',
            begin_day=pre_trading_day,
            end_day=pre_trading_day,
            tracking_freq=86400,
            #     adjust_method="forward",
        )
        stock_quotes = qfq_quote.get_stock_quote().loc[:, ['close', 'ticker', 'datetime_str']]
        stock_quotes = stock_quotes.rename(columns={'close': 'pre_close'})
        return stock_quotes

    @staticmethod
    def add_suffix(ticker):
        """股票添加后缀"""
        str_ticker = str(ticker).zfill(6)
        suffix = '.SH' if str_ticker.startswith('6') else '.SZ'
        return str_ticker + suffix

    @staticmethod
    def round_price(price, buy=True):
        """将价格规整到小数点后两位
        如果是买入，应该使用进一法
        如果是卖出，直接去掉小数点2位数以后的值
        """

        add = 0.01 if buy else 0

        price_str = str(price)
        price_ls = price_str.split('.')
        if len(price_ls) <= 1 or len(price_ls[1]) <= 2:
            return price_str

        price_str = re.match(r'\d+.\d{2}', price_str).group(0)
        price_float_tmp = str(float(price_str) + add)
        return re.match(r'\d+.\d{2}', price_float_tmp + '00').group(0)

    @staticmethod
    def filter_ticker(pre_holds_real_df):
        """
        跌停的股票无法卖出(但是要挂出买单出去)
        涨停的股票不卖
        """
        holds_df = pre_holds_real_df.copy()
        holds_df['low_diff'] = holds_df['real_time_price'] - holds_df['nLowLimited']
        holds_df['high_diff'] = holds_df['nHighLimited'] - holds_df['real_time_price']

        low_holds_df = holds_df[holds_df['low_diff'] < 0.01]  # 跌停的股票
        high_holds_df = holds_df[holds_df['high_diff'] < 0.01]  # 涨停的股票
        if len(low_holds_df) > 0:
            print('昨日持仓中当前跌停的股票列表：')
            print(low_holds_df['ticker'].tolist())

        if len(high_holds_df) > 0:
            print('昨日持仓中当前涨停的股票列表：')
            print(high_holds_df['ticker'].tolist())

        return holds_df[holds_df['high_diff'] >= 0.01].copy().drop(columns=['low_diff', 'high_diff'])

    def trade_amount_filter(self, trading_day):
        """获取成交量在前90%的股票列表（剔除成交量最小的10%的股票）"""
        trade_amount = SmartFactor('monthlytradeamount').load(trading_day, trading_day)
        trade_amount = trade_amount.set_index('security_code')['factor_value']
        trade_amount = trade_amount.sort_values(ascending=False)
        filter_count = int(len(trade_amount) * self.trade_amount_threshold)
        filter_basket = trade_amount.head(filter_count)
        return filter_basket.index.tolist()

    def get_filtered_signal(self):
        """获取成交量过滤后的因子，选择top300个，参数由self.holds_num"""
        try:
            signal_df = SmartFactor(self.factor_name).load(self.pre_trading_day, self.pre_trading_day)
            signal_se = signal_df.set_index('security_code')['factor_value']
        except Exception as e:
            raise ValueError('读取因子错误!')

        filter_basket = self.trade_amount_filter(self.pre_trading_day)  # 成交量过滤
        ascending = False if self.factor_direction == -1 else True
        filtered = signal_se.filter(filter_basket).sort_values(ascending=ascending)
        return filtered[:self.holds_num].index.tolist()  # 先期获取300个股票

    def subscribe_quote(self, to_sub_list):
        """订阅行情
        订阅行情完成后要进行测试，是否已经完全推送过来
        """
        # 映射字段
        self.stock_tick_store.set_field_mapping({
            'code': 'szWindCode',
            'price': 'nMatch',
            'date': 'nActionDay',
            'time': 'nTime',
        })
        # 订阅个股行情
        self.stock_tick_store.subscribe(to_sub_list)

        print('开始订阅行情，订阅数量为：' + str(len(to_sub_list)))
        self.stock_tick_store.connect()

    def prepare_data(self):
        """准备数据
        1. 读取因子数据
        2. 读取历史持仓
        3. 读取历史持仓的昨收
        4. 订阅实时行情
        """
        self.today_un_filtered_holds_tickers = self.get_filtered_signal()
        print('今日持仓生成完毕，股票数量： ' + str(len(self.today_un_filtered_holds_tickers)))

        self.pre_holds_df = self.get_pre_holds()
        print('获取昨日持仓，股票数量： ' + str(len(self.pre_holds_df)))

        self.pre_holds_tickers = self.pre_holds_df['ticker'].tolist()
        self.pre_close_df = self.get_pre_close(self.pre_trading_day)

        to_sub_list = self.today_un_filtered_holds_tickers + self.pre_holds_tickers
        self.subscribe_quote(to_sub_list)

        self.is_prepared = True

    def check_real_time_price_is_ready(self, try_times=1):
        if not self.is_prepared:
            raise ValueError('数据尚未开始准备好!')
        to_sub_list = self.today_un_filtered_holds_tickers + self.pre_holds_tickers

        print('订阅的股票数量：'+str(len(to_sub_list)))

        retry_times = try_times  # 尝试10次，如果没有取到数据，就直接返回
        while True:
            if retry_times <= 0:
                break

            real_tick_ls = self.stock_tick_store.get_ticks(to_sub_list, None)

            if len(to_sub_list) == len(real_tick_ls):
                print('实时行情已经正确获取')
                break

            else:
                retry_times -= 1
                msg = '实时行情推送的股票数量：' + str(len(real_tick_ls)) + '. 订阅的股票数量：' + str(len(to_sub_list))
                print('try ' + str(try_times - retry_times) + 'th. ' + msg)
                time.sleep(2)

    def get_real_time_price(self, ticker_ls):
        """
        从订阅的行情里面读取数据
        :param ticker_ls: 股票列表，注意要在初始化的时候被订阅过
        :return:
        """
        tick_ls = self.stock_tick_store.get_ticks(ticker_ls, None)
        filter_tick_ls = []
        for my_dict in tick_ls:
            filter_tick_ls.append(
                {k: v for k, v in my_dict.items() if k in ['szWindCode', 'nMatch', 'nHighLimited', 'nLowLimited']})

        real_time_price_df = pd.DataFrame(filter_tick_ls)
        real_time_price_df = real_time_price_df.rename(
            columns={'szWindCode': 'ticker', 'nMatch': 'real_time_price'})

        return real_time_price_df

    def sell_stocks(self):
        # 根据实时行情得到今日持仓列表
        today_real_time_price_df = self.get_real_time_price(self.today_un_filtered_holds_tickers)

        # 根据高低开过滤，产生今日目标持仓
        self.today_holds_df = self.filter_by_jump_ret(self.pre_close_df, today_real_time_price_df)

        # 根据实时行情，剔除昨日持仓中停牌和涨停的
        pre_real_time_price_df = self.get_real_time_price(self.pre_holds_tickers)
        pre_holds_real_df = self.pre_holds_df.merge(pre_real_time_price_df, on='ticker', how='inner')
        pre_holds_real_df = self.filter_ticker(pre_holds_real_df)

        # 根据昨日持仓和今日目标持仓得到最终的卖出清单
        self.common_tickers = set(pre_holds_real_df['ticker'].tolist()) & set(self.today_holds_df['ticker'].tolist())
        self.record_dict['common_tickers'] = self.common_tickers

        to_sell_df = pre_holds_real_df[~pre_holds_real_df['ticker'].isin(self.common_tickers)].copy()
        to_sell_df['real_time_price'] = to_sell_df['real_time_price'] * self.sell_price_scale  # 将价格下调，提高成交概率

        sell_order_ls = self.generate_orders_by_df(to_sell_df, dtp_type.ORDER_SIDE_SELL)
        self.send_orders(sell_order_ls)
        self.record_dict['sell_order_df'] = to_sell_df

    def get_available_capital(self):
        """获取可用资金"""
        query_result = query_capital()

        # Warning: 下面是伪造的查询结果
        # query_result = [{'accountId': '293419',
        #                  'accountName': 'gxj',
        #                  'currency': '00',
        #                  'available': '1100000'
        #                  }]

        initial_capital = float(query_result[0]['available'])
        return initial_capital

    def get_current_time(self):
        return str(dt.datetime.now().time())

    def _buy_stocks(self, tickers_ls):

        initial_capital = self.get_available_capital()
        to_buy_df = self.get_real_time_price(tickers_ls)

        single_stock_capital = initial_capital / len(to_buy_df)
        to_buy_df['real_time_price'] = to_buy_df['real_time_price'] * self.buy_price_scale  # 将成交价格提升1%，提高成交概率
        to_buy_df['quantity'] = single_stock_capital / (to_buy_df['real_time_price'] * 100)

        # 去掉不足一手的股票，重新分配资金
        if len(to_buy_df[to_buy_df['quantity'] < 1]) > 0:
            print(to_buy_df[to_buy_df['quantity'] < 1])
            to_buy_df = to_buy_df[to_buy_df['quantity'] >= 1]
            single_stock_capital = initial_capital / len(to_buy_df)
            to_buy_df['quantity'] = single_stock_capital / (to_buy_df['real_time_price'] * 100)

        to_buy_df['quantity'] = to_buy_df['quantity'].astype(int) * 100  # 持仓量

        buy_order_ls = self.generate_orders_by_df(to_buy_df, dtp_type.ORDER_SIDE_BUY)
        self.send_orders(buy_order_ls)

        self.record_dict['initial_capital_'+self.get_current_time()] = initial_capital
        self.record_dict['buy_order_df_'+self.get_current_time()] = to_buy_df

    def buy_stocks(self):
        # 共同的持仓不进行买入
        try:
            already_buy_tickers = self.get_already_buy()
            filter_tickers = already_buy_tickers + self.common_tickers
            filtered_today_holds_df = self.today_holds_df[~self.today_holds_df['ticker'].isin(filter_tickers)]
            self._buy_stocks(filtered_today_holds_df['ticker'].tolist())
        except Exception as e:
            traceback.print_exc()

    def get_already_buy(self):
        """获取已经买入的股票列表"""
        positions = query_positions()
        df = pd.DataFrame(positions)[['code', 'buyQuantity', 'cost']]
        df.rename(columns={'code': 'ticker'}, inplace=True)
        df = df[df['buyQuantity'] >= 100]
        df['ticker'] = df['ticker'].apply(self.add_suffix)
        self.record_dict['already_buy_'+self.get_current_time()] = df   # 记录已经买入的股票
        return df['ticker'].tolist()

    def generate_orders_by_df(self, df, order_side):
        """
        产生订单列表
        :param df: 包含股票[ticker, quantity, real_time_price]三列的DataFrame
        :param order_side: 订单方向，为dtp_type.ORDER_SIDE_BUY或者dtp_type.ORDER_SIDE_SELL
        :return:
        """
        if order_side == dtp_type.ORDER_SIDE_BUY:
            initial_id = self.buy_order_initial_id
        elif order_side == dtp_type.ORDER_SIDE_SELL:
            initial_id = self.sell_order_initial_id
        else:
            raise ValueError('输入买卖方向参数错误！')

        order_ls = []

        for index, row in df.iterrows():
            order = Order()
            order.code = row['ticker'][:6]
            order.exchange = 1 if row['ticker'].endswith('SH') else 2
            order.original_id = str(initial_id)
            order.price = self.round_price(row['real_time_price'], buy=True)
            order.quantity = row['quantity']
            order.side = order_side

            order_ls.append(order)
            initial_id += 1
        return order_ls

    def send_orders(self, order_ls):
        """批量发送订单列表"""
        place_batch_order(order_ls)

    def canceled_tickers(self):
        """撤掉所有未成交订单，返回这些股票列表
        """
        canceled_orders = cancel_all()
        return canceled_orders['tickers']   # TODO: 返回的结构

    def re_buy_canceled_stocks(self):
        """重新买入已经"""
        tickers = self.canceled_tickers()
        self._buy_stocks(tickers)


if __name__ == '__main__':
    order_handle = OrderGenerator('ep_ttm')
    order_handle.prepare_data()
    order_handle.check_real_time_price_is_ready(3)
    order_handle.sell_stocks()
    order_handle.buy_stocks()