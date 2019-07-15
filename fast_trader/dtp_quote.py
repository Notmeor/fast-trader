# -*- coding: utf-8 -*-


class MarketFeed:
    pass


class TradeFeed(MarketFeed):
    """
    逐笔成交
    """
    name = 'trade_feed'

    @classmethod
    def get_int64_fields(cls):
        return ['nPrice', 'nTurnover']
                
    @classmethod
    def standardize(cls, data):
        price_fields = ['nPrice']

        for field in cls.get_int64_fields():
            data[field] = int(data[field])
        for field in price_fields:
            data[field] = data[field] / 10000
        return data
        


class IndexFeed(MarketFeed):
    """
    指数行情
    """
    name = 'index_feed'

    @classmethod
    def get_int64_fields(cls):
        return [
            'nOpenIndex', 'nHighIndex', 'nLowIndex',
            'nLastIndex', 'iTotalVolume', 'iTurnover',
            'nPreCloseIndex']

    @classmethod
    def standardize(cls, data):
        price_fields = [
            'nOpenIndex', 'nHighIndex', 'nLowIndex',
            'nLastIndex', 'nPreCloseIndex']

        for field in cls.get_int64_fields():
            data[field] = int(data[field])
        for field in price_fields:
            data[field] = data[field] / 10000
        return data


class TickFeed(MarketFeed):
    """
    快照行情
    """
    name = 'tick_feed'
    
    @classmethod
    def get_int64_fields(cls):
        return [
            'nPreClose',
            'nOpen',
            'nHigh',
            'nLow',
            'nMatch',
            'nAskPrice_0',
            'nAskPrice_1',
            'nAskPrice_2',
            'nAskPrice_3',
            'nAskPrice_4',
            'nAskPrice_5',
            'nAskPrice_6',
            'nAskPrice_7',
            'nAskPrice_8',
            'nAskPrice_9',
            'nAskVol_0',
            'nAskVol_1',
            'nAskVol_2',
            'nAskVol_3',
            'nAskVol_4',
            'nAskVol_5',
            'nAskVol_6',
            'nAskVol_7',
            'nAskVol_8',
            'nAskVol_9',
            'nBidPrice_0',
            'nBidPrice_1',
            'nBidPrice_2',
            'nBidPrice_3',
            'nBidPrice_4',
            'nBidPrice_5',
            'nBidPrice_6',
            'nBidPrice_7',
            'nBidPrice_8',
            'nBidPrice_9',
            'nBidVol_0',
            'nBidVol_1',
            'nBidVol_2',
            'nBidVol_3',
            'nBidVol_4',
            'nBidVol_5',
            'nBidVol_6',
            'nBidVol_7',
            'nBidVol_8',
            'nBidVol_9',
            'iVolume',
            'iTurnover',
            'nTotalBidVol',
            'nTotalAskVol',
            'nWeightedAvgBidPrice',
            'nWeightedAvgAskPrice',
            'nHighLimited',
            'nLowLimited']

    @classmethod
    def standardize(cls, data):
        price_fields = [
            'nAskPrice_0', 'nAskPrice_1', 'nAskPrice_2',
            'nAskPrice_3', 'nAskPrice_4', 'nAskPrice_5',
            'nAskPrice_6', 'nAskPrice_7', 'nAskPrice_8',
            'nAskPrice_9', 'nBidPrice_0', 'nBidPrice_1',
            'nBidPrice_2', 'nBidPrice_3', 'nBidPrice_4',
            'nBidPrice_5', 'nBidPrice_6', 'nBidPrice_7',
            'nBidPrice_8', 'nBidPrice_9', 'nHigh',
            'nHighLimited', 'nLow', 'nLowLimited',
            'nMatch', 'nOpen', 'nPreClose',
            'nWeightedAvgAskPrice', 'nWeightedAvgBidPrice']
        
        for field in cls.get_int64_fields():
            data[field] = int(data[field])

        for field in price_fields:
            data[field] = data[field] / 10000
        return data


class OrderFeed(MarketFeed):
    """
    逐笔报单
    """
    name = 'order_feed'

    @classmethod
    def get_int64_fields(cls):
        return ['nPrice']

    @classmethod
    def standardize(cls, data):
        price_fields = ['nPrice']

        for field in cls.get_int64_fields():
            data[field] = int(data[field])

        for field in price_fields:
            data[field] = data[field] / 10000
        return data


class QueueFeed(MarketFeed):
    """
    成交队列
    """
    name = 'queue_feed'

    @classmethod
    def get_int64_fields(cls):
        return ['nPrice']

    @classmethod
    def standardize(cls, data):
        price_fields = ['nPrice']

        for field in cls.get_int64_fields():
            data[field] = int(data[field])

        for field in price_fields:
            data[field] = data[field] / 10000
        return data


class OptionsFeed(MarketFeed):
    """
    期权行情
    """
    name = 'options_feed'

    @classmethod
    def get_int64_fields(cls):
        return [
            'openInterest',
            'preOpenInterest']

    @classmethod
    def standardize(cls, data):
        for field in cls.get_int64_fields():
            data[field] = int(data[field])
        return data


class FuturesFeed(MarketFeed):
    """
    期货ctp行情
    """
    name = 'ctp_feed'

    @classmethod
    def get_int64_fields(cls):
        return [
            'openInterest',
            'preOpenInterest']
    
    @classmethod
    def standardize(cls, data):
        for field in cls.get_int64_fields():
            data[field] = int(data[field])
        return data


FEED_TYPE_NAME_MAPPING = {
    'tick_feed': TickFeed,
    'trade_feed': TradeFeed,
    'order_feed': OrderFeed,
    'order_queue': QueueFeed,
    'index_feed': IndexFeed,
    'options_feed': OptionsFeed,
    'ctp_feed': FuturesFeed
}

