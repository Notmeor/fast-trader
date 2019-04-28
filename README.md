# 快速交易平台

本模块是对快速交易平台接口（DTP）的python版本封装样例，如需在实际环境中使用，请自行保证测试的完整性，或参考本模块重新封装DTP接口。

有关DTP接口的详细信息，请参考《东吴证券快速交易平台接口使用说明书》。

## 模块结构

<img src="flowchart.svg"> 


### DTP接口

通过ZeroMQ实现接口通信，代码实现为`DTP`类

#### 同步请求通道

使用ZeroMQ的REQ/REP通信模式，包含  

- 账号登录   
- 账号登出
- 委托查询
- 成交查询
- 资金查询
- 持仓查询 

#### 异步请求通道

采用ZeroMQ的DEALER/DEALER通信模式，包含

- 委托申报
- 批量委托申报
- 撤单申报 

#### 柜台状态回报通道

采用ZeroMQ的PUB/SUB通信模式，包含

- 委托回报
- 成交回报
- 撤单回报 

#### 风控违规回报通道

采用ZeroMQ的PUB/SUB通信模式。包含
- 风控回报

### Trader交易端

基于DTP通道封装交易接口，供策略端调用，代码实现为`Trader`类

### QuoteFeed行情端

基于DTP通道封装行情接口，供策略端调用，代码实现为`QuoteFeed`类


## 数据结构

### 报单回报结构
```protobuf
message PlacedReport                                /* 消息: 委托确认回报 */
{                                                   // MUST: ReportHeader.api_id = 20001001
    string order_exchange_id = 1;                   // 交易所委托号
    string placed_time = 2;                         // 交易所委托确认时间,单位毫秒,不存日期 ;顶点柜台返回的是 HH:MM:SS 格式，推断交易所返回的也是这个格式，所以改成字符串格式 yinwb
    string freeze_amount = 3;                       // 冻结金额(负数表示解冻)
    OrderStatus status = 4;                         // 委托状态: 申报结果

    string order_original_id = 5;                   // 客户委托号

    string account_no = 6;                          // 资金账号
    Exchange exchange = 7;                          // 交易所
    string code = 8;                                // 证券代码
    uint32 quantity = 9;                            // 委托数量
    OrderSide order_side = 10;                      // 委托竞价方向
    string price = 11;                              // 价格留3位小数
}
```

### 成交回报结构
```protobuf
message FillReport                                  /* 消息: 委托成交回报 */
{                                                   // MUST: ReportHeader.api_id = 20001002
    string fill_exchange_id = 1;                    // 交易所成交编号
    string fill_time = 2;                           // 成交的时间,单位毫秒,不存日期;顶点柜台返回的是 HH:MM:SS 格式，推断交易所返回的也是这个格式，所以改成字符串格式 yinwb
    FillStatus fill_status = 3;                     // 成交状态 0:未知 1:成交 2:撤单 3: 废单 4:确认 (TBD: 可能来自'撤销标志')
                                                    // TBD: 成交状态中指的是什么，其中是否有撤单? 或者撤单成功会使用该回报?
                                                    // *** 成交回报的状态只会有成交，所以这个状态可以删除
    string fill_price = 4;                          // 本次成交价格
    uint32 fill_quantity = 5;                       // 本次成交数量; fill_status为撤单时，此数值为撤单数量(TBD)
    string fill_amount = 6;                         // 本次成交金额
    string clear_amount = 7;                        // (TBD)本次清算资金(委托为卖出方向时表示本次成交新增的可用资金),宽睿接口无此参数，建议取消
    uint32 total_fill_quantity = 8;                 // 该委托总成交数量(本笔成交处理后)
    string total_fill_amount = 9;                   // 该委托总成交金额(本笔成交处理后)
    uint32 total_cancelled_quantity = 10;           // (TBD)该委托已撤单数量? 怀疑撤单成功会有该回报? ***撤单成功走撤单回报

    string order_exchange_id = 11;                  // 交易所委托号
    string order_original_id = 12;                  // 客户委托号
    string account_no = 13;                         // 资金账号
    Exchange exchange = 14;
    string code = 15;
    string price = 16;                              // (TBD)价格留3位小数，宽睿接口无此参数，建议取消
    uint32 quantity = 17;                           // (TBD)委托数量，宽睿接口无此参数，建议取消
    OrderSide order_side = 18;                      // 委托竞价方向
}        
```

### 查询资金回报结构
```protobuf
message QueryCapitalResponse
{                                                   // MUST: ResponseHeader.api_id = 11003003
    string account_no = 1;
    string balance = 2;                             // 账户余额
    string available = 3;                           // 可用资金
    string freeze = 4;                              // 冻结金额
    string securities = 5;                          // 证券市值
    string total = 6;                               // 总资产
}
```

### 查询报单回报结构
```protobuf
message Order                                   // 委托明细
{
    string order_exchange_id = 1;               // 交易所委托号
    string order_original_id = 2;               // 客户委托号
    Exchange exchange = 3;
    string code = 4;
    string name = 5;
    string price = 6;                           // 价格留3位小数
    uint32 quantity = 7;                        // 委托数量
    OrderSide order_side = 8;                   // 委托竞价方向
    OrderType order_type = 9;                   // 委托竞价类型
    OrderStatus status = 10;
    string order_time = 11;                     // 交易所委托时间,单位毫秒,不存日期 ;顶点柜台返回的是 HH:MM:SS 格式，推断交易所返回的也是这个格式，所以改成字符串格式 yinwb

    string account_no = 12;
    string average_fill_price = 13;             // 成交均价
    uint32 total_fill_quantity = 14;            // 该委托总成交数量
    string total_fill_amount = 15;              // 该委托总成交金额
    string freeze_amount = 16;                  // 冻结金额(负数表示解冻)
    string clear_amount = 17;                   // (TBD)清算资金 ***买入为负，卖出为正
    uint32 total_cancelled_quantity = 18;       // 该委托已撤单数量
    string status_message = 19;                 // 委托状态的文字说明，包括废单原因
}
```

### 查询成交回报结构
```protobuf
message Fill                                    // 分笔成交明细
{
    string fill_exchange_id = 1;                // 交易所成交编号
    string fill_time = 2;                       // 成交的时间,单位毫秒,不存日期 ;顶点柜台返回的是 HH:MM:SS 格式，推断交易所返回的也是这个格式，所以改成字符串格式 yinwb
    FillStatus fill_status = 3;                 // (TBD: 同上FillReport)成交状态 0:未知 1:成交 2:撤单 3: 废单 4:确认 (TBD: 可能来自'撤销标志') *** 只有撤单和成交两个状态

    string fill_price = 4;                      // 本次成交价格
    int32 fill_quantity = 5;                   // 本次成交数量; fill_status为撤单时，此数值为撤单数量
    string fill_amount = 6;                     // 本次成交金额
    string clear_amount = 7;                    // 本次清算资金(委托为卖出方向时表示本次成交新增的可用资金)

    string order_exchange_id = 8;               // 交易所委托号
    string order_original_id = 9;               // 客户委托号
    Exchange exchange = 10;
    string code = 11;
    string name = 12;
    OrderSide order_side = 13;                  // 委托竞价方向
}
```


### 查询持仓回报结构
```protobuf
message PositionDetail
{
    Exchange exchange = 1;                      // 交易所
    string code = 2;                            // 证券代码
    string name = 3;                            // 证券名称
    int64 balance = 4;                          // 剩余数量(今持仓量)
    int64 available_quantity = 5;               // 可用数量(可卖出数量)
    int32 freeze_quantity = 6;                  // 冻结数量(可能是真正的冻结数量，例如：司法冻结等)
    int64 buy_quantity = 7;                     // 当日买入数量
    int64 sell_quantity = 8;                    // 当日卖出数量
    string market_value = 9;                    // 最新市值
    string cost = 10;                           // 持仓均价
}
```

**NOTE**   
*在`Strategy`中，以上返回结构中的表示具体金额的字段，均已从`str`转为`float`类型*

## API Reference

### fast_trader.dtp_quote.QuoteFeed.start

启动行情接收

```python
fast_trader.dtp_quote.QuoteFeed.start()
```

### fast_trader.dtp_quote.QuoteFeed.subscribe

订阅行情

```python
fast_trader.dtp_quote.QuoteFeed.subscribe(code)
```

- *code* (str | list) - 证券代码或证券代码列表

### fast_trader.dtp_quote.QuoteFeed.subscribe_all

订阅全部行情

```python
fast_trader.dtp_quote.QuoteFeed.subscribe_all()
```

### fast_trader.dtp_quote.QuoteFeed.add_listener

添加消息消费者

```python
fast_trader.dtp_quote.QuoteFeed.add_listener(listener)
```

- *listener* (Any) - Dispatcher对象

### fast_trader.strategy.Strategy.start

启动策略

```python
fast_trader.strategy.Strategy.start()
```

### fast_trader.strategy.Strategy.on_start

策略启动回调，策略启动时触发。用户策略覆盖此方法，实现策略启动后的一系列操作

```python
fast_trader.strategy.Strategy.on_start()
```

### fast_trader.strategy.Strategy.set_dispatcher

设置dispatcher

```python
fast_trader.strategy.Strategy.set_dispatcher(dispatcher)
```

- *dispatcher* (Dispatcher) - 消息分发者Dispatcher实例

### fast_trader.strategy.Strategy.set_trader

设置trader

```python
fast_trader.strategy.Strategy.set_trader(trader)
```

- *trader* (Trader) - Trader实例

### fast_trader.strategy.Strategy.start_market

启动所有绑定的行情数据源

```python
fast_trader.strategy.Strategy.start_market()
```

### fast_trader.strategy.Strategy.add_datasource

绑定行情数据源

```python
fast_trader.strategy.Strategy.add_datasource(datasource)
```

- *datasource* (QuoteFeed) - 行情数据源

### fast_trader.strategy.Strategy.get_positions

获取持仓列表

```python
fast_trader.strategy.Strategy.get_positions()
```

返回值

- *positions* (list) - 持仓列表

### fast_trader.strategy.Strategy.get_orders

获取委托列表

```python
fast_trader.strategy.Strategy.get_orders()
```

返回值

- *orders* (list) - 委托列表

### fast_trader.strategy.Strategy.get_open_orders

获取未成交委托列表

```python
fast_trader.strategy.Strategy.get_open_orders()
```

返回值

- *open_orders* (list) - 未成交委托列表

### fast_trader.strategy.Strategy.get_trades

获取成交列表

```python
fast_trader.strategy.Strategy.get_trades()
```

返回值

- *trades* (list) - 成交列表

### fast_trader.strategy.Strategy.get_capital

查询账户权益

```python
fast_trader.strategy.Strategy.get_capital()
```

返回值

- *capital_detail* (dict) - 账户权益详情

### fast_trader.strategy.Strategy.on_market_trade

响应逐笔成交推送

```python
fast_trader.strategy.Strategy.on_market_trade(data)
```

- *data* (Message) - 逐笔成交数据

### fast_trader.strategy.Strategy.on_market_snapshot

响应行情快照推送

```python
fast_trader.strategy.Strategy.on_market_snapshot(data)
```

- *data* (Message) - 行情快照数据

### fast_trader.strategy.Strategy.on_market_order

响应逐笔委托推送

```python
fast_trader.strategy.Strategy.on_market_order(data)
```

- *data* (Message) - 逐笔委托数据

### fast_trader.strategy.Strategy.on_market_queue

响应委托队列推送

```python
fast_trader.strategy.Strategy.on_market_queue(data)
```

- *data* (Message) - 委托队列数据

### fast_trader.strategy.Strategy.on_market_index

响应指数行情推送

```python
fast_trader.strategy.Strategy.on_market_index(data)
```

- *data* (Message) - 指数行情数据

### fast_trader.strategy.Strategy.on_order

报单回报

```python
fast_trader.strategy.Strategy.on_order(data)
```

- *data* (Message) - 报单回报数据

### fast_trader.strategy.Strategy.on_trade

成交回报

```python
fast_trader.strategy.Strategy.on_trade(data)
```

- *data* (Message) - 成交回报数据

### fast_trader.strategy.Strategy.on_batch_order_submission

批量委托提交确认

```python
fast_trader.strategy.Strategy.on_batch_order_submission(data)
```

- *data* (Message) - 批量委托提交确认消息

### fast_trader.strategy.Strategy.on_order_cancelation

撤单回报

```python
fast_trader.strategy.Strategy.on_order_cancelation(data)
```

- *data* (Message) - 撤单回报数据

### fast_trader.strategy.Strategy.on_order_cancelation_submission

撤单委托提交确认

```python
fast_trader.strategy.Strategy.on_order_cancelation_submission(data)
```

- *data* (Message) - 撤单委托提交确认消息

### fast_trader.strategy.Strategy.on_compliance_report

风控消息推送

```python
fast_trader.strategy.Strategy.on_compliance_report(data)
```

- *data* (Message) - 风控消息

### fast_trader.strategy.Strategy.buy

买入委托

```python
fast_trader.strategy.Strategy.buy(code, price, quantity)
```

- *code* (str) - 证券交易代码
- *price* (float) - 价格，最多允许两位小数
- *quantity* (int) - 股票数量

### fast_trader.strategy.Strategy.sell

卖出委托

```python
fast_trader.strategy.Strategy.sell(code, price, quantity)
```

- *code* (str) - 证券交易代码
- *price* (float) - 价格，最多允许两位小数
- *quantity* (int) - 股票数量

### fast_trader.strategy.Strategy.buy_many

批量买入委托

```python
fast_trader.strategy.Strategy.buy_many(orders)
```

- *orders* (list) - 批量委托参数列表，每个元素即为买入委托的参数字典

### fast_trader.strategy.Strategy.sell_many

批量卖出委托

```python
fast_trader.strategy.Strategy.sell_many(orders)
```

- *orders* (list) - 批量委托参数列表，每个元素即为买入委托的参数字典

### fast_trader.strategy.Strategy.cancel_order

撤单委托

```python
fast_trader.strategy.Strategy.cancel_order(exchange, order_exchange_id)
```

- *exchange* (Enum) - 交易所代码
- *order_exchange_id* (int) - 交易所报单编号

### fast_trader.strategy.get_strategy_instance

- *UserStrategy* (class) 用户策略类

返回值

- *strategy* (Strategy) - 用户策略实例