## 持仓状态

```python
class Instrument(object):
    # 当前持仓
    position = 0
    # 昨日最终持仓
    yd_last_position = 0
    # 昨日持仓均价
    yd_avg_price = 0
```

维护昨日持仓状态，与今日成交列表
