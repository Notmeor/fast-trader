import decimal
import collections
import threading
import time

from ipywidgets import interact, Layout
import ipywidgets as widgets

from data_provider.datafeed.universe import Universe

def get_stock_name(code):
    if not hasattr(get_stock_name, '_pairs'):
        ipo_info = Universe().get_all_ipo_info()
        pairs = ipo_info[['name', 'ticker']].set_index(
            'ticker')['name'].to_dict()
        get_stock_name._pairs = pairs

    if '.' not in code:
        if code.startswith('6'):
            code += '.SH'
        else:
            code += '.SZ'
    try:
        return get_stock_name._pairs[code]
    except KeyError:
        return '未知代码'

def create_trading_panel(ea):
    stock_code = widgets.Text(description='股票代码', continuous_update=True)
    stock_name = widgets.Text(description='股票名称', continuous_update=False)
    order_price = widgets.BoundedFloatText(description='价格', min=0.01, max=100000,
                                           step=0.01, continuous_update=False)
    stock_quantity = widgets.BoundedIntText(description='数量', min=100, max=1000000000000,
                                            step=100, continuous_update=False)
    order_side = widgets.Dropdown(description='买卖方向', continuous_update=False)
    order_side.options = [('买入', 1), ('卖出', 2)]

    confirm_btn = widgets.Button(description='下单')

    out = widgets.Output()

    panel = box = widgets.Box([stock_code, stock_name, order_price,
                       stock_quantity, order_side, confirm_btn, out])

    def fill_stock_name(change):
        try:
            value = change['new']
            if len(value) >= 6:
                name = get_stock_name(value)
                stock_name.value = name
        except Exception as e:
            with out:
                print(e)

    def insert_order(b):
        with out:
            try:
                code = box.children[0].value
                price = box.children[2].value
                quantity = box.children[3].value
                order_side = box.children[4].value

                err_msg = None
                if price:
                    if decimal.Decimal(str(price)).as_tuple().exponent < -2:
                        err_msg = '最多保留小数点后两位！'

                if err_msg is not None:
                    raise Exception(err_msg)

                if order_side == 1:
                    ret = ea.buy(code, price, quantity)
                else:
                    ret = ea.sell(code, price, quantity)

            except Exception as e:
                with out:
                    display(HTML('<p style=color:red>{}<p>'.format(e)))

            else:
                pass

    confirm_btn.on_click(insert_order)

    stock_code.observe(fill_stock_name, names='value')

    confirm_btn.layout = Layout(
        width='212px',
        margin='10px 0px 10px 90px')

    panel.layout = Layout(
        display='flex',
        flex_flow='column',
        align_items='stretch',
        border=None,
        width='40%')

    return panel

account_label_dict = collections.OrderedDict([
    ('账户', 'account_no'),
    ('余额', 'balance'),
    ('可用资金', 'available'),
    ('冻结金额', 'freeze'),
    ('证券市值', 'securities'),
    ('总资产', 'total')
])

trade_label_dict = collections.OrderedDict([
    ('证券代码', 'code'),
    ('证券名称', 'name'),
    ('交易方向', 'order_side'),
    ('成交价格', 'fill_price'),
    ('成交数量', 'fill_quantity'),
    ('状态', 'fill_status'),
    ('成交时间', 'fill_time'),
    ('成交金额', 'fill_amount'),
    ('清算资金', 'clear_amount'),
    ('客户委托编号', 'order_original_id'),
    ('交易所委托编号', 'order_exchange_id'),
    ('交易所成交编号', 'fill_exchange_id'),
    ('交易所', 'exchange')])
        
order_label_dict = collections.OrderedDict([
    ('证券代码', 'code'),
    ('证券名称', 'name'),
    ('委托价格', 'price'),
    ('委托数量', 'quantity'),
    ('已成交数量', 'total_fill_quantity'),
    ('交易方向', 'order_side'),
    ('报价类型', 'order_type'),
    ('状态', 'status_message'),
    ('委托时间', 'order_time'),
    ('冻结资金', 'freeze_amount'),
    ('交易所委托编号', 'order_exchange_id'),
    ('客户委托编号', 'order_original_id'),
    ('交易所', 'exchange')])

# position_label_dict = collections.OrderedDict([
#     ('证券代码', 'code'),
#     ('证券名称', 'name'),
#     ('交易所', 'exchange'),
#     ('持仓', 'balance'),
#     ('可用数量', 'available_quantity'),
#     ('冻结数量', 'freeze_quantity'),
#     ('买入数量', 'buy_quantity'),
#     ('卖出数量', 'sell_quantity'),
#     ('市值', 'market_value'),
#     ('成本价', 'cost')])

position_label_dict = collections.OrderedDict([
    ('证券代码', 'code'),
    ('证券名称', 'name'),
    ('持仓', 'quantity'),
    ('成本价', 'cost_price'),
    ('可用数量', 'available_quantity'),
    ('冻结数量', 'freeze_quantity'),
    ('买入数量', 'buy_quantity'),
    ('卖出数量', 'sell_quantity'),
    ('市值', 'market_value'),
    ('交易所', 'exchange')])

unit_cls = widgets.Button
# unit_cls = widgets.Label

# ATTR = 'value'
ATTR = 'description'

def apply_style(**kwargs):
    def decorator(func):
        def wrapper(*args, **kw):
            obj = func(*args, **kw)
            for k, v in kwargs.items():
                setattr(obj.layout, k, v)
            return obj
        return wrapper
    return decorator

class Dashboard(object):
    


    def __init__(self, ea):
        
        self._running = False
        self.ea = ea
        
        self.trading_panel = None
        self.account_panel = None
        self.trade_panel = None
        self.order_panel = None
        self.position_panel = None

    def is_alive(self):
        return self._running

    @staticmethod
    def has_item(panel, ident):
        for child in panel.children:
            if child.ident == ident:
                return True
        return False
    
    @staticmethod
    def get_item(panel, ident):
        for child in panel.children:
            if child.ident == ident:
                return child
        return None

    @staticmethod
    def create_panel(label_dict):
        headers = [unit_cls(**{ATTR: label, 'button_style': ''})
                   for label in label_dict]
        [setattr(el.style, 'font_weight', 'bold') for el in headers]
        header_box = widgets.HBox(headers)
        header_box.ident = 'header'
#         placeholder = widgets.HTML('<hr>')
#         placeholder.ident = 'placeholder'
#         panel = widgets.VBox([header_box, placeholder])
        panel = widgets.VBox([header_box])
        return panel

    @staticmethod
    def add_panel_entry(panel, label_dict, entry, ident):
        def to_widget(entry):
            item = widgets.HBox([unit_cls(**{ATTR: str(entry.get(v, '-')),
                                             'style': {'button_color': 'white'}})
                                 for v in label_dict.values()])
            return item
        item = to_widget(entry)
        item.ident = ident
        panel.children = (*panel.children[:1], item, *panel.children[1:])

    @staticmethod
    def update_panel_entry(panel, label_dict, entry, ident, upsert=True):
        
        if upsert:
            if not Dashboard.has_item(panel, ident):
                Dashboard.add_panel_entry(panel, label_dict, entry, ident)
                return
                
        fields = list(label_dict.values())
        for child in panel.children:
            if child.ident == ident:
                for i, el in enumerate(child.children):
                    key = fields[i]
                    if key in entry:
                        setattr(el, ATTR, str(entry[key]))
                break
    
    @apply_style(width='fit-content')
    def create_account_panel(self):
        self.account_panel = self.create_panel(account_label_dict)
        return self.account_panel
    
    def update_account_panel(self, entry):
        ident = entry['account_no']
        self.update_panel_entry(
            self.account_panel,
            account_label_dict,
            entry=entry,
            ident=ident)

    def refresh_capital(self):
        capital = self.ea.get_capital()
        self.update_account_panel(capital)
    
    @apply_style(width='fit-content')
    def create_trade_panel(self):
        self.trade_panel = self.create_panel(trade_label_dict)
        return self.trade_panel

    def update_trade_panel(self, entry):
        ident = entry['order_original_id']
        self.update_panel_entry(
            self.trade_panel,
            trade_label_dict,
            entry=entry,
            ident=ident)
        
    def refresh_trade_panel(self, keep=20):
        trades = self.ea.get_trades()[-keep:]
        for trade in trades:
            ident = trade['order_original_id']
            if self.has_item(self.trade_panel, ident):
                continue
            self.update_trade_panel(trade)
        
        if keep > 0 and len(self.trade_panel.children) - 1 > keep:
            self.trade_panel.children = self.trade_panel.children[:1] +\
                self.trade_panel.children[-20:] 

    @apply_style(width='fit-content')
    def create_position_panel(self):
        self.position_panel = self.create_panel(position_label_dict)
        return self.position_panel

    def update_position_panel(self, entry):
        ident = entry['code']
        self.update_panel_entry(
            self.position_panel,
            position_label_dict,
            entry=entry,
            ident=ident)
        
    def refresh_position_panel(self):
        positions = self.ea.get_positions()
        for pos in positions:
            ident = pos['code']
#             if self.has_item(self.position_panel, ident):
#                 continue
            self.update_position_panel(pos)
    
    @apply_style(width='fit-content')
    def create_order_panel(self):
        self.order_panel = self.create_panel(order_label_dict)
        return self.order_panel

    def update_order_panel(self, entry):
        ident = entry['order_original_id']
        self.update_panel_entry(
            self.order_panel,
            order_label_dict,
            entry=entry,
            ident=ident)
        
    def refresh_order_panel(self):
        orders = self.ea.get_open_orders()[-20:]
        valid_idents = ['header']
        for order in orders:
            ident = order['order_original_id']
            valid_idents.append(ident)
            item = self.get_item(self.order_panel, ident)
            if item is not None:
                ind = list(order_label_dict.values()).index('freeze_amount')
                if item.children[ind] == str(order['freeze_amount']):
                    continue
            self.update_order_panel(order)
        
#         children_to_remove = []
#         for child in self.order_panel.children[1:]:
#             if child.ident not in valid_idents:
#                 children_to_remove.append(child)
        self.order_panel.children = [child for child in self.order_panel.children 
                                     if child.ident in valid_idents]

    def run(self):
        self._running = True
        while self._running:

            time.sleep(0.5)
            if self.account_panel:
                self.refresh_capital()
            if self.trade_panel:
                self.refresh_trade_panel()
            if self.position_panel:
                self.refresh_position_panel()
            if self.order_panel:
                self.refresh_order_panel()
    
    def refresh_dashboard(self):
        self._thread = threading.Thread(target=self.run)
        self._thread.start()
            