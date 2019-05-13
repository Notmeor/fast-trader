import os
import datetime
import threading
import yaml
import logging.config

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fast_trader.models import StrategyLogModel, Base


def load_config(path=None):
    if path is None:
        path = os.getenv('FAST_TRADER_CONFIG')
    if path is None:
        dirname = os.path.dirname(__file__)
        path = os.path.join(dirname, 'config.yaml')
    with open(path, 'r', encoding='UTF-8') as f:
        conf = yaml.load(f)
    return conf


class Settings:

    def __init__(self):
        self._lock = threading.Lock()
        self._default_settings = self._format(load_config())
        self._custom_settings = self._default_settings.copy()
    
    def _format(self, conf):
        conf_str = str(conf).replace(
            '{fast_trader_home}', os.getenv('FAST_TRADER_HOME')).replace(
            '{today_str}', datetime.date.today().strftime('%Y%m%d'))
        conf_str_raw = rf'{conf_str}'
        ret = eval(conf_str_raw)
        return ret

    def set(self, config):
        assert isinstance(config, dict)
        with self._lock:
            self._custom_settings.update(config)

    def __getitem__(self, key):
        return self._custom_settings[key]

    def get(self, k, d=None):
        return self._custom_settings.get(k, d)

    def copy(self):
        return self._custom_settings.copy()

    def __repr__(self):
        return repr(self._default_settings)


settings = Settings()


def ensure_directories():

    def _mkdir(path):
        if not os.path.exists(path):
            os.makedirs(path)

    working_dir = os.getenv('FAST_TRADER_HOME')
    _mkdir(working_dir)

    logging_dir = os.path.join(working_dir, 'logs')
    _mkdir(logging_dir)

    strategy_dir = settings['strategy_directory']
    _mkdir(strategy_dir)


ensure_directories()

engine = None
Session = None


def config_sqlalchemy():
    global engine
    global Session
    uri = settings['sqlalchemy_url']
    engine = create_engine(uri)
    Session = sessionmaker(bind=engine)
    Base.metadata.create_all(bind=engine)


config_sqlalchemy()


class SqlLogHandler(logging.Handler):

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        self.setFormatter(formatter)

    def emit(self, record):
        try:
            msg = self.format(record)
            session = Session()

            item = StrategyLogModel()
            item.msg = msg

            session.add(item)
            session.commit()
        except Exception:
            self.handleError(record)


def setup_logging():
    try:
        logging.config.dictConfig(settings['logging'])
    except:
        logging.basicConfig(level=logging.INFO)

setup_logging()

