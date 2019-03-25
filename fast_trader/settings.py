import os
import threading
import yaml


def load_config(path=None):
    if path is None:
        path = os.getenv('FAST_TRADER_CONFIG')
    if path is None:
        dirname = os.path.dirname(__file__)
        path = os.path.join(dirname, 'config.yaml')
    with open(path, 'r') as f:
        conf = yaml.load(f)
    return conf


class Settings:

    def __init__(self):
        self._lock = threading.Lock()
        self._default_settings = load_config()
        self._custom_settings = self._default_settings.copy()

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
