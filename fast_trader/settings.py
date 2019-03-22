import os
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
        self._default_settings = load_config()
        self._custom_settings = self._default_settings.copy()

    def set(self, config):
        assert isinstance(config, dict)
        self._custom_settings.update(config)

    @property
    def _settings(self):
        ret = self._default_settings.copy()
        ret.update(self._custom_settings)
        return ret

    def __getitem__(self, key):
        return self._custom_settings[key]

    def get(self, k, d=None):
        return self._custom_settings.get(k, d)


settings = Settings()
