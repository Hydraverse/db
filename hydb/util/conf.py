import os
import yaml
from attrdict import AttrDict

from hydra import log


class Config:
    ENV_BASE = "HYVE_HOME"
    APPS_GRP = "hyve"
    APP_FILE = "db.yml"
    DIR_APPS = ".local"

    DIR_BASE = os.path.abspath(os.getenv(ENV_BASE, os.getenv("HOME", os.getcwd())))
    APP_BASE = os.path.join(DIR_BASE, DIR_APPS, APPS_GRP)
    APP_CONF = os.path.join(APP_BASE, APP_FILE)

    DEFAULT = AttrDict()

    @staticmethod
    def get(cls: type, defaults=False, save_defaults=False) -> AttrDict:
        conf = AttrDict(Config.read(create=True).get(
            cls.__name__, Config.DEFAULT[cls.__name__]
        ))

        if defaults:
            defaults = Config.DEFAULT[cls.__name__]
            updated = False

            for k, v in defaults.items():
                if k not in conf:
                    updated = True
                    conf[k] = v

            if updated and save_defaults:
                Config.set(cls, conf)

        return conf

    @staticmethod
    def set(cls: type, data: dict) -> None:
        curr_data = Config.read(create=True)
        curr_data[cls.__name__] = data
        Config.write(curr_data)

    @staticmethod
    def defaults(cls):
        """Decorator to include cls.CONF in global default.
        """
        cls.CONF = AttrDict(cls.CONF)
        Config.DEFAULT[cls.__name__] = cls.CONF
        return cls

    @staticmethod
    def exists() -> bool:
        return os.path.isfile(Config.APP_CONF)

    @staticmethod
    def read(create: bool = False) -> AttrDict:

        if not Config.exists():
            if not create:
                raise FileNotFoundError(Config.APP_CONF)

            Config.write(Config.DEFAULT, create=True)
            return Config.DEFAULT

        with open(Config.APP_CONF, "r") as conf:
            data = yaml.safe_load(conf)

            if data is None:
                if create:
                    data = Config.DEFAULT
                    Config.write(data)
                else:
                    return AttrDict()

            return AttrDict(data)

    @staticmethod
    def write(data: dict, create: bool = True) -> None:
        if not Config.exists():
            if not create:
                raise FileNotFoundError(Config.APP_CONF)

            log.debug(f"create: {Config.APP_CONF}")
            os.makedirs(Config.APP_BASE, exist_ok=True)

        with open(Config.APP_CONF, "w") as conf:
            yaml.dump(
                stream=conf,
                data={
                    k: dict(v) for k, v in data.items()
                }
            )

