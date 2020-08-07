import json
import logging
import os
import re
from collections import ChainMap

import requests

logger = logging.getLogger("flask.app.main")


class ConfigError(Exception):
    pass


class BaseConfig:
    APP_NAME = None
    CONFIG_SERVER_URL = os.getenv('CONFIG_SERVER_URL')
    ENV = os.getenv('ENV')
    LABEL = os.getenv('LABEL')

    STATUS_DOC = {
        "start_timestamp": "",
        "fetch_id": "",
        "tenant_id": "",
        "import_type": "",
        "total_records": 0,
        "error": "",
        "result": "",
        "end_timestamp": "",
        "status": "",
        "notify": [],
        "child_imports": []
    }

    NOTIFY_DOC = {
        "function": "",
        "function_index": -1,
        "error": ""
    }

    base_es_config = None

    v = None

    def __init__(self):
        self.get_config()

    def __str__(self):
        val = [dict(base.__dict__) for base in self.__class__.__bases__]
        val.append(dict(self.__class__.__dict__))
        val = ChainMap(*val)
        return ", ".join([f"{k}: {v}" for k, v in val.items() if "__" not in k])

    def get_config(self):
        """Pull config from spring cloud config server"""
        response = requests.get(f"{self.CONFIG_SERVER_URL}/{self.APP_NAME}/{self.ENV}/{self.LABEL}")
        if response.status_code != 200:
            raise ConfigError(f"Unable to access config server. {response.text}")
        app_config = response.json()

        self.v = self.patch_config(app_config)

    @staticmethod
    def patch_config(app_config):
        # it seems the list is in priority order, merge them into a single dict
        config_list: list = app_config['propertySources'][::-1]
        master_config = dict(ChainMap(*[i["source"] for i in config_list]))

        # look up the value in our master config, and replace in the config value
        def repl(match_obj):
            try:
                return str(master_config[match_obj.group(1)])
            except KeyError as e:
                raise ConfigError(e)

        # now we need to find any ${} in the values, look them up in our master dict and path the string
        pattern = r'\$\{(.*?)\}'
        for k, v in master_config.items():
            master_config[k] = re.sub(pattern, repl, str(v))

        return master_config


def build_gateway(gateway_url, header):
    return {
        'gateway_url': gateway_url,
        'header': header
    }


def build_auth_config(application_name, gateway_url, clientId, clientSecret, grantType, scope):
    return {
        "application_name": application_name,
        "gateway_url": gateway_url,
        "auth_body": bytes(json.dumps({
            "clientId": clientId,
            "clientSecret": clientSecret,
            "grantType": grantType,
            "scope": scope
        }), "utf-8"),
        "header": {'Content-Type': 'application/json; charset=utf-8',
                   "apikey": clientId}
    }


def get_bearer_token(config):
    logger.debug(config)
    result = requests.post(url=config["gateway_url"],
                           data=config["auth_body"],
                           headers=config["header"])
    logger.debug(result.text)
    return result.json()
