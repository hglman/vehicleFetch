import logging

import certifi
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

logger = logging.getLogger("flask.app.connector.elasticsearch")


def build_es_config(index, url, port, username, password):
    return {
        "index": index,
        "url": url,
        "port": port,
        "password": password,
        "username": username
    }


class ElasticSearch:

    def __init__(self, config):
        self.index = config["index"]
        self.conn = self.get_conn(config)

    @staticmethod
    def get_conn(config):
        if config["password"] != "" and config["username"] != "":
            return Elasticsearch([{'host': config["url"], 'port': config["port"]}],
                                 use_ssl=True,
                                 ca_certs=certifi.where(),
                                 http_auth=(config["username"], config["password"]))
        if not (config.es_password == "" and config.es_username == ""):
            raise Exception("The username and password for elasticSearch must either be both included or excluded,"
                            " only one is set at current")
        else:
            return Elasticsearch([config["url"]],
                                 use_ssl=True,
                                 ca_certs=certifi.where())

    def get(self, id_):
        try:
            logger.debug(f"get {id_}, {self.index}")
            result = self.conn.get(index=self.index, id=id_, doc_type="_doc")
            logger.debug(result)
        except NotFoundError:
            result = None
        finally:
            return result

    def insert(self, data, id_):
        logger.debug(f"insert {data}, {id_}, {self.index}")
        result = self.conn.create(index=self.index, id=id_, doc_type="_doc", body=data)
        logger.debug(result)

    def update(self, data, id_):
        logger.debug(f"update {data}, {id_}, {self.index}")
        result = self.conn.update(index=self.index, id=id_, doc_type="_doc", body={"doc": data})
        logger.debug(result)

    def delete(self, data, id_):
        logger.debug(f"delete {data}, {id_}, {self.index}")
        result = self.conn.delete(index=self.index, id=id_, doc_type="_doc", body=data)
        logger.debug(result)

    def search(self, query):
        logger.debug(f"search {query}, {self.index}")
        result = self.conn.search(index=self.index, body=query)
        logger.debug(result)
        return result
