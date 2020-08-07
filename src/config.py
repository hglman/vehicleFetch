from copy import deepcopy

from fetch import BaseConfig
from fetch.connectors.elasticsearch import build_es_config
from fetch.connectors.eureka import build_eureka_config, register_eureka
from fetch.fetch_config import build_gateway, build_auth_config


class Config(BaseConfig):
    APP_NAME = "VehicleFetch"

    def __init__(self):
        super().__init__()

        self.eureka_config = build_eureka_config(
            app_name=self.APP_NAME,
            hostname=f"{self.v['eureka.hostname']}",
            service_port=self.v["eureka.service_port"],
        )
        self.eureka_service = register_eureka(self.eureka_config)

        self.auth_config = build_auth_config(
            application_name=self.APP_NAME,
            gateway_url=f"{self.v['gateway.url']}/api/v1/signin",
            clientId=self.v['auth.clientId'],
            clientSecret=self.v['auth.clientSecret'],
            grantType=self.v['auth.grantType'],
            scope=self.v['auth.scope']
        )

        self.base_es_config = build_es_config(
            index=self.v['elastic_search.status_index'],
            url=self.v['elastic_search.url'],
            port=int(self.v['elastic_search.port']),
            password=self.v['elastic_search.password'],
            username=self.v['elastic_search.username']
        )

        self.es_raw_config = deepcopy(self.base_es_config)
        self.es_raw_config["index"] = self.v["elastic_search.raw_index"]

        self.vehicle_query_wsapi_config = build_gateway(
            gateway_url=f"{self.v['gateway.url']}/api/v4.0/vehicles/assetId",
            header={
                "Authorization": None,
                'Content-Type': 'application/json; charset=utf-8',
                'apikey': self.v["auth.clientId"]
            }
        )

        self.tenant_service_config = build_gateway(
            gateway_url=f"{self.v['gateway.url']}/api/v2.0/tenants/",
            header={
                "Authorization": None,
                'Content-Type': 'application/json; charset=utf-8',
                'apikey': self.v["auth.clientId"]
            }
        )
