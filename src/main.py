import asyncio
import base64
import hashlib
import json
import logging
import sys
import time
import traceback
from copy import deepcopy
from functools import partial
from os import wait
from threading import Thread
from uuid import uuid4, UUID
from xml.etree import ElementTree

import aiohttp
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from flask import jsonify

from config import Config
from fetch import flask_setup, ImportCycle
from fetch.connectors.elasticsearch import ElasticSearch
from fetch.fetch_config import get_bearer_token
from fetch.setup import make_aiohttp_app

config = Config()
app = flask_setup(config.v['log_level'], config.APP_NAME)
logger = logging.getLogger("flask.app.main")


@app.route("/")
def root():
    return "Vehicle Fetch"


@app.route("/health")
def health():
    try:
        es = ElasticSearch(config.base_es_config)
        es.search("""{
          "size": 0, 
          "query": {
            "match_all": {}
          }
        }""")
        return jsonify({
            "status": "UP"
        })
    except Exception:
        return jsonify({'status': 'DOWN'})


@app.errorhandler(404)
def not_found(e):
    return jsonify({"serviceCode": 1040, "serviceMessage": f"Page Not Found, {e}"}), 404


@app.route("/import/vehicles/<uuid:tenant_id>", methods=["POST"])
def import_vehicle(tenant_id):
    cycle = None

    async def run():
        b_token = f"Bearer {get_bearer_token(config.auth_config)['accessToken']}"

        vqwc = deepcopy(config.vehicle_query_wsapi_config)
        vqwc["header"]["Authorization"] = b_token
        logger.debug(vqwc)

        es = ElasticSearch(config.es_raw_config)

        tsc = deepcopy(config.tenant_service_config)
        tsc["header"]["Authorization"] = b_token

        async with aiohttp.ClientSession() as session:
            get_ = partial(get, tenant_id, tsc, config.v['app_config.key'])
            store_ = partial(store, vqwc, session, es)

            nonlocal cycle
            cycle = ImportCycle("vehicle", tenant_id, config, get_, store_, notify)

            await cycle.run()

    try:
        def run_loop(loop):
            loop.run_until_complete(run())
            loop.close()

        loop = asyncio.new_event_loop()
        t = Thread(target=run_loop, args=(loop, ))
        t.start()

        while cycle is None or cycle.fetch_id is None:
            time.sleep(1)

        return jsonify({"serviceCode": None, "serviceMessage": None,
                        "content": {"fetch_id": cycle.fetch_id, "Status": cycle.status}})
    except Exception:
        error = repr(traceback.format_exception(*sys.exc_info()))
        logger.error(error)
        return jsonify({"serviceCode": 1050,
                        "serviceMessage": "Critical Error, FAILURE"}), 500


@app.route("/import/status/<uuid:fetch_id>")
def status(fetch_id):
    app.logger.info(f"get status for fetch_id: {fetch_id}")
    es = ElasticSearch(config.base_es_config)
    result = es.get(fetch_id)
    app.logger.debug(result)

    try:
        if result is None or result["found"] is not True:
            return jsonify({"serviceCode": 1030,
                            "serviceMessage": "fetch_id {fetch_id} not found"}), 404
        else:
            return jsonify({"serviceCode": None, "serviceMessage": None,
                            "content": result["_source"]}), 200
    except Exception:
        error = repr(traceback.format_exception(*sys.exc_info()))
        logger.error(error)
        return jsonify({"serviceCode": 1050,
                        "serviceMessage": "Critical Error, FAILURE"}), 500


async def make_vehicle(xml, customer_id, tenant_id):
    """
    vehicle is our json object to load
    """

    # INTERNAL MODEL
    vehicle = {
        "vin": None,
        "licensenumber": None,
        "busNumber": None,
        "deviceId": None,
        "manufacturer": None,
        "model": "unknown",
        "tenantId": None,
        "assetId": None,
        "customerId": None,
        "deviceModel": "zonar",
        "deviceBrand": "zonar",
        "RecordStatus": None
    }

    # CLIENT MAPPING
    zonar_mapping = {
        "vin": "vin",
        "name": "licensenumber",
        "exsid": "busNumber",
        "mfg": "manufacturer",
        "opstatus": "status",
        "gps": "deviceId",
        "status": "zonarRecordStatus"
    }

    assets = ElementTree.fromstring(xml)
    if assets.tag == "assetlist":
        for asset in assets:
            curr = deepcopy(vehicle)
            curr["assetId"] = asset.attrib["id"]
            curr["customerId"] = customer_id
            curr["tenantId"] = tenant_id
            for elem in asset:
                if elem.tag in zonar_mapping:
                    curr[zonar_mapping[elem.tag]] = elem.text
            yield curr


async def get_vehicle_info(tenant_id, session, tsc, key):
    async with session.get(f"{tsc['gateway_url']}/{str(tenant_id)}", headers=tsc["header"]) as response:
        data = await response.json()

        if response.status != 200 or data["serviceCode"] or data["content"] is None:
            raise Exception("could not get tenant data", data)

    key = bytearray(key, 'UTF-8')
    sha1 = hashlib.sha1()
    sha1.update(key)
    key = sha1.digest()[:16]
    cipher = Cipher(algorithms.AES(key), modes.ECB(), backend=default_backend())

    customers = []
    for record in data["content"]["Integrations"]:
        decrypt = cipher.decryptor()
        password = decrypt.update(base64.b64decode(bytearray(record["Password"], 'UTF-8')))
        password += decrypt.finalize()
        password = password.decode('UTF-8')
        password = password.strip('x\03')

        result = {
            "customer_id": record["CustomerId"],
            "host_name": record["HostName"],
            "password": password,
            "username": record["Username"],
        }
        customers.append(result)

    return customers


async def get(tenant_id, tsc, key):
    async with aiohttp.ClientSession() as session:
        customers = await get_vehicle_info(tenant_id, session, tsc, key)
        for customer in customers:
            params = {"operation": "showassets", "format": "xml", "action": "showopen",
                      "customer": customer["customer_id"]}
            auth = aiohttp.BasicAuth(customer["username"], customer["password"])
            logger.debug(f"get next customer {customer['host_name']} {params}")

            async with session.get(f"{customer['host_name']}/interface.php", auth=auth, params=params) as resp:
                xml = await resp.text()

            logger.log(1, f"xml from {customer['customer_id']}, {xml}")
            async for data in make_vehicle(xml, customer["customer_id"], tenant_id):
                yield data, 1


async def store(vqwc, session, es, data, fetch_id):
    class UUIDEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, UUID):
                # if the obj is uuid, we simply return the value of uuid
                return str(obj)
            return json.JSONEncoder.default(self, obj)

    # store raw data with fetch id and then remove
    data['fetch_id'] = fetch_id

    # store raw data
    es.insert(data, uuid4())
    del data['fetch_id']

    try:
        async with session.put(vqwc["gateway_url"], data=json.dumps(data, cls=UUIDEncoder),
                               headers=vqwc["header"]) as response:
            body = await response.text()
            logger.debug(f"vehicleQueryWSAPI status: {response.status} response: {response.__dict__}")
            # 400 is for bad data, that should not halt the import
            if response.status not in (200, 400):
                raise Exception(f'bad reponse: {response.status}, {body}')
            return {'status': response.status, 'body': body}
    except (OSError, RuntimeError) as e:
        error = repr(traceback.format_exception(*sys.exc_info()))
        logger.error(error)
        raise e


async def notify(status):
    pass


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
else:
    aioapp = make_aiohttp_app(app)
