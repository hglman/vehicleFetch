import asyncio
import logging
import sys
import traceback
from copy import deepcopy
from datetime import datetime
from enum import Enum
from typing import Dict, Any
from uuid import uuid4

from .connectors.elasticsearch import ElasticSearch
from .fetch_config import BaseConfig

logger = logging.getLogger("flask.app.fetch")


class FetchStatus(Enum):
    FAIL = -1
    RUNNING = 0
    SUCCESS = 1


class ImportCycle:

    def __init__(self, import_type: str, tenant_id, config: BaseConfig, get, store, notify):

        self.config = config
        self.get = get
        self.store = store
        self.notify = notify
        self.es = ElasticSearch(config.base_es_config)
        self.status_doc = deepcopy(config.STATUS_DOC)
        self.fetch_id = self.status_doc["fetch_id"] = uuid4()
        self.status = self.status_doc["status"] = FetchStatus.RUNNING.name
        self.status_doc["tenant_id"] = self.tenant_id = tenant_id
        self.status_doc["import_type"] = import_type
        self.status_doc["start_timestamp"] = datetime.utcnow().isoformat(timespec='seconds')

    async def run(self):
        """
        run the event loop, consume the get list and clean up
        """
        self.es.insert(self.status_doc, self.status_doc["fetch_id"])

        # downstream service cant handle a large number of connections, limit with this
        concurrent_count = int(self.config.v["app_config.concurrent_count"])
        dl_tasks = set()
        event_loop = asyncio.get_event_loop()

        async for data, halt in self._get():
            if data is None or halt:
                raise Exception(f"get failed, halt, error: {data}")

            if len(dl_tasks) >= concurrent_count:
                # Wait for some download to finish before adding a new one
                _done, dl_tasks = await asyncio.wait(dl_tasks, return_when=asyncio.FIRST_COMPLETED)

            dl_tasks.add(event_loop.create_task(self._store(data)))

        # Wait for the remaining downloads to finish
        await asyncio.wait(dl_tasks)

        if self.status == FetchStatus.FAIL.name:
            logger.error(f"Import Failed, {self.status_doc}")
        else:
            self.status = self.status_doc["status"] = FetchStatus.SUCCESS.name

        await self._notify()
        self.es.update(self.status_doc, self.status_doc["fetch_id"])
        return self.status_doc

    async def _get(self) -> (Any, bool):
        """Get source data and emit json data """
        try:
            logger.info(f"Starting run function {repr(self.get)}")

            async for data, count in self.get():
                self.status_doc['total_records'] += count
                logger.debug(f"Get return count: {count} data: {data}")
                yield data, False

        except Exception as e:
            """ Stop the import on failure"""
            logger.error("Exception thrown by run")
            error = repr(traceback.format_exception(*sys.exc_info()))
            logger.error(error)
            self.status_doc["error"] = error
            self.status = self.status_doc["status"] = FetchStatus.FAIL.name
            self.es.update(self.status_doc, self.status_doc["fetch_id"])
            yield error, True

    async def _store(self, data: Dict) -> None:
        """ Accept a json data object and store it where needed"""
        status_doc = deepcopy(self.config.STATUS_DOC)
        status_doc['start_timestamp'] = datetime.utcnow().isoformat(timespec='seconds')
        status_doc['import_type'] = 'data_record'
        status_doc['fetch_id'] = self.fetch_id
        status_doc['total_records'] = 1
        status_doc['tenant_id'] = self.tenant_id
        try:
            logger.info(f"Starting store function {repr(self.store)}")
            result = await self.store(data, self.fetch_id)
            status_doc["result"] = result
            logger.info("Store complete")
            status_doc['status'] = FetchStatus.SUCCESS.name

        except Exception:
            logger.error("Exception thrown by store")
            error = repr(traceback.format_exception(*sys.exc_info()))
            logger.error(error)
            status_doc["error"] = error
            status_doc["status"] = FetchStatus.FAIL.name
            self.status = self.status_doc["status"] = FetchStatus.FAIL.name

        self.status_doc['child_imports'].append(status_doc)

    async def _notify(self) -> None:
        """ dont let errors leak"""
        notify_status = deepcopy(self.config.NOTIFY_DOC)
        notify_status["function"] = repr(self.notify)

        try:
            logger.info(f"starting notify function: {repr(self.notify)}")
            await self.notify(self.status_doc)
            logger.info(f"notify function complete")

        except Exception:
            """Keep looping on failure of one notify"""
            logger.error(f"Exception thrown by notify function, halting")
            error = repr(traceback.format_exception(*sys.exc_info()))
            logger.error(error)
            notify_status["error"] = error

        finally:
            self.status_doc["notify"].append(notify_status)
