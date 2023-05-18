import json
from http import HTTPStatus
from typing import Any

import requests
from loguru import logger

from etl.logic.backoff.backoff import etl_es_backoff
from etl.logic.transform.dataclasses import ESBulk
from etl.settings.settings import ESSettings


class ElasticSearchLoader:
    def __init__(self, settings: ESSettings) -> None:
        self.settings = settings
        self.session = requests.session()

    def get_base_url(self) -> str:
        return f"http://{self.settings.host}:{self.settings.port}"

    @etl_es_backoff()
    def define_schema(self) -> None:
        logger.info("loading ES schema")

        index_schema: dict[str, Any]

        with open(self.settings.path_to_schema, "r") as file:
            index_schema = json.load(file)

        url = f"{self.get_base_url()}/{self.settings.index}"
        result = self.session.put(url=url, json=index_schema)

        if result.status_code == HTTPStatus.BAD_REQUEST:
            error_msg = result.json()
            error_type = error_msg["error"]["type"]
            if error_type != "resource_already_exists_exception":
                raise RuntimeError("Cannot load schema to the ES")

        logger.info("ES schema already exists")

    @etl_es_backoff()
    def load_bulk(self, bulk: ESBulk) -> None:
        url = f"{self.get_base_url()}/_bulk"

        result = self.session.post(
            url=url,
            headers={"Content-Type": "application/x-ndjson"},
            data=bulk.to_bulk_request(),
        )

        if result.status_code == HTTPStatus.OK:
            logger.info("Successfully loaded bulk")
            return

        logger.error(f"Something went wrong got {result.status_code} status code")
