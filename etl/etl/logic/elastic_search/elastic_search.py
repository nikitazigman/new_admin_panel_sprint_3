import requests
from etl.settings.settings import ESSettings
import json
from typing import Any
from loguru import logger
from etl.logic.transform.dataclasses import ESBulk


class ElasticSearchLoader:
    def __init__(self, settings: ESSettings) -> None:
        self.settings = settings
        self.session = requests.session()

    def get_base_url(self) -> str:
        return f"http://{self.settings.host}:{self.settings.port}"

    def define_schema(self) -> None:
        logger.info("loading ES schema")

        index_schema: dict[str, Any]

        with open(self.settings.path_to_schema, "r") as file:
            index_schema = json.load(file)

        url = f"{self.get_base_url()}/{self.settings.index}"
        result = self.session.put(url=url, json=index_schema)

        if result.status_code == 400:
            error_msg = result.json()
            error_type = error_msg["error"]["type"]
            if error_type != "resource_already_exists_exception":
                raise RuntimeError("Cannot load schema to the ES")

        logger.info("ES schema already exists")

    def load_bulk(self, bulk: ESBulk) -> None:
        url = f"{self.get_base_url()}/_bulk"

        result = self.session.post(
            url=url,
            headers={"Content-Type": "application/x-ndjson"},
            data=bulk.to_bulk_request(),
        )

        if result.status_code // 100 == 2:
            logger.info("Successfully loaded bulk")
            return

        logger.error(f"Something went wrong got {result.status_code} status code")
