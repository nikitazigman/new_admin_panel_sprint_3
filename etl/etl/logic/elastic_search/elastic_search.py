from etl.settings.settings import ESSettings
import json
from typing import Any
from loguru import logger
from etl.logic.transform.dataclasses import ESBulk
from etl.logic.backoff.backoff import etl_es_backoff
from abc import ABC, abstractmethod
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk as es_bulk


class ElasticSearchLoaderInt(ABC):
    @abstractmethod
    def __init__(self, settings: ESSettings) -> None:
        ...

    @abstractmethod
    def define_schema(self) -> None:
        ...

    @abstractmethod
    def load_bulk(self, bulk: ESBulk) -> None:
        ...


class ElasticSearchLoader:
    def __init__(self, settings: ESSettings) -> None:
        self.settings = settings
        host = f"https://{settings.host}:{settings.port}"
        self.es_client = Elasticsearch(
            hosts=host,
            basic_auth=(settings.user, settings.password),
            verify_certs=False,
        )

    @etl_es_backoff()
    def define_schema(self) -> None:
        logger.info("loading ES schema")

        index_schema: dict[str, Any]
        with open(self.settings.path_to_schema, "r") as file:
            index_schema = json.load(file)

        self.es_client.options(ignore_status=400).indices.create(
            index="movies", **index_schema
        )

    @etl_es_backoff()
    def load_bulk(self, bulk: ESBulk) -> None:
        es_bulk(self.es_client, actions=bulk.to_actions())
        logger.info("Successfully loaded bulk")
