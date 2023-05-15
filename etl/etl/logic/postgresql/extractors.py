import psycopg2
from psycopg2.extras import DictCursor

from etl.settings.settings import PGSettings
from typing import Type, Any, Generator
from types import TracebackType
from loguru import logger
from datetime import datetime

from etl.logic.postgresql.interfaces import (
    EnricherInt,
    ExtractorInt,
    MergerInt,
    ProducerInt,
)
from psycopg2._psycopg import connection as PGConnection


class PostgreExtractor(ExtractorInt):
    connection: PGConnection

    film_producer: ProducerInt
    genre_producer: ProducerInt
    person_producer: ProducerInt
    enricher: EnricherInt
    merger: MergerInt

    def __init__(
        self,
        settings: PGSettings,
        film_producer_class: Type[ProducerInt],
        genre_producer_class: Type[ProducerInt],
        person_producer_class: Type[ProducerInt],
        enricher_class: Type[EnricherInt],
        merger_class: Type[MergerInt],
    ) -> None:
        self.settings = settings

        self.film_producer_class = film_producer_class
        self.genre_producer_class = genre_producer_class
        self.person_producer_class = person_producer_class
        self.enricher_class = enricher_class
        self.merger_class = merger_class

    def connect(self) -> None:
        self.connection = psycopg2.connect(
            **self.settings.dict(), cursor_factory=DictCursor
        )
        logger.info("succesfully connected to the Postgre DBMS")

    def disconnect(self) -> None:
        self.connection.close()
        logger.info("succesfully disconnected from the Postgre DBMS")

    def initiate_worker_classes(self) -> None:
        if self.connection is None:
            raise RuntimeError(
                "Method `connect` should be called"
                " before method `initiate_worker_classes`"
            )

        self.film_producer = self.film_producer_class(self.connection)
        self.genre_producer = self.genre_producer_class(self.connection)
        self.person_producer = self.person_producer_class(self.connection)
        self.enricher = self.enricher_class(self.connection)
        self.merger = self.merger_class(self.connection)

    def __enter__(self) -> "ExtractorInt":
        self.connect()
        self.initiate_worker_classes()
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.disconnect()

    def get_modified_films(
        self, last_checkup: datetime | None = None
    ) -> Generator[list[dict[str, Any]], None, None]:
        logger.info(f"Retieving all films ids from the {last_checkup=}")

        films_ids = self.film_producer.get_ids(last_checkup)
        genre_ids = self.genre_producer.get_ids(last_checkup)
        person_ids = self.person_producer.get_ids(last_checkup)

        modified_films_ids = self.enricher.get_modified_films_ids(
            films_ids, genre_ids, person_ids
        )

        logger.info("Merging all related data to the modified films")
        yield from self.merger.get_films_data(modified_films_ids)
