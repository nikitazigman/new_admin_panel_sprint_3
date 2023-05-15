from abc import ABC, abstractmethod
from uuid import UUID
from typing import Generator, Any, Type
from datetime import datetime
from etl.settings.settings import PGSettings
from types import TracebackType
from psycopg2._psycopg import connection


class PostgreWorkerInt(ABC):
    @abstractmethod
    def __init__(self, pg_connection: connection) -> None:
        ...


class EnricherInt(PostgreWorkerInt):
    @abstractmethod
    def get_modified_films_ids(
        self,
        films_ids: list[UUID],
        genre_ids: list[UUID],
        person_ids: list[UUID],
    ) -> list[UUID]:
        ...


class ProducerInt(PostgreWorkerInt):
    @abstractmethod
    def get_ids(self, last_checkup: datetime | None) -> list[UUID]:
        ...


class MergerInt(PostgreWorkerInt):
    @abstractmethod
    def get_films_data(
        self, films_ids: list[UUID]
    ) -> Generator[list[dict[str, Any]], None, None]:
        ...


class ExtractorInt(PostgreWorkerInt):
    @abstractmethod
    def __init__(
        self,
        settings: PGSettings,
        film_producer_class: Type[ProducerInt],
        genre_producer_class: Type[ProducerInt],
        person_producer_class: Type[ProducerInt],
        enricher_class: Type[EnricherInt],
        merger_class: Type[MergerInt],
    ) -> None:
        ...

    @abstractmethod
    def connect(self) -> None:
        ...

    @abstractmethod
    def disconnect(self) -> None:
        ...

    @abstractmethod
    def __enter__(self) -> "ExtractorInt":
        ...

    @abstractmethod
    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        ...

    @abstractmethod
    def get_modified_films(
        self, last_checkup: datetime | None = None
    ) -> Generator[list[dict[str, Any]], None, None]:
        ...
