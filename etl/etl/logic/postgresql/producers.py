from uuid import UUID
from datetime import datetime
from etl.logic.postgresql.interfaces import ProducerInt
from loguru import logger
from psycopg2._psycopg import connection


class BaseProducer(ProducerInt):
    table: str | None = None  # should be redifined in chiled classes

    def __init__(self, pg_connection: connection) -> None:
        self.connection = pg_connection

    def get_query(self) -> str:
        query = f"""
        --sql
        SELECT id FROM {self.table}
        WHERE modified > (%s)
        ORDER BY modified
        ;
        """
        return query

    def get_ids(self, last_checkup: datetime | None) -> list[UUID]:
        logger.debug("Getting modified persons ids from the last checkup")

        ids = []
        query = self.get_query()
        with self.connection.cursor() as cursor:
            cursor.execute(query, vars=(last_checkup,))
            ids = [res["id"] for res in cursor.fetchall()]

        return ids


class PersonProducer(BaseProducer):
    table = "content.person"


class GenreProducer(BaseProducer):
    table = "content.genre"


class FilmWorkProducer(BaseProducer):
    table = "content.film_work"
