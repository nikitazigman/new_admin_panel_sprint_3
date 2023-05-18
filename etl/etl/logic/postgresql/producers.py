from datetime import datetime
from typing import Generator
from uuid import UUID

from loguru import logger
from psycopg2._psycopg import connection

from etl.logic.postgresql.interfaces import ProducerInt


class BaseProducer(ProducerInt):
    batch_size = 30
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

    def get_ids(
        self, last_checkup: datetime | None
    ) -> Generator[list[UUID], None, None]:
        logger.debug("Getting modified ids from the last checkup")

        query = self.get_query()
        with self.connection.cursor() as cursor:
            cursor.execute(query, vars=(last_checkup,))

            while response := cursor.fetchmany(size=self.batch_size):
                logger.debug(f"Retrieved {len(response)} ids from `{self.table}` table")
                yield [res["id"] for res in response]


class PersonProducer(BaseProducer):
    table = "content.person"


class GenreProducer(BaseProducer):
    table = "content.genre"


class FilmWorkProducer(BaseProducer):
    table = "content.film_work"
