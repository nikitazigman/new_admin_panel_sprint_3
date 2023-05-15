from uuid import UUID
from typing import Generator, Any
from etl.logic.postgresql.interfaces import MergerInt
from psycopg2._psycopg import connection
from loguru import logger


class BaseMerger(MergerInt):
    table = "content.film_work"
    batch_size = 50

    def __init__(self, pg_connection: connection) -> None:
        self.connection = pg_connection

    def get_query(self) -> str:
        query = f"""
        --sql
        SELECT
            fw.id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.created,
            fw.modified,
            ARRAY_AGG (DISTINCT g.name) as genres,
            JSON_AGG (
                DISTINCT jsonb_build_object(
                    'person_role', pfw.role,
                    'person_id', p.id,
                    'person_name', p.full_name
                )
            ) as persons
        FROM {self.table} as fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.id IN %s
        GROUP BY fw.id
        ORDER BY fw.modified
        ;
        """
        return query

    def get_films_data(
        self, films_ids: list[UUID]
    ) -> Generator[list[dict[str, Any]], None, None]:
        query = self.get_query()

        with self.connection.cursor() as cursor:
            cursor.execute(query, vars=(tuple(films_ids),))
            film_data = cursor.fetchmany(size=self.batch_size)
            while film_data:
                logger.debug(
                    f"Retrieved {len(film_data)} rows from `{self.table}` table"
                )
                yield film_data
                film_data = cursor.fetchmany(size=self.batch_size)
