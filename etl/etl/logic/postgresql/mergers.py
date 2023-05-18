from typing import Any, Generator
from uuid import UUID

from loguru import logger
from psycopg2._psycopg import connection

from etl.logic.postgresql.interfaces import MergerInt


class BaseMerger(MergerInt):
    table = "content.film_work"
    batch_size = 50

    def __init__(self, pg_connection: connection) -> None:
        self.connection = pg_connection

    def get_query(self, filter: bool = True) -> str:
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
            COALESCE (
                JSON_AGG(
                    DISTINCT jsonb_build_object(
                        'person_role', pfw.role,
                        'person_id', p.id,
                        'person_name', p.full_name
                    )
                ) FILTER (WHERE p.id is not null),
                '[]'
            ) as persons
        FROM {self.table} as fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        {'WHERE fw.id IN %s' if filter else ''}
        GROUP BY fw.id
        ORDER BY fw.modified
        ;
        """
        return query

    def get_films_data(
        self, filter_films_ids: list[UUID] | None
    ) -> Generator[list[dict[str, Any]], None, None]:
        with self.connection.cursor() as cursor:
            if filter_films_ids is None:
                query = self.get_query(filter=False)
                cursor.execute(query)
            else:
                query = self.get_query()
                cursor.execute(query, vars=(tuple(filter_films_ids),))

            while film_data := cursor.fetchmany(size=self.batch_size):
                logger.debug(
                    f"Retrieved {len(film_data)} rows from `{self.table}` table"
                )
                yield film_data
