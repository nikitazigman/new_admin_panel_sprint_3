from uuid import UUID
from etl.logic.postgresql.interfaces import EnricherInt
from loguru import logger
from psycopg2._psycopg import connection


class BaseEnricher(EnricherInt):
    table = "content.film_work"

    def __init__(self, pg_connection: connection) -> None:
        self.connection = pg_connection

    def get_query(self) -> str:
        query = f"""
        --sql
        SELECT DISTINCT fw.id, fw.modified FROM {self.table} as fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        WHERE
            fw.id IN %(films_ids)s OR
            pfw.person_id IN %(persons_ids)s OR
            gfw.genre_id IN %(genres_ids)s
        ORDER BY fw.modified
        ;
        """
        return query

    def get_modified_films_ids(
        self,
        films_ids: list[UUID],
        genre_ids: list[UUID],
        person_ids: list[UUID],
    ) -> list[UUID]:
        logger.debug("Getting all modified films ids from the last checkup")

        modified_films_ids: list[UUID] = []
        query = self.get_query()
        with self.connection.cursor() as cursor:
            cursor.execute(
                query,
                vars={
                    "films_ids": tuple(films_ids),
                    "genres_ids": tuple(genre_ids),
                    "persons_ids": tuple(person_ids),
                },
            )
            modified_films_ids = [res["id"] for res in cursor.fetchall()]

        return modified_films_ids
