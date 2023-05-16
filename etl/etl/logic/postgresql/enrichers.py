from uuid import UUID
from etl.logic.postgresql.interfaces import EnricherInt
from loguru import logger
from psycopg2._psycopg import connection


class BaseEnricher(EnricherInt):
    table = "content.film_work"

    def __init__(self, pg_connection: connection) -> None:
        self.connection = pg_connection

    def get_query(
        self,
        films_ids: list[UUID],
        genre_ids: list[UUID],
        person_ids: list[UUID],
    ) -> str:
        query = f"""
        --sql
        SELECT DISTINCT fw.id, fw.modified FROM {self.table} as fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        WHERE
            {'fw.id IN %(films_ids)s OR' if films_ids else ''}
            {'pfw.person_id IN %(persons_ids)s OR' if person_ids else ''}
            {'gfw.genre_id IN %(genres_ids)s' if genre_ids else ''}
        ORDER BY fw.modified
        ;
        """
        return query

    def get_query_vars(
        self, films_ids: list[UUID], genre_ids: list[UUID], person_ids: list[UUID]
    ) -> dict[str, tuple]:
        mapping = zip(
            ["films_ids", "genres_ids", "persons_ids"],
            [films_ids, genre_ids, person_ids],
        )

        vars = {}
        for key, value in mapping:
            if value:
                vars[key] = value

        return vars

    def get_modified_films_ids(
        self,
        films_ids: list[UUID],
        genre_ids: list[UUID],
        person_ids: list[UUID],
    ) -> list[UUID]:
        logger.debug("Getting all modified films ids from the last checkup")

        query = self.get_query(films_ids, genre_ids, person_ids)
        vars = self.get_query_vars(films_ids, genre_ids, person_ids)

        modified_films_ids: list[UUID] = []
        with self.connection.cursor() as cursor:
            cursor.execute(query, vars=vars)
            modified_films_ids = [res["id"] for res in cursor.fetchall()]

        return modified_films_ids
