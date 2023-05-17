from uuid import UUID
from etl.logic.postgresql.interfaces import EnricherInt
from loguru import logger
from psycopg2._psycopg import connection


class BaseEnricher(EnricherInt):
    table = "content.film_work"

    def __init__(self, pg_connection: connection) -> None:
        self.connection = pg_connection

    @staticmethod
    def get_filtration_subquery(
        films_ids: list[UUID],
        genre_ids: list[UUID],
        person_ids: list[UUID],
    ) -> str:
        mapping = zip(
            [
                "fw.id IN %(films_ids)s",
                "pfw.person_id IN %(persons_ids)s",
                "gfw.genre_id IN %(genres_ids)s",
            ],
            [films_ids, person_ids, genre_ids],
        )

        filtrations = []
        for filter, data in mapping:
            if data:
                filtrations.append(filter)

        return " OR ".join(filtrations)

    def get_query(
        self,
        films_ids: list[UUID],
        genre_ids: list[UUID],
        person_ids: list[UUID],
    ) -> str:
        filtration_subquery = self.get_filtration_subquery(
            films_ids, genre_ids, person_ids
        )

        query = f"""
        --sql
        SELECT DISTINCT fw.id, fw.modified FROM {self.table} as fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        WHERE
            {filtration_subquery}
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
                vars[key] = tuple(value)

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
