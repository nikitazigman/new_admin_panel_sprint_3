from etl.logic.postgresql.extractors import PostgreExtractor
from etl.logic.postgresql.producers import (
    GenreProducer,
    FilmWorkProducer,
    PersonProducer,
)
from etl.logic.postgresql.enrichers import BaseEnricher
from etl.logic.postgresql.mergers import BaseMerger
from etl.settings.settings import PGSettings, ESSettings
from etl.logic.transform.dataclasses import FilmWorkContainer
from etl.logic.elastic_search.elastic_search import ElasticSearchLoader
from datetime import datetime


def main() -> None:
    pg_settings = PGSettings()  # type: ignore
    es_settings = ESSettings()  # type: ignore

    state = datetime(2023, 5, 14, 16, 0, 0)

    es_loader = ElasticSearchLoader(settings=es_settings)
    es_loader.define_schema()
    with PostgreExtractor(
        settings=pg_settings,
        film_producer_class=FilmWorkProducer,
        genre_producer_class=GenreProducer,
        person_producer_class=PersonProducer,
        enricher_class=BaseEnricher,
        merger_class=BaseMerger,
    ) as pg_extractor:
        for film_batch in pg_extractor.get_modified_films(state):
            es_bulk = FilmWorkContainer(batch=film_batch).transform_to_es_bulk()  # type: ignore
            es_loader.load_bulk(es_bulk)


if __name__ == "__main__":
    main()
