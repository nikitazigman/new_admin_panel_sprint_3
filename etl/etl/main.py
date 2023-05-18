from time import sleep

from loguru import logger

from etl.logic.elastic_search.elastic_search import ElasticSearchLoader
from etl.logic.postgresql.enrichers import BaseEnricher
from etl.logic.postgresql.extractors import PostgreExtractor
from etl.logic.postgresql.mergers import BaseMerger
from etl.logic.postgresql.producers import (FilmWorkProducer, GenreProducer,
                                            PersonProducer)
from etl.logic.state.state import State
from etl.logic.transform.dataclasses import FilmWorkContainer
from etl.settings.settings import (ESSettings, PGSettings, StateSettings,
                                   SystemSettings)


def main() -> None:
    logger.info("ETL has been started")

    pg_settings = PGSettings()  # type: ignore
    es_settings = ESSettings()  # type: ignore
    state_settings = StateSettings()
    system_settings = SystemSettings()

    state = State(settings=state_settings)

    es_loader = ElasticSearchLoader(settings=es_settings)
    pg_extractor = PostgreExtractor(
        settings=pg_settings,
        film_producer_class=FilmWorkProducer,
        genre_producer_class=GenreProducer,
        person_producer_class=PersonProducer,
        enricher_class=BaseEnricher,
        merger_class=BaseMerger,
    )

    es_loader.define_schema()

    while True:
        logger.info("Runnig the synchronization process")

        state_data = state.get_state()
        state.update_state()

        counter = 0

        for film_batch in pg_extractor.get_films(state_data.last_checkup):
            es_bulk = FilmWorkContainer(batch=film_batch).transform_to_es_bulk()  # type: ignore
            counter += len(es_bulk.bulk)

            if es_bulk.bulk:
                es_loader.load_bulk(es_bulk)

        state.store_state()

        logger.info(f"{counter} films have been successfully synchronized.")
        logger.info("Going to sleep")
        sleep(system_settings.synhronization_time_sec)


# ToDo: Fail tollerance
# ToDo: Dockerfile
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Stop the ETL process")
