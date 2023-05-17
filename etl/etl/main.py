from etl.logic.postgresql.extractors import PostgreExtractor
from etl.logic.postgresql.producers import (
    GenreProducer,
    FilmWorkProducer,
    PersonProducer,
)
from etl.logic.postgresql.enrichers import BaseEnricher
from etl.logic.postgresql.mergers import BaseMerger
from etl.settings.settings import PGSettings, ESSettings, StateSettings, SystemSettings
from etl.logic.transform.dataclasses import FilmWorkContainer
from etl.logic.elastic_search.elastic_search import ElasticSearchLoader
from etl.logic.state.state import State
from time import sleep
from loguru import logger


def main() -> None:
    logger.info("ETL has been started")

    pg_settings = PGSettings()  # type: ignore
    es_settings = ESSettings()  # type: ignore
    state_settings = StateSettings()
    system_settings = SystemSettings()

    state = State(state_settings.state_path.absolute())

    es_loader = ElasticSearchLoader(settings=es_settings)
    es_loader.define_schema()

    while True:
        logger.info("Runnig the synchronization process")
        state_data = state.get_state()

        counter = 0

        with PostgreExtractor(
            settings=pg_settings,
            film_producer_class=FilmWorkProducer,
            genre_producer_class=GenreProducer,
            person_producer_class=PersonProducer,
            enricher_class=BaseEnricher,
            merger_class=BaseMerger,
        ) as pg_extractor:
            for film_batch in pg_extractor.get_films(state_data.last_checkup):
                es_bulk = FilmWorkContainer(batch=film_batch).transform_to_es_bulk()  # type: ignore
                counter += len(es_bulk.bulk)

                if es_bulk.bulk:
                    es_loader.load_bulk(es_bulk)

        state.update_state()

        logger.info(f"{counter} films have been successfully synchronized.")
        logger.info("Going to sleep")
        sleep(system_settings.sleep_time_sec)


# ToDo: Empty ids
# ToDo: Fail tollerance
# ToDo: Dockerfile
# ToDO: add code from previous sprints
if __name__ == "__main__":
    main()
