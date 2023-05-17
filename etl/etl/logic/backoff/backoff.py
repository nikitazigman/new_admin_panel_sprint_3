from functools import wraps, partial
from loguru import logger
from psycopg2.errors import OperationalError
from etl.logic.postgresql.interfaces import ExtractorInt
import time
from requests.exceptions import ConnectionError


def get_time_interval(
    sleep_time: float, factor: int, border_sleep_time_sec: int
) -> float:
    """
    Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """
    return min(sleep_time * (2**factor), border_sleep_time_sec)


def postgre_backoff(start_sleep_time=0.1, factor=2, border_sleep_time_sec=10):
    """
    retries to execute postgre operation in case of any connection error

    """
    get_pg_time = partial(
        get_time_interval,
        factor=factor,
        border_sleep_time_sec=border_sleep_time_sec,
    )

    def func_wrapper(func):
        @wraps(func)
        def inner(object: ExtractorInt, *args, **kwargs):
            sleep_time = start_sleep_time
            while True:
                try:
                    yield from func(object, *args, **kwargs)
                    return
                except OperationalError as e:
                    sleep_time = get_pg_time(sleep_time=sleep_time)
                    logger.error(
                        f"PostgreSQL connection error. {e}. Trying again in {sleep_time} sec"
                    )
                    time.sleep(sleep_time)

        return inner

    return func_wrapper


def es_backoff(start_sleep_time=0.1, factor=2, border_sleep_time_sec=10):
    """
    retries to execute elastic search request in case of any connection error
    """
    get_es_time = partial(
        get_time_interval,
        factor=factor,
        border_sleep_time_sec=border_sleep_time_sec,
    )

    def func_wrapper(func):
        @wraps(func)
        def inner(object: ExtractorInt, *args, **kwargs):
            sleep_time = start_sleep_time
            while True:
                try:
                    return func(object, *args, **kwargs)
                except ConnectionError as e:
                    sleep_time = get_es_time(sleep_time=sleep_time)
                    logger.error(
                        f"ES connection error. {e}. Trying again in {sleep_time} sec"
                    )
                    time.sleep(sleep_time)

        return inner

    return func_wrapper
