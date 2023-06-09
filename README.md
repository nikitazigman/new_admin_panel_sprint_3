# How to X?

## как запустить все это дело :)

0. Создать prod.env файлы для sql_etl, etl, movies_admin. Примеры файлов приведены ниже.
   sql_etl

```bash
dbname=movies_database
user=app
password=123qwe
host=postgres
port=5432

chunk_size=100
```

etl

```bash
DB_NAME=movies_database
DB_USER=app
DB_PASSWORD=123qwe
DB_PORT=5432
DB_HOST=postgres

ES_INDEX=movies
ES_USER=elastic
ES_PASSWORD=123qwe
ES_HOST=elastic_search
ES_PORT=9200
```

movies_admin

```bash
DJANGO_SECRET_KEY='jango-insecure-it1=^l55psi7oad*4z&$%2*8k1pp@0#3qk*9#5vb25&k3^r$_3'
DEBUG=True

DB_NAME=movies_database
DB_USER=app
DB_PASSWORD=123qwe
DB_PORT=5432
DB_HOST=postgres


ALLOWED_HOSTS=127.0.0.1
INTERNAL_IPS=127.0.0.1

POSTGRES_PASSWORD=123qwe
POSTGRES_DB=movies_database
POSTGRES_USER=app
```

1. Запустить большой docker-compose.yaml в корневой папке репозитория. Он должен поднять все сервисы.

```bash
docker compose up -d --build
```

2. Дождаться пока выполниться первый шаг. При самом первом старте запустить sqlite_etl чтобы заполнить реальную БД.

```bash
docker compose -f docker-compose.sqlite.yaml up
```

3. Создать админа для Djando movies_admin сервиса

```bash
docker compose exec app poetry run python manage.py createsuperuser
```

## как проверить что все это дело работает

1. запустить согласно инструкции выше (имеет смысл запустить не в режиме daemon или отдельно запусить etl чтобы видеть логи)
2. добавить ./etl/tests/etl_tests.json в постман
3. запустить тесты в Postman
4. открыть админку
5. изменить любую сущность
6. посмотреть что в логе от etl сервиса есть информация о синхронизации (будет написанно сколько фильмов было синхронизированно)
7. запустить тесты в Postman и убедиться что ничего не сломалось

## ver2:

1. Исправлена проблема с созданием state.json. Причина была в докере. Он не позволяет замапить volume на несуществующий файл. Код сам по себе создает/вал фаил при первой записи к нему и если фаил не существует возвращает стейт с None. Если ETL получает в стейте None она синхронизирует всю базу с ES. Исправил проблему слудющим образом. Вынес стейт в отдельную папку, для разработки прокинул папку в host через volume. Для "продакшн" добавил отдельный volume который замапил на папку где храниться стейт. Теоретически если надо принудитльно синхронизировать всю базу можно в прод etl контейнере просто грохнуть всю папку и он запуститься инициализацию с нуля.
2. Добавил описание prod.env в Readme.md
3. Интегрировал isort в dev зависимости и применил его ко всему etl. Сейчас импорты должны быть по Pep8
4. Добавил volume для ES. Вы не просили, но пусть будет :)
5. Перекинул сетку в etl docker-compose в host это только для разработки (так проще тестить локально интеграцию между postgre, es, etl). Большой докер работает через bridge.
6. Исправил генератор в mergers.py на := оператор
7. Добавил генератор на producer-ов. В генераторе на enricher не вижу смысла так как количество данных уже ограничено продюсерами.
8. Вынес sqlite_etl в отдельный docker-compose файл. Так мне кажется более правильным.

Надеюсь ничего не пропустил :)
