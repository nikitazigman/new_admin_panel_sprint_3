poetry run python manage.py collectstatic --no-input && \
poetry run gunicorn -c gunicorn/gunicorn.py
