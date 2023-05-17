FROM python:3.10-slim

# Update base
RUN apt-get update \
    && apt-get install -y --reinstall build-essential

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set work directory
WORKDIR /opt/app

# Installing dependencies
RUN pip install --upgrade pip \
    && pip install poetry

COPY poetry.lock poetry.lock
COPY pyproject.toml pyproject.toml


RUN poetry config installer.max-workers 10 \
    && poetry install 

COPY . .

# RUN chmod +x /opt/app/run.sh

