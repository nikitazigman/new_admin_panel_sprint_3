from pathlib import Path

from pydantic import BaseSettings

BASE_DIR = Path(__name__).resolve().parent


class Settings(BaseSettings):
    dbname: str
    user: str
    password: str
    host: str
    port: int

    sqlite_file_path: Path = BASE_DIR.joinpath("db.sqlite").absolute()
    chunk_size: int = 50
