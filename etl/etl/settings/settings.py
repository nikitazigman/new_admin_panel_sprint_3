from pathlib import Path

from pydantic import BaseSettings, Field

BASE_DIR = Path(__file__).resolve().parent.parent.parent


class MetaSettings(BaseSettings):
    class Config:
        env_file = str(BASE_DIR.joinpath(".env").absolute())
        env_file_encoding = "utf-8"


class StateSettings(MetaSettings):
    state_folder: Path = BASE_DIR.joinpath("etl_state")
    file_name: str = "state.json"

    def get_file_path(self) -> Path:
        self.state_folder.mkdir(exist_ok=True)
        return self.state_folder.joinpath(self.file_name)


class SystemSettings(MetaSettings):
    synhronization_time_sec: int = 10

    original_wait_for_sevice_time_sec: float = 0.1
    wait_for_sevice_factor: int = 2
    wait_for_sevice_edge_time_sec: float = 10


class PGSettings(MetaSettings):
    dbname: str = Field(env="DB_NAME")
    user: str = Field(env="DB_USER")
    password: str = Field(env="DB_PASSWORD")
    host: str = Field(env="DB_HOST")
    port: int = Field(env="DB_PORT")


class ESSettings(MetaSettings):
    index: str = Field(env="ES_INDEX")
    user: str = Field(env="ES_USER")
    password: str = Field(env="ES_PASSWORD")
    host: str = Field(env="ES_HOST")
    port: int = Field(env="ES_PORT")
    path_to_schema: Path = Field(
        env="ES_SCHEMA", default=BASE_DIR.joinpath("es_schema.json")
    )
