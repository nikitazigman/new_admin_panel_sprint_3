import json
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from loguru import logger
from pydantic import BaseModel

from etl.settings.settings import StateSettings


class StateData(BaseModel):
    last_checkup: datetime | None

    def get_dict(self) -> dict[str, str]:
        state = self.dict()
        state["last_checkup"] = str(state["last_checkup"])

        return state


class StateInt(ABC):
    @abstractmethod
    def __init__(self, path: Path) -> None:
        ...

    @abstractmethod
    def get_state(self) -> StateData:
        ...

    @abstractmethod
    def store_state(self) -> None:
        ...

    @abstractmethod
    def update_state(self) -> None:
        ...


class State(StateInt):
    def __init__(self, settings: StateSettings) -> None:
        self.path = settings.get_file_path()
        self.state = StateData(last_checkup=None)

    def get_state(self) -> StateData:
        raw_state: dict[str, datetime]

        logger.info("Reading the state")

        if not self.path.is_file():
            return StateData(last_checkup=None)

        with open(self.path, "r") as f:
            raw_state = json.load(f)

        self.state = StateData(**raw_state)
        return self.state

    def update_state(self) -> None:
        logger.info("Updating the state")

        self.state = StateData(last_checkup=datetime.utcnow())

    def store_state(self) -> None:
        logger.info("Storing the state")

        with open(self.path, "w+") as f:
            json.dump(self.state.get_dict(), f)
