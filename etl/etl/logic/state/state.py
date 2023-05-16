from pathlib import Path
from datetime import datetime
from abc import ABC, abstractmethod
import json
from pydantic import BaseModel
from loguru import logger


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
    def update_state(self) -> None:
        ...


class State(StateInt):
    def __init__(self, path: Path) -> None:
        self.path = path

    def get_state(self) -> StateData:
        raw_state: dict[str, datetime]

        logger.info("Reading the state")

        if not self.path.exists():
            return StateData(last_checkup=None)

        with open(self.path, "r") as f:
            raw_state = json.load(f)

        return StateData(**raw_state)

    def update_state(self) -> None:
        logger.info("Updating the state")

        new_state = StateData(last_checkup=datetime.now())

        with open(self.path, "w+") as f:
            json.dump(new_state.get_dict(), f)
