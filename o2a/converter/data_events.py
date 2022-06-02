from dataclasses import dataclass
from typing import Optional


@dataclass()
class DataEvent:
    name: str
    dataset: str
    instance: Optional[str]


@dataclass()
class InputEvent(DataEvent):
    start_instance: Optional[str] = None
    end_instance: Optional[str] = None


@dataclass()
class OutputEvent(DataEvent):
    nocleanup: bool
