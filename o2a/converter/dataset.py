from dataclasses import dataclass
from typing import Dict

@dataclass()
class Dataset:
    name: str
    frequency: str
    initial_instance: str
    timezone: str
    uri_template: str
    done_flag: Dict

