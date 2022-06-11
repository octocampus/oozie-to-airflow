from dataclasses import dataclass
from typing import Dict, List


@dataclass()
class Dataset:
    name: str
    frequency: str
    initial_instance: str
    timezone: str
    uri_template: str
    done_flag: Dict


def get_dataset_name_from_task_doc(doc):
    return doc.split('=')[1]


def find_dataset_by_name(datasets: List, name: str):
    for dataset in datasets:
        if dataset['name'] == name: return dataset

    return None

