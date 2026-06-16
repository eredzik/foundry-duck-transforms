from dataclasses import dataclass


@dataclass
class DatasetCountReference:
    dataset_name: str


class DatasetRef:
    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name

    def count(self) -> DatasetCountReference:
        return DatasetCountReference(self.dataset_name)


def dataset_ref(dataset_name: str) -> DatasetRef:
    return DatasetRef(dataset_name)
