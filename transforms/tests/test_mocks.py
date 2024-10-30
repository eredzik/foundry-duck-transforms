from contextlib import contextmanager
from pathlib import Path
from typing import Any, TypedDict

import duckdb
import pytest
from foundry_dev_tools import Config, FoundryContext, JWTTokenProvider
from foundry_dev_tools.errors.dataset import BranchNotFoundError
from foundry_dev_tools.utils import api_types
from pytest import MonkeyPatch

from transforms.api import Input, Output, transform_df
from transforms.manage import FoundryManager


class TestStoreDataset(TypedDict):
    dataset_rid: str
    available_branches: list[str]
    dataset_name: str


dataset_store: list[TestStoreDataset] = [
    {
        "dataset_rid": "rid_1",
        "available_branches": ["master", "dev"],
        "dataset_name": "somefilename1",
    },
    {
        "dataset_rid": "rid_2",
        "available_branches": ["master"],
        "dataset_name": "somefilename2",
    },
]


def mock_dataset_identity(
    monkeypatch: MonkeyPatch,
    obj: Any,
    rids_to_available_branches: list[TestStoreDataset],
):
    def _get_dataset_identity(
        dataset_path_or_rid: str, branch: str
    ) -> api_types.DatasetIdentity:
        res = [
            dataset
            for dataset in rids_to_available_branches
            if dataset_path_or_rid == dataset["dataset_rid"]
            and branch in dataset["available_branches"]
        ]
        if len(res) > 0:
            return api_types.DatasetIdentity(
                dataset_path=res[0]["dataset_name"],
                dataset_rid=dataset_path_or_rid,
                last_transaction_rid=dataset_path_or_rid,
                last_transaction=None,
            )
        raise BranchNotFoundError(info="not found branch")

    monkeypatch.setattr(obj, "_get_dataset_identity", _get_dataset_identity)


def mock_get_temp_files(
    monkeypatch: MonkeyPatch,
    obj: Any,
    rids_to_available_branches: list[TestStoreDataset],
):
    @contextmanager
    def mock_get_temp_files(
        dataset_rid: str,
        view: str,
    ):
        res = [
            dataset
            for dataset in rids_to_available_branches
            if dataset["dataset_rid"] == dataset_rid
            and view in dataset["available_branches"]
        ]
        if len(res) > 0:
            yield Path(__file__).parent / "test_datasets/iris"
        else:
            raise BranchNotFoundError(info="not found branch")

    monkeypatch.setattr(obj, "download_dataset_files", mock_get_temp_files)


@pytest.fixture()
def fndry_ctx(monkeypatch):
    ctx = FoundryContext(
        config=Config(), token_provider=JWTTokenProvider("test", jwt="test2")
    )

    mock_dataset_identity(monkeypatch, ctx.cached_foundry_client, dataset_store)
    mock_get_temp_files(monkeypatch, ctx.cached_foundry_client.api, dataset_store)
    return ctx
