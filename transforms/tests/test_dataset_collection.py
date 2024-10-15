from contextlib import contextmanager
from pathlib import Path
from typing import Any, TypedDict

import duckdb
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
        dataset_rid: str, branch: str
    ) -> api_types.DatasetIdentity:
        res = [
            dataset
            for dataset in rids_to_available_branches
            if dataset_rid == dataset['dataset_rid'] and branch in dataset["available_branches"]
        ]
        if len(res) > 0:
            return api_types.DatasetIdentity(
                dataset_path=res[0]['dataset_name'],
                dataset_rid=dataset_rid,
                last_transaction_rid=dataset_rid,
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
            if dataset["dataset_rid"] == dataset_rid and view in dataset["available_branches"]
        ]
        if len(res) > 0:
            yield Path(__file__).parent / "test_datasets/iris"
        else:
            raise BranchNotFoundError(info="not found branch")

    monkeypatch.setattr(obj, "download_dataset_files_temporary", mock_get_temp_files)


def test_collecting_single_input(monkeypatch: MonkeyPatch):
    mngr = FoundryManager(
        duckdb_conn=duckdb.connect(":memory:"),
        ctx=FoundryContext(
            config=Config(), token_provider=JWTTokenProvider("test", jwt="test2")
        ),
    )

    mock_dataset_identity(monkeypatch, mngr.ctx.cached_foundry_client, dataset_store)
    mock_get_temp_files(monkeypatch, mngr.ctx.cached_foundry_client.api, dataset_store)
    
    
    mngr.get_dataset_from_foundry_into_duckdb("rid_1", "master")

    check_if_dataset_exists_and_has_rows(mngr, "fndry_master.somefilename1")


def test_transform_collection(monkeypatch: MonkeyPatch):
    mngr = FoundryManager(
        duckdb_conn=duckdb.connect(":memory:"),
        ctx=FoundryContext(
            config=Config(), token_provider=JWTTokenProvider("test", jwt="test2")
        ),
    )
    mock_dataset_identity(
        monkeypatch,
        mngr.ctx.cached_foundry_client,
        dataset_store,
    )
    mock_get_temp_files(monkeypatch, mngr.ctx.cached_foundry_client.api, dataset_store)

    from pyspark.sql import DataFrame

    @transform_df(Output("rid_2"), df=Input("rid_1"))
    def transform(df: DataFrame) -> DataFrame:
        return df

    mngr.collect_transform_inputs(transform)
    check_if_dataset_exists_and_has_rows(mngr, "fndry_master.somefilename1")


def check_if_dataset_exists_and_has_rows(mngr: FoundryManager, dataset_name: str):
    query_result: tuple[int] | None = mngr.duckdb_conn.query(
        f"SELECT count(*) FROM {dataset_name}"
    ).fetchone()
    assert query_result is not None
    assert query_result[0] > 0
