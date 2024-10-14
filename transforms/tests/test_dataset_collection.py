from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb
from foundry_dev_tools import Config, FoundryContext, JWTTokenProvider
from foundry_dev_tools.errors.dataset import BranchNotFoundError
from pytest import MonkeyPatch

from transforms.api import Input, Output, transform_df
from transforms.manage import FoundryManager


def mock_dataset_identity(
    monkeypatch: MonkeyPatch,
    obj: Any,
    rids_to_available_branches: dict[str, list[str]],
):
    def _get_dataset_identity(dataset_rid: str, branch: str):
        res = [
            branch_res
            for branch_res in rids_to_available_branches[dataset_rid]
            if branch == branch_res
        ]
        if len(res) > 0:
            return {"dataset_path": f"filename_{dataset_rid}"}
        raise BranchNotFoundError(info="not found branch")

    monkeypatch.setattr(obj, "_get_dataset_identity", _get_dataset_identity)


def mock_get_temp_files(
    monkeypatch: MonkeyPatch,
    obj: Any,
    rids_to_available_branches: dict[str, list[str]],
):
    @contextmanager
    def mock_get_temp_files(
        dataset_rid: str,
        view: str,
    ):
        res = [
            branch_res
            for branch_res in rids_to_available_branches[dataset_rid]
            if view == branch_res
        ]
        if len(res) > 0:
            yield Path(__file__).parent / "test_datasets/iris"
        else:
            raise BranchNotFoundError(info="not found branch")

    monkeypatch.setattr(obj, "download_dataset_files_temporary", mock_get_temp_files)


dataset_store: dict[str, list[str]] = {
    "rid_1": ["master", "dev"],
    "rid_2": ["master"],
}


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
    query_result: tuple[int] | None = mngr.duckdb_conn.query(
        "SELECT count(*) FROM fndry_master.filename_rid_1"
    ).fetchone()
    assert query_result is not None
    assert query_result[0] > 0


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
    query_result: tuple[int] | None = mngr.duckdb_conn.query(
        "SELECT count(*) FROM fndry_master.filename_rid_1"
    ).fetchone()
    assert query_result is not None
    assert query_result[0] > 0
