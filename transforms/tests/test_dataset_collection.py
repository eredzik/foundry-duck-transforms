from contextlib import contextmanager
from pathlib import Path
from typing import Any

import duckdb
from foundry_dev_tools import Config, FoundryContext, JWTTokenProvider
from pytest import MonkeyPatch

from transforms.api import Input, Output, transform_df
from transforms.manage import FoundryManager


def mock_dataset_identity(monkeypatch: MonkeyPatch, obj:Any, result_dataset_name:str):
    def _get_dataset_identity(dataset_rid:str, branch:str):
        return {'dataset_path':result_dataset_name}
    monkeypatch.setattr(obj, '_get_dataset_identity', _get_dataset_identity)

def mock_get_temp_files(monkeypatch: MonkeyPatch, obj:Any, filename: str):
    @contextmanager
    def mock_get_temp_files(dataset_rid: str, view: str, ):
        yield Path(__file__).parent / filename
    
    monkeypatch.setattr(obj, "download_dataset_files_temporary", mock_get_temp_files)



def test_collecting_single_input(monkeypatch: MonkeyPatch):
    mngr = FoundryManager(duckdb_conn=duckdb.connect(':memory:'), ctx=FoundryContext(config=Config(), token_provider=JWTTokenProvider("test", jwt='test2')))
    
    mock_dataset_identity(monkeypatch, mngr.ctx.cached_foundry_client, "some_dataset_name")
    mock_get_temp_files(monkeypatch, mngr.ctx.cached_foundry_client.api, "test_datasets/iris")
    
    mngr.get_dataset_from_foundry_into_duckdb('rid_1', 'master')
    query_result: tuple[int] | None= mngr.duckdb_conn.query("SELECT count(*) FROM fndry_master.some_dataset_name").fetchone()
    assert query_result is not None
    assert query_result[0] > 0


def test_transform_collection(monkeypatch: MonkeyPatch):
    mngr = FoundryManager(duckdb_conn=duckdb.connect(':memory:'), ctx=FoundryContext(config=Config(), token_provider=JWTTokenProvider("test", jwt='test2')))
    mock_dataset_identity(monkeypatch, mngr.ctx.cached_foundry_client, "some_dataset_name")
    mock_get_temp_files(monkeypatch, mngr.ctx.cached_foundry_client.api, "test_datasets/iris")
    
    from pyspark.sql import DataFrame
    @transform_df(Output("rid_2"), df=Input('rid_1'))
    def transform(df: DataFrame) -> DataFrame:
        return df
    
    mngr.collect_transform_inputs(transform)
    query_result: tuple[int] | None= mngr.duckdb_conn.query("SELECT count(*) FROM fndry_master.some_dataset_name").fetchone()
    assert query_result is not None
    assert query_result[0] > 0