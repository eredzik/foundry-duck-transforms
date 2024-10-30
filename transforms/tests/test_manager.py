import asyncio
from pathlib import Path

from foundry_dev_tools import Config, FoundryContext, JWTTokenProvider
from pytest import MonkeyPatch

from transforms.data_manager import DataManager
from transforms.tests.test_mocks import (
    fndry_ctx,
    mock_dataset_identity,
    mock_get_temp_files,
)


def test_manager(fndry_ctx):
    async def somework():
        async with DataManager(
            ctx=fndry_ctx,
        ) as mngr:
            mngr.reset_meta()
            await mngr.load_latest_parquet_into_database(
                dataset_rid_or_path="somerid", branch="dev"
            )

    asyncio.run(somework())
