import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb
from foundry_dev_tools import FoundryContext
from foundry_dev_tools.errors.dataset import BranchNotFoundError
from foundry_dev_tools.errors.meta import FoundryAPIError

from transforms.prisma_client import Prisma
from transforms.prisma_client.cli import prisma as cli


@dataclass
class DatasetVersion:
    dataset_rid: str
    dataset_branch: str
    sanitized_rid: str
    sanitized_branch_name: str
    dataset_name: str
    dataset_identity: str
    last_update: datetime


class DataManager:
    def __init__(
        self,
        metadata_path: Path = Path.home() / ".fndry_duck" / "meta.db",
        storage_dir: Path = Path.home() / ".fndry_duck" / "store",
        config_dir: Path = Path.home() / ".fndry_duck",
        ctx: FoundryContext | None = None,
    ):
        self.metadata_path = metadata_path
        self.storage_dir = storage_dir
        self.config_dir = config_dir
        self.ctx = ctx or FoundryContext()
        # cli.run(
        #     ["migrate", "deploy", f'--schema={Path(__file__).parent/'schema.prisma'}'],
        #     env={"DATABASE_URL": self.metadata_path},
        # )
        cli.run(
            ["db", "push", f'--schema={Path(__file__).parent/'schema.prisma'}'],
            env={"DATABASE_URL": f"file:{str(self.metadata_path)}"},
        )

    def reset_meta(self):
        self.metadata_path.unlink(missing_ok=True)

        cli.run(
            ["db", "push", f'--schema={Path(__file__).parent/'schema.prisma'}'],
            env={"DATABASE_URL": f"file:{str(self.metadata_path)}"},
        )

    async def __aenter__(self):
        self.prisma = Prisma(datasource={"url": f"file:{str(self.metadata_path)}"})
        await self.prisma.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.prisma.disconnect()

    async def load_latest_parquet_into_database(
        self,
        dataset_rid_or_path: str,
        branch: str,
    ):
        dataset = await self.prisma.dataset_identifier.find_first(
            where={"rid_or_path": dataset_rid_or_path}, include={"dataset": True}
        )
        if dataset:
            version = await self.prisma.dataset_version.find_first(
                where={
                    "branch": {"is": {"full_branch_name": "branch"}},
                    "datasetId": dataset.id,
                },
                order={"data_identity_date": "desc"},
            )

        identity = self.ctx.cached_foundry_client._get_dataset_identity(
            dataset_path_or_rid=dataset_rid_or_path, branch=branch
        )
        identity_id = identity["last_transaction_rid"]
        # No version found or is outdated and there is some version available
        if identity_id is None:
            return "No data"
        if not version or (identity_id != version.data_identity_id):
            temp = tempfile.mkdtemp(
                suffix=f"foundry_dev_tools-{identity['last_transaction_rid']}"
            )

            try:
                self.ctx.cached_foundry_client.api.download_dataset_files(
                    dataset_rid=identity_id,
                    view=branch,
                    output_directory=temp,
                )
                return temp
            except FoundryAPIError:
                print("TODO: Download dataset through sql")
                return "TODO"

        else:
            return identity_id
