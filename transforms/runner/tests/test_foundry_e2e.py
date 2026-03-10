import os
from pathlib import Path

import pytest
from dotenv import load_dotenv
from foundry_dev_tools import FoundryContext, JWTTokenProvider
from foundry_dev_tools.errors.config import MissingCredentialsConfigError

from transforms.engine.duckdb import init_sess
from transforms.runner.data_source.foundry_source_with_duck import FoundrySourceWithDuck


# Load environment variables from a .env file (if present)
# This allows configuring e2e test parameters via a local .env in the repo root.
load_dotenv()

FOUNDRY_E2E_ENABLED = os.getenv("FOUNDRY_DUCK_E2E_ENABLED") == "1"
FOUNDRY_E2E_DATASET = os.getenv("FOUNDRY_DUCK_E2E_DATASET_PATH", "somerid")
FOUNDRY_E2E_BRANCH = os.getenv("FOUNDRY_DUCK_E2E_BRANCH", "master")
FOUNDRY_E2E_HOST = os.getenv("FOUNDRY_HOST", "somehost.com")
FOUNDRY_E2E_TOKEN = os.getenv("FOUNDRY_TOKEN", "somejwt.com")


pytestmark = pytest.mark.e2e



@pytest.mark.asyncio
@pytest.mark.skipif(
    not FOUNDRY_E2E_ENABLED or not FOUNDRY_E2E_DATASET or not FOUNDRY_E2E_HOST or not FOUNDRY_E2E_TOKEN,
    reason="Foundry e2e tests not configured (set FOUNDRY_DUCK_E2E_ENABLED=1 and FOUNDRY_DUCK_E2E_DATASET_PATH)",
)
async def test_foundry_source_with_duck_downloads_real_dataset(tmp_path: Path) -> None:
    """
    Basic end-to-end test that:
    - connects to a real Foundry environment via foundry-dev-tools
    - downloads a dataset into a DuckDB-backed SQLFrame session
    - verifies that the resulting DataFrame is usable
    - verifies that DuckDB view definitions are materialized

    This test requires:
    - valid foundry-dev-tools configuration in the environment
    - FOUNDRY_DUCK_E2E_ENABLED=1
    - FOUNDRY_DUCK_E2E_DATASET_PATH set to a readable dataset path or RID
    - optionally FOUNDRY_DUCK_E2E_BRANCH (defaults to 'master')
    """

    try:
        ctx = FoundryContext(token_provider=JWTTokenProvider(jwt=FOUNDRY_E2E_TOKEN, host=FOUNDRY_E2E_HOST))
    except MissingCredentialsConfigError as exc:
        pytest.skip(
            f"Foundry credentials not configured for e2e tests: {exc}. "
            "See https://emdgroup.github.io/foundry-dev-tools/getting_started/installation.html"
        )
    session = init_sess()

    duckdb_db_path = tmp_path / "analytical_db.db"
    duckdb_sql_path = tmp_path / "analytical_db.sql"

    source = FoundrySourceWithDuck(
        ctx=ctx,
        session=session,
        duckdb_path=str(duckdb_db_path),
        duckdb_path_sql=str(duckdb_sql_path),
    )

    df = await source.download_dataset(FOUNDRY_E2E_DATASET, branch=FOUNDRY_E2E_BRANCH)
    

    # The returned object should be a SQLFrame DataFrame (Spark-like API)
    assert hasattr(df, "limit")
    assert hasattr(df, "collect")

    # Ensure we can touch the data without errors
    df.limit(1).collect()
    assert df.count() > 5

    # Ensure DuckDB artifacts were written alongside the download
    assert duckdb_db_path.exists()
    assert duckdb_sql_path.exists()
    assert duckdb_sql_path.read_text().strip() != ""

