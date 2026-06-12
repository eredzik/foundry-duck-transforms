from __future__ import annotations

# pyright: reportMissingImports=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false

import pytest


fastapi = pytest.importorskip("fastapi")
pytest.importorskip("uvicorn")
pytest.importorskip("httpx")


def test_lineage_server_app_serves_json(monkeypatch):  # type: ignore[no-untyped-def]
    # We don't actually start uvicorn in tests; we just validate the FastAPI app wiring
    # by importing the module and calling its route handler via TestClient.
    from fastapi.testclient import TestClient

    # Patch uvicorn.run to avoid opening sockets if someone calls serve_lineage_ui accidentally.
    def _noop_run(*args: object, **kwargs: object) -> None:  # noqa: ARG001
        return

    monkeypatch.setattr("transforms.lineage.server.uvicorn.run", _noop_run, raising=False)

    from transforms.lineage import server

    lineage = {"datasets": {"ds": {"columns": ["a"], "graph": {"ops": [1]}}}}

    app = server.create_app(lineage_json=lineage)

    client = TestClient(app)
    r = client.get("/api/lineage")
    assert r.status_code == 200
    assert r.json()["datasets"]["ds"]["columns"] == ["a"]

