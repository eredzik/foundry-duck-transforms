from __future__ import annotations

# pyright: reportMissingImports=false, reportUntypedFunctionDecorator=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownArgumentType=false, reportUnusedFunction=false

from pathlib import Path
from typing import Any


def create_app(lineage_json: dict[str, Any]):  # type: ignore[no-untyped-def]
    from fastapi import FastAPI  # type: ignore[import-untyped]
    from fastapi.responses import JSONResponse  # type: ignore[import-untyped]
    from fastapi.staticfiles import StaticFiles  # type: ignore[import-untyped]

    app = FastAPI()

    @app.get("/api/lineage")
    def get_lineage() -> JSONResponse:
        return JSONResponse(lineage_json)

    dist_dir = Path(__file__).parent / "ui-dist"
    if dist_dir.exists():
        # Mount after /api so it doesn't shadow API routes.
        app.mount("/", StaticFiles(directory=str(dist_dir), html=True), name="ui")
    else:
        @app.get("/{full_path:path}")
        def missing_ui(full_path: str) -> JSONResponse:  # noqa: ARG001
            return JSONResponse(
                {
                    "error": "UI build not found",
                    "expected": str(dist_dir),
                    "hint": "Build the UI and copy dist/ into transforms/lineage/ui-dist/",
                },
                status_code=500,
            )
    return app

def serve_lineage_ui(lineage_json: dict[str, Any], port: int = 3000) -> None:
    """
    Serve the bundled React SPA and an API endpoint returning lineage JSON.

    The SPA build is expected at: transforms/lineage/ui-dist/
    """
    import uvicorn  # type: ignore[import-untyped]

    app = create_app(lineage_json=lineage_json)
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="info")

