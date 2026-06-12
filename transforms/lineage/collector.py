from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class GlobalLineageCollector:
    """
    Global aggregation of lineage across all output datasets.

    Stored shape is intentionally simple for UI consumption:
    {
      "datasets": {
        "<dataset_id>": {
          "columns": [...],
          "graph": {...}  # raw stub lineage graph
        }
      }
    }
    """

    _datasets: dict[str, dict[str, Any]] = field(default_factory=dict)
    _transforms: list[dict[str, Any]] = field(default_factory=list)
    _active_transform: dict[str, Any] | None = None

    def start_transform(
        self,
        *,
        transform_id: str,
        name: str,
        file: str | None,
        module: str | None = None,
    ) -> None:
        self._active_transform = {
            "id": transform_id,
            "name": name,
            "module": module,
            "file": file,
            "filename": os.path.basename(file) if file else None,
        }

    def end_transform(self) -> None:
        self._active_transform = None

    def record_output(self, dataset_id: str, df: Any) -> None:
        cols = list(getattr(df, "columns", []) or [])
        graph: dict[str, Any] = {}
        if hasattr(df, "lineage_graph"):
            try:
                graph = df.lineage_graph()
            except Exception:
                graph = {}

        # Enrich stub dataframe ids (df_123, ...) with human-friendly labels.
        # We keep the original ids for graph integrity and add a mapping for UIs/consumers.
        try:
            ops_raw = graph.get("ops")
            dfs_raw = graph.get("dataframes")
            if isinstance(ops_raw, list) and isinstance(dfs_raw, dict):
                op_by_out: dict[str, str] = {}
                for o_any in ops_raw:  # type: ignore[reportUnknownVariableType]
                    if not isinstance(o_any, dict):
                        continue
                    o: dict[Any, Any] = o_any
                    out = o.get("out")
                    op = o.get("op")
                    if isinstance(out, str) and isinstance(op, str) and out not in op_by_out:
                        op_by_out[out] = op

                producer_filename = (
                    self._active_transform.get("filename")
                    if isinstance(self._active_transform, dict)
                    else None
                )
                labels: dict[str, str] = {}
                for df_id_any in dfs_raw.keys():  # type: ignore[reportUnknownVariableType]
                    if not isinstance(df_id_any, str):
                        continue
                    df_id = df_id_any
                    op = op_by_out.get(df_id)
                    if producer_filename and op:
                        labels[df_id] = f"{producer_filename} · {op} · {df_id}"
                    elif producer_filename:
                        labels[df_id] = f"{producer_filename} · {df_id}"
                    elif op:
                        labels[df_id] = f"{op} · {df_id}"
                    else:
                        labels[df_id] = df_id
                graph["df_labels"] = labels
        except Exception:
            # Best-effort enrichment only.
            pass

        payload: dict[str, Any] = {"columns": cols, "graph": graph}
        if self._active_transform is not None:
            payload["producer"] = dict(self._active_transform)
        self._datasets[dataset_id] = payload

    def record_transform(
        self,
        *,
        transform_id: str,
        name: str,
        inputs: list[str],
        outputs: list[str],
        module: str | None = None,
        file: str | None = None,
    ) -> None:
        self._transforms.append(
            {
                "id": transform_id,
                "name": name,
                "module": module,
                "file": file,
                "filename": os.path.basename(file) if file else None,
                "inputs": inputs,
                "outputs": outputs,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {"datasets": self._datasets, "transforms": self._transforms}

    def write_json(self, path: Path) -> None:
        path.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True))

