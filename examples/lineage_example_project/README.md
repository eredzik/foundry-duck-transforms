# Lineage example project

This is a tiny “consumer project” used to manually test `foundry-duck-transforms` lineage mode.

## Run lineage UI (from repo root)

Prereqs:
- `pyspark-lineage-stub` installed (local path or internal index)
- `fastapi` + `uvicorn` installed (via `foundry-duck-transforms[lineage]`)

Command:

```bash
python -m transforms.run lineage examples/lineage_example_project/lineage_example dev --port 3000
```

Then open `http://127.0.0.1:3000`.

