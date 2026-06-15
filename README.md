# Transforms package

A package for transforming data using transform syntax similar to Palantir Foundry. It enables you to write and test transforms locally before running them on foundry. Also they can be run faster using duckdb as underlying data engine.

## Getting Started

### Installation

Install the package using pip:

```bash
pip install foundry-duck-transforms
```

### Basic Usage

1. Create a transform file (e.g. `my_transform.py`)
2. Run it using the transforms CLI:

```bash
python -m transforms.run my_transform.py dev,master
```

Above command will run the transform while downloading data from dev, fallbacking to master if dataset has no data on dev.

### CLI Options

The transforms runner supports several options to customize execution:

```bash
python -m transforms.run [OPTIONS] TRANSFORM_TO_RUN FALLBACK_BRANCHES
```

Available options:

- `--engine [spark|duckdb|spark-sail]`: Engine to use for the transformation (default: spark)
- `--omit-checks`: Disables checks running
- `--sail-server-url TEXT`: Sail server url (required when using spark-sail engine)
- `--dry-run`: Dry run the transformation without writing results
- `--local-dev-branch-name TEXT`: Branch name for local development (default: "duck-fndry-dev")

Example with options:

```bash
python -m transforms.run my_transform.py dev,master --engine duckdb --dry-run
```

## Development Setup

### Prerequisites

- Python 3.7+
- pip
- Access to Palantir Foundry environment

### Local Development

1. Clone the repository
2. Install development dependencies:

```bash
pip install -e ".[dev]"
```

### Foundry Dev Tools Configuration

See [here](https://emdgroup.github.io/foundry-dev-tools/configuration.html) for detailed configuration instructions.

### Duck Transforms Settings

Create `config_foundry_duck_transforms.toml` in the same locations as foundry-dev-tools config:

- `~/.foundry-dev-tools/config_foundry_duck_transforms.toml`
- `~/.config/foundry-dev-tools/config_foundry_duck_transforms.toml`
- `{project_root}/config_foundry_duck_transforms.toml`

Example:

```toml
[config]
allowed_stale_time = "7d"

[datasets."ri.foundry.main.dataset.33dd1b10-cbcb-4035-94a4-a5e9d26699ce"]
allowed_stale_time = "30d"
source_query = "SELECT col_a, col_b FROM `{dataset_rid}` WHERE event_date >= '2025-01-01'"
```

Options:

- `allowed_stale_time` — keep using the on-disk Foundry cache even when a newer transaction exists, as long as the cached transaction `closeTime` is within this TTL (`90m`, `24h`, `7d`, etc.). Omit for always-fresh downloads.
- `source_query` — Foundry SQL used instead of a full file download. Placeholders: `{dataset_rid}`, `{dataset_path}`, `{branch}`. Query results are cached locally under the foundry-dev-tools cache directory; cache TTL uses the same `allowed_stale_time`.

Per-dataset `[datasets."..."]` tables override global `[config]` defaults. Dataset keys can be a dataset RID or Foundry compass path.

### VSCode Setup

Add this to your `.vscode/launch.json` for debugging support:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Python Debugger: Current File",
      "type": "debugpy",
      "request": "launch",
      "module": "transforms.run",
      "args": ["${file}", "dev,master"],
      "console": "integratedTerminal"
    }
  ]
}
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
