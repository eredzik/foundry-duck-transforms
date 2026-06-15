
from pathlib import Path
from pyspark.sql import DataFrame


def generate_from_spark(dataset_name: str, spark_df: DataFrame) -> None:
    generate_from_spark_batch([(dataset_name, spark_df)])


def generate_from_spark_batch(
    entries: list[tuple[str, DataFrame]],
) -> None:
    if not entries:
        return

    types_path = Path(__file__).parent / "types.py"
    with open(types_path, "r") as f:
        lines = f.readlines()

    declarations: dict[str, str] = {}
    for dataset_name, spark_df in entries:
        names = ", ".join([f'"{name}"' for name in spark_df.columns])
        declarations[dataset_name] = (
            f"\n    {dataset_name}:TypeAlias = 'DataFrame[Literal[{names}]]'\n"
        )

    replaced_names: set[str] = set()
    out_lines: list[str] = []
    for line in lines:
        matched = False
        for dataset_name, declaration in declarations.items():
            if line.startswith(f"    {dataset_name}"):
                out_lines.append(declaration)
                replaced_names.add(dataset_name)
                matched = True
                break
        if not matched:
            out_lines.append(line)

    for dataset_name, declaration in declarations.items():
        if dataset_name not in replaced_names:
            out_lines.append(declaration)

    with open(types_path, "w") as f:
        f.writelines(out_lines)
