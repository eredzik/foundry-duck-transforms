from dataclasses import dataclass, field
from pathlib import Path

from foundry_dev_tools.errors.dataset import BranchNotFoundError

from transforms.api.transform_df import Transform
from transforms.runner.data_source.base import DataSource


@dataclass
class TransformRunner:
    fallback_branches: list[str] = field(default_factory=list)
    output_dir: Path = Path.home() / ".fndry_duck" / "output"

    def __post_init__(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
    def exec_transform(self, transform: Transform, data_sourcer: DataSource) -> None:
        sources = {}
        for argname, input in transform.inputs.items():
            branches = [
                b for b in ([input.branch] + self.fallback_branches) if b is not None
            ]
            for branch in branches:
                try:
                    sources[argname] = data_sourcer.download_dataset(
                        input.path_or_rid, branch=branch
                    )
                    break
                except BranchNotFoundError:
                    print(
                        f"Branch not found for dataset [{argname}={input.path_or_rid}]"
                    )
        res = transform.transform(**sources)
        res.write.parquet(f"{self.output_dir}/{transform.outputs[0].path_or_rid}")