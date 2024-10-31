import importlib.util
import sys

from sqlframe import activate

activate(engine="duckdb")
from foundry_dev_tools import Config, FoundryContext, JWTTokenProvider

from transforms.api.transform_df import Transform
from transforms.runner.data_source.foundry_source import FoundrySource
from transforms.runner.exec_transform import TransformRunner


def import_from_path(module_name: str, file_path: str):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module  # Register the module in sys.modules
    spec.loader.exec_module(module)  # Execute the module in its own namespace
    return module


def main():
    mod = import_from_path("transform", sys.argv[1])
    transforms = {}
    for name, item in mod.__dict__.items():
        if isinstance(item, Transform):
            transforms[name] = item
    if len(transforms) > 1:
        print("There is more than one transform specified. Please specify its name")
        print("names are", list(transforms.keys()))
        return
    if len(transforms) == 0:
        print("file has no transforms")
        return

    TransformRunner(fallback_branches=["dev"]).exec_transform(
        list(transforms.values())[0],
        data_sourcer=FoundrySource(
            ctx=FoundryContext(
                config=Config(), token_provider=JWTTokenProvider("test", jwt="test2")
            )
        ),
    )


if __name__ == "__main__":
    main()
