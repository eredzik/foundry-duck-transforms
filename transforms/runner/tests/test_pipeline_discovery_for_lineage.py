from __future__ import annotations

from transforms.api.pipeline import Pipeline


def test_pipeline_discovers_transforms_recursively() -> None:
    # This package already exists in-repo and includes transforms in submodules/subpackages.
    from transforms.api.tests import example_module

    p = Pipeline()
    p.discover_transforms(example_module)
    assert len(p._transforms) >= 2

