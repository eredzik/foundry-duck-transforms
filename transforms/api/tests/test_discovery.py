from transforms.api.pipeline import Pipeline


def test_discovery():
    from . import example_module
    pipeline = Pipeline()
    pipeline.discover_transforms(example_module)
    
    assert len(pipeline._transforms) == 3