from transforms.api.pipeline import Pipeline


def build_pipeline() -> Pipeline:
    """
    Example pipeline entrypoint for discovery.

    Lineage mode itself calls Pipeline.discover_transforms(...) directly, but this file
    mirrors how a consumer could explicitly define a pipeline module.
    """
    from lineage_example import transforms_a, transforms_b

    p = Pipeline()
    p.discover_transforms(transforms_a, transforms_b)
    return p

