from transforms.api.transform_df import Transform


def configure(
    profile: str | list[str] | None = None,
    *args,
    **kwargs
):
    def _configure(transform: Transform):
        return transform
    return _configure