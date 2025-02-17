from typing import Any

from transforms.api.transform_df import Transform


def configure(
    profile: str | list[str] | None = None,
    *args: Any,
    **kwargs: Any
):
    def _configure(transform: Transform):
        return transform
    return _configure