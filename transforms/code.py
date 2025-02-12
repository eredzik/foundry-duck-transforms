from typing import Literal


def edit_annotations(transform_path: str, to_annotate: dict[str, str], mode: Literal['add', 'remove']) -> None:
    # TODO: Implement it
    # to_annotate = mapping[rid, type_name in transforms.types]
    # 1. Read the file
    # 2. Find line starting transform_df decorator
    # 2a. go through arguments and extract rids of inputs
    # 3. Find line starting decorated function
    # 3a. add/remove types according to mode
    # from typing import TYPE_CHECKING
    # if TYPE_CHECKING:
    #     from transforms.types import 
    # def compute(df1: "dt.sometype"):
    # ...
    raise NotImplementedError()

