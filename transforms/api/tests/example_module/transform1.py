from transforms.api import Input, Output, transform_df


@transform_df(
    output=Output("t1"),
)
def compute():
    return {}
