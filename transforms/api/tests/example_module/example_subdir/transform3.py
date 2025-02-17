from transforms.api import Input, Output, transform_df


@transform_df(
        output=Output("t3"),
        input=Input("t2"),
)
def compute(input):
    return input