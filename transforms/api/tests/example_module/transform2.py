from transforms.api import Input, Output, transform_df


@transform_df(
        output=Output("t2"),
        input=Input("t1"),
)
def transform1(input):
    return input