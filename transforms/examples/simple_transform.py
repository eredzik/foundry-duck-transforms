from transforms.api import Input, Output, transform_df


@transform_df(output=Output("a"), a=Input("b"))
def sometransf(a):
    return a
