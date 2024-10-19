from transforms.runner import Runner


def test_basic_transform_using_pyspark():
    runner = Runner("duckdb")

    from pyspark.sql import DataFrame

    from transforms.api.transform_df import Input, Output, transform_df

    @transform_df(output=Output("test1"), input1=Input("test0"))
    def some_transform(input1: DataFrame):
        return
