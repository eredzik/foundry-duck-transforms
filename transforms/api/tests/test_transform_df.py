from transforms.api.transform_df import Input, Output, transform_df


def test_transform():
    from pyspark.sql import DataFrame

    @transform_df(output=Output("dataset"), df1=Input("dataset"), df2=Input("dataset"))
    def some_transform(df1: DataFrame, df2: DataFrame):
        return df1
    
    
