

def test_transform():
    from pyspark.sql import DataFrame

    from transforms.api.transform_df import (
        Input,
        Output,
        transform_df,
    )

    @transform_df(output=Output("dataset"), df1=Input("dataset"), df2=Input("dataset"))
    def some_transform(df1: DataFrame, df2: DataFrame):
        return df1
    assert some_transform.inputs["df1"] is not None
    


def test_incremental():
    from pyspark.sql import DataFrame

    from transforms.api.incremental_transform import incremental
    from transforms.api.transform_df import Input, Output, transform_df

    @incremental(require_incremental=True)
    @transform_df(Output("dataset"), df1=Input("dataset"))
    def some_transform(df1: DataFrame):
        return df1
    assert some_transform.inputs["df1"] is not None

def test_all():
    from pyspark.sql import DataFrame

    from transforms.api import configure
    from transforms.api.incremental_transform import incremental
    from transforms.api.transform_df import Input, Output, transform_df

    @configure(profile=["dev"])
    @incremental(require_incremental=True)
    @transform_df(Output("dataset"), df1=Input("dataset"))
    def some_transform(df1: DataFrame):
        return df1
    assert some_transform.inputs["df1"] is not None

def test_multi_output():
    from transforms.api.transform_df import (
        Input,
        Output,
        TransformInput,
        TransformOutput,
        transform,
    )

    
    
    @transform(out1=Output("dataset"), df1=Input("dataset"))
    def some_transform(out1: TransformOutput, df1: TransformInput):
        return df1
    assert some_transform.inputs["df1"] is not None
