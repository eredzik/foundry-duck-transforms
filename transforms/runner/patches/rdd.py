from typing import Callable, Iterable

from pandas import DataFrame as PandasDF
from pyspark.sql import DataFrame


def patch_dataframe_rdd(df: DataFrame):
    import pandas as pd
    from pyspark import RDD
    from pyspark.sql import Row, SparkSession
    from pyspark.sql.types import StructType

    class PatchedRDD:
        def __init__(self, session: SparkSession, pandas_df: PandasDF):
            self.df = pandas_df
            self.num_partitions = 8
            self.session = session

        def mapPartitions(
            self, func: Callable[[Iterable[Row]], Iterable[Row]]
        ) -> "PatchedRDD":
            num_rows = len(self.df)
            partitions = (
                self.df.iloc[i * num_rows : (i + 1) * num_rows].to_dict(
                    orient="records"
                )
                for i in range(self.num_partitions)
            )

            new_dfs: list[PandasDF] = []
            for partition in partitions:
                row_gen = (Row(**row) for row in partition)
                res_data = list(r.asDict() for r in func(row_gen))
                res_df = PandasDF(res_data)
                new_dfs.append(res_df)
            res_df = pd.concat(new_dfs, axis=0)
            new_rdd = PatchedRDD(session=self.session, pandas_df=res_df)
            return new_rdd

        def toDF(self, schema: StructType) -> DataFrame:
            new_data = [Row(**row) for row in self.df.to_dict(orient="records")]
            return self.session.createDataFrame(data=new_data)

    class PatchedDfWithRdd(DataFrame):
        @property
        def rdd(self):
            rdd1 = self.toPandas()
            return PatchedRDD(self.session, rdd1)

    df.__class__ = PatchedDfWithRdd
    return df
