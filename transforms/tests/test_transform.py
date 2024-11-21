
from transforms.engine.spark_sail import init_sess


def test_transform():
    sess = init_sess()
    parq = sess.read.parquet('transforms/tests/test_datasets/iris/spark/iris.parquet')
    count = parq.count()
    print(count)


def test_transform_big():
    sess = init_sess()
    def init_dataset():
        parq = sess.read.parquet('transforms/tests/test_datasets/iris/spark/iris.parquet')
        return parq
    df = init_dataset()
    for i in range(490):
        df = df.union(init_dataset())
    count = df.count()
    print(count)