
from transforms.engine.spark_sail import init_sess


def test_transform():
    sess = init_sess()
    parq = sess.read.parquet('transforms/tests/test_datasets/iris/spark/iris.parquet')
    count = parq.count()
    print(count)