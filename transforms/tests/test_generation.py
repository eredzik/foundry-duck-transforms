def test_generate():
    from transforms.engine.duckdb import init_sess
    sess = init_sess()
    from transforms.generate_types import generate_from_spark
    parq = sess.read.parquet('transforms/tests/test_datasets/iris/spark/iris.parquet')
    generate_from_spark('iris',parq)
    pass