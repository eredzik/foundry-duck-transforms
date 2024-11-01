from pyspark.sql import DataFrame

from ..api.check import Check


def execute_check(df: DataFrame, check: Check ):
    try:
        check.expectation.run(df)
    except Exception as e:
        print(f"{check.on_error}: Check {check.description} failed with error {e}")
        if check.on_error=='FAIL':
            raise e

    