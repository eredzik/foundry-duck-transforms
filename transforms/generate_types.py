
from pathlib import Path
from pyspark.sql import DataFrame
def generate_from_spark(dataset_name: str,spark_df: DataFrame):
    types_path = Path(__file__).parent / "types.py"
    print(types_path)
    with open(types_path, "r") as f:
        dataset = f.readlines()
        print(dataset)
    with open(types_path, "w") as f:
        replaced_declaration = False
        names =', '.join([f'"{name}"' for name in spark_df.columns])
        declaration = f"\n    {dataset_name}:TypeAlias = 'DataFrame[Literal[{names}]]'\n"
        for line in dataset:
            if line.startswith(f'    {dataset_name}'):
                replaced_declaration = True
                f.write(declaration)
            else:
                print(line)
                f.write(line)
        if not replaced_declaration:
            f.write(declaration)
        
            
    return

