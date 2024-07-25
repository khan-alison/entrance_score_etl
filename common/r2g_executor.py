from common.base_r2g_executor import BaseR2GExecutor
from pyspark.sql import functions as F

class R2GExecutor(BaseR2GExecutor):
    def __init__(self, spark, reader):
        self.spark = spark
        self.reader = reader

    def read_dfs(self):
        return self.reader.read_table()

    @staticmethod
    def write_to_parquet(df, write_mode, partition_col, target_path, process_date):
        df = df.withColumn(partition_col, F.lit(process_date))
        df.show()
        if df.count() > 0:
            df.write.partitionBy(partition_col).mode(
                write_mode).parquet(target_path)

    def execute(self):
        raise NotImplementedError
