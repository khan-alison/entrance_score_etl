from common.base_r2g_executor import BaseR2GExecutor
from pyspark.sql import functions as F

class R2GExecutor(BaseR2GExecutor):
    def __init__(self, spark, reader):
        self.spark = spark
        self.reader = reader

    def read_dfs(self):
        return self.reader.read_table()

    @staticmethod
    def write_to_parquet(df, write_mode, partition_cols, target_path, process_date):
        if "partition_date" in partition_cols:
            df = df.withColumn("partition_date", F.lit(process_date))

        if df.count() > 0:
            df.write.partitionBy(partition_cols).mode(
                write_mode).parquet(target_path)

    def execute(self):
        raise NotImplementedError
