from common.r2g_executor import R2GExecutor
from pyspark.sql import SparkSession
from reader.raw import RawCSVReader
import os
import re

class R2GExamResultExecutor(R2GExecutor):
    def __init__(self, spark, reader, target_path, process_date):
        self.spark = spark
        self.reader = reader
        self.target_path = target_path
        self.process_date = process_date
        self.partition_cols = "group_code"

    @staticmethod
    def snake_case(s):
        s = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', s)
        return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s).lower()

    def normalize_columns(self, df):
        for col_name in df.columns:
            df = df.withColumnRenamed(col_name, self.snake_case(col_name))
        return df

    def execute(self):
        df = self.read_dfs()
        nor_df = self.normalize_columns(df)
        print(nor_df)
        self.write_to_parquet(nor_df, write_mode='overwrite',
                              partition_cols=self.partition_cols, target_path=self.target_path, process_date=self.process_date)


if __name__ == '__main__':
    configurations = {
        "appName": "PySpark raw to golder for example result",
        "master": "local",
        "input_table": {
            "path": './raw/',
            "table": 'exam_result'
        },
        "target_output_path":
        {
            "path": "./golden/exam_results/",
        },
        "process_date": "2024-07-26"
    }

    spark = SparkSession.builder.appName(configurations['appName'])\
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")\
        .master(configurations['master'])\
        .getOrCreate()

    reader = RawCSVReader(
        spark, path_to_raw=configurations['input_table']['path'], table_name=configurations['input_table']['table'])

    executor = R2GExamResultExecutor(
        spark, reader, target_path=configurations['target_output_path']['path'], process_date=configurations['process_date'])
    executor.execute()
