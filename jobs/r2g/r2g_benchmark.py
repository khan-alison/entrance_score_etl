from common.r2g_executor import R2GExecutor
from pyspark.sql import SparkSession
from reader.raw import RawCSVReader
import os


class R2GBenchmarkExecutor(R2GExecutor):
    def __init__(self, spark, reader, target_path, process_date):
        self.spark = spark
        self.reader = reader
        self.partition_cols = ["major_code"]
        self.target_path = target_path
        self.process_date = process_date

    def execute(self):
        df = self.read_dfs()
        self.write_to_parquet(df, write_mode='overwrite', partition_cols=self.partition_cols,
                              target_path=self.target_path, process_date=self.process_date)

    def _transform(self, df):
        dim_majors = df.select('major_code', 'major_name').distinct()
        fact_benchmark = df.select(
            'major_code', 'subject_group', 'point', 'note', 'year')
        return dim_majors, fact_benchmark


if __name__ == '__main__':
    configurations = {
        "appName": "PySpark raw to golden for benchmark data",
        "master": "local",
        "target_output_path":
        {
            "path": "./golden/benchmark/",
        },
        "input_table":
        {
            "path": './raw/benchmark/',
        },
        "sub_path": [
            "benchmark2019",
            "benchmark2020",
            "benchmark2021",
            "benchmark2022",
            "benchmark2023",
        ],
        "process_date": "2024-07-26"
    }

    spark = SparkSession.builder.appName(configurations['appName'])\
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .master(configurations['master'])\
        .getOrCreate()

    for file in configurations['sub_path']:
        reader = RawCSVReader(
            spark, path_to_raw=configurations['input_table']['path'], table_name=file)
        executor = R2GBenchmarkExecutor(spark, reader, target_path=f'{\
                                        configurations['target_output_path']['path']}{file}', process_date=configurations['process_date'])
        executor.execute()
