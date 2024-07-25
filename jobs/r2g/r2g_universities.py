from common.r2g_executor import R2GExecutor
from pyspark.sql import SparkSession
from reader.raw import RawCSVReader


class R2GUniversitiesExecutor(R2GExecutor):
    def __init__(self, spark, reader, target_path):
        self.spark = spark
        self.reader = reader
        self.partition_col = "partition_date"
        self.target_path = target_path

    def execute(self):
        df = self.read_dfs()
        self.write_to_parquet(df, write_mode='overwrite',
                              partition_col=self.partition_col, target_path=self.target_path, process_date="2024-07-25")

    def _transform(self, df):
        pass

    def _join_data(self, df_l, df_r):
        pass


if __name__ == '__main__':
    configurations = {
        "appName": "PySpark raw to golden for universities data",
        "master": "local",
        "target_output_path": "./golden/universities",
        "input_table":
        {
            "table": "universities_information",
            "path": './raw/'
        }
    }

    spark = SparkSession.builder\
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .appName(configurations['appName'])\
        .master(configurations['master'])\
        .getOrCreate()

    reader = RawCSVReader(
        spark, path_to_raw=configurations['input_table']['path'], table_name=configurations['input_table']['table'])

    executor = R2GUniversitiesExecutor(
        spark, reader, target_path=configurations['target_output_path'])
    executor.execute()
