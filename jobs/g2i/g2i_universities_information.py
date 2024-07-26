from common.g2i_executor import G2IExecutor
from pyspark.sql import SparkSession
from reader.raw import RawParquetReader
from pyspark.sql import functions as F
import os


class G2IUniversitiesInformation(G2IExecutor):
    def __init__(self, spark, reader):
        self.spark = spark
        self.reader = reader

    def execute(self):
        df = self.read_parquets()
        df = df.where(F.col('major_code') == '7520207')
        df.show()


if __name__ == '__main__':
    configurations = {
        "appName": "PySpark for generate insight from golden universities data",
        "master": "local",
        "input_table": {
            "path": "./golden/",
            "table": [
                {"name": "universities"},
                {
                    "name": "benchmark/benchmark2019",
                    "sub_table":
                    {
                        "benchmark2019",
                        "benchmark2020",
                        "benchmark2021",
                        "benchmark2022",
                        "benchmark2023"
                    }
                },
                {
                    "name": "exam_result"
                }
            ]
        }
    }

    spark = SparkSession.builder\
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .appName(configurations['appName'])\
        .master(configurations['master'])\
        .getOrCreate()

    reader = RawParquetReader(
        spark, path_to_golden=configurations['input_table']['path'], table_name=configurations['input_table']['table'][1]['name'])

    executor = G2IUniversitiesInformation(spark, reader)
    executor.execute()
    # list_dir = os.walk(f'{configurations['input_table']['path']}')
    # for fd in list_dir:
    #     print(fd[0])
