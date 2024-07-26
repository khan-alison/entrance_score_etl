from abc import ABC, abstractmethod


class RawReader(ABC):
    def __init__(self, spark, path_to_raw, table_name):
        pass

    @abstractmethod
    def read_table(self):
        pass


class RawCSVReader(RawReader):
    def __init__(self, spark, path_to_raw, table_name):
        super().__init__(spark, path_to_raw, table_name)
        self.spark = spark
        self.path_to_raw = path_to_raw
        self.table_name = table_name

    def read_table(self):
        path_of_table = self.path_to_raw + self.table_name
        return self.spark.read.options(header=True, inferSchema=True).csv(path_of_table).cache()
