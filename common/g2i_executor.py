from common.base_g2i_executor import BaseG2IExecutor


class G2IExecutor(BaseG2IExecutor):
    def __init__(self, spark, reader):
        self.spark = spark
        self.reader = reader

    def read_parquets(self):
        return self.reader.read_table()

    def clean_data(self):
        raise NotImplementedError

    def generate_insight(self):
        raise NotImplementedError

    def execute(self):
        raise NotImplementedError
