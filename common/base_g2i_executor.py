from abc import ABC, abstractclassmethod


class BaseG2IExecutor(ABC):
    def __init__(self, spark):
        self.spark = spark

    @abstractclassmethod
    def read_parquets(self):
        raise NotImplementedError

    @abstractclassmethod
    def clean_data(self):
        raise NotImplementedError

    @abstractclassmethod
    def generate_insight(self):
        raise NotImplementedError

    @abstractclassmethod
    def execute(self):
        raise NotImplementedError
