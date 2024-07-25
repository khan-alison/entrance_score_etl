from abc import ABC, abstractmethod

class BaseR2GExecutor(ABC):
  def __init__(self, spark):
    self.spark = spark

  @abstractmethod
  def read_dfs(self):
    raise NotImplementedError

  @abstractmethod
  def write_to_parquet(self, df, target_path):
    raise NotImplementedError

  @abstractmethod
  def execute(self):
    raise NotImplementedError