from abc import ABC, abstractmethod

class BaseCrawler(ABC):
  def __init__(self, requests, url, dir_manager):
    self.requests = requests
    self.url = url
    self.dir_manager = dir_manager

  @abstractmethod
  def fetch_data(self):
    raise NotImplementedError

  @abstractmethod
  def extract_data(self):
    raise NotImplementedError

  @abstractmethod
  def save_data(self):
    raise NotImplementedError

  @abstractmethod
  def crawl(self):
    raise NotImplementedError
