from common.crawler_base import BaseCrawler
import requests
from bs4 import BeautifulSoup
from common.management_data import BaseManagementCSVData
import pandas as pd
import os



class UniversitiesInformationCrawler(BaseCrawler):
  def __init__(self, requests, url, dir_manager):
    self.requests = requests
    self.url = url
    self.dir_manager = dir_manager

  def fetch_data(self):
    response = self.requests.get(self.url)
    return response.content

  def extract_data(self, html):
    universities_data = []
    soup = BeautifulSoup(html, 'html.parser')
    for e_li in soup.select('#benchmarking > li'):
      a_tag = e_li.find('a')
      relative_url = a_tag.get('href')
      base_url_parts = self.url.split('/')[:3]
      base_url = '/'.join(base_url_parts)
      full_url = base_url + relative_url
      university_code = a_tag.find('strong').getText()
      university_name = a_tag.getText().split('-')[1].strip()
      universities_information = {
        "url": full_url,
        "university_code": university_code,
        "university_name": university_name
      }
      universities_data.append(universities_information)
    return universities_data


  def save_data(self, data):
    self.dir_manager.save_data(data)

  def crawl(self):
    html = self.fetch_data()
    data = self.extract_data(html)
    self.save_data(data)



if __name__ == '__main__':
  url = 'https://diemthi.tuyensinh247.com/diem-chuan.html'

  csv_manager = BaseManagementCSVData('./raw/universities_information', 'universities_information.csv', os, pd)
  csv_manager.load_data()
  uni_info = UniversitiesInformationCrawler(requests, url,dir_manager=csv_manager)
  data = uni_info.crawl()

