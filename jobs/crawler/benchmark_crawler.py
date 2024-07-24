from common.crawler_base import BaseCrawler
from common.management_data import BaseManagementCSVData
import requests
import os
import pandas as pd
from bs4 import BeautifulSoup
from helper.logger_helper import LoggerSimple

logger = LoggerSimple.get_logger(__name__)

class BenchmarkCrawler(BaseCrawler):
    def __init__(self, requests, url, dir_manager):
        self.requests = requests
        self.url = url
        self.dir_manager = dir_manager

    def fetch_data(self):
        response = self.requests.get(self.url)
        if response.status_code == 200:
            return response.content
        else:
            logger.warning(f'{response.status_code} - {self.url}')

    def extract_data(self, html):
        benchmark_data = []
        try:
            soup = BeautifulSoup(html, 'html.parser')
            e_table = soup.select_one('table')
            for e_tr in e_table.select('.bg_white'):
                e_tds = e_tr.select('td')
                major_code = e_tds[1].get_text()
                major_name = e_tds[2].get_text()
                subjects_group = [subject_group.strip()
                                  for subject_group in e_tds[3].get_text().split(';')]
                point = e_tds[4].get_text()
                note = e_tds[5].get_text()
                for subject_group in subjects_group:
                    benchmark_obj = {
                        'major_code': major_code,
                        'major_name': major_name,
                        'subject_group': subject_group,
                        'point': point,
                        'note': note,
                        'year': year
                    }
                    logger.info(benchmark_info)
                    benchmark_data.append(benchmark_obj)
            return benchmark_data
        except Exception as e:
            logger.error(f'Error fetching data from {self.url}: {e}')

    def save_data(self, data):
        self.dir_manager.save_data(data)

    def crawl(self):
        html = self.fetch_data()
        data = self.extract_data(html)
        self.save_data(data)


if __name__ == '__main__':
    universities_data_manager = BaseManagementCSVData(
        './raw/universities_information', 'universities_information.csv', os, pd)

    universities_data = universities_data_manager.load_data()

    for year in range(2019, 2024):
        for university_data in universities_data:
            benchmark_data_manager = BaseManagementCSVData(f'./raw/benchmark/benchmark{year}', f'{university_data["university_code"]}_{year}.csv', os, pd)
            url = f'{university_data.get('url')}?y={year}'
            benchmark_info = BenchmarkCrawler(requests, url, benchmark_data_manager)
            benchmark_info.crawl()
