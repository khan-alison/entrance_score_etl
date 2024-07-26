from common.crawler_base import BaseCrawler
from common.management_data import BaseManagementCSVData
import requests
import os
import pandas as pd
from bs4 import BeautifulSoup
from helper.logger_helper import LoggerSimple

logger = LoggerSimple.get_logger(__name__)


class BenchmarkCrawler(BaseCrawler):
    def __init__(self, requests, url, dir_manager, year):
        self.requests = requests
        self.url = url
        self.dir_manager = dir_manager
        self.year = year

    def fetch_data(self):
        response = self.requests.get(self.url)
        if response.status_code == 200:
            return response.content
        else:
            logger.warning(f'{response.status_code} - {self.url}')
            return None

    def extract_data(self, html):
        benchmark_data = []
        try:
            soup = BeautifulSoup(html, 'html.parser')
            e_table = soup.select_one('table')
            for e_tr in e_table.select('.bg_white'):
                e_tds = e_tr.select('td')
                major_code = e_tds[1].get_text().strip()
                major_name = e_tds[2].get_text().strip()
                subjects_group = [subject_group.strip()
                                  for subject_group in e_tds[3].get_text().split(';')]
                point = e_tds[4].get_text().strip()
                note = e_tds[5].get_text().strip()

                if '\n' in major_name:
                    major_name_parts = major_name.split('\n')
                    base_name = major_name_parts[0].strip()
                    for part in major_name_parts[1:]:
                        if part.strip().startswith('-'):
                            cleaned_row = {
                                'major_code': major_code,
                                'major_name': part.strip('-').strip(),
                                'subject_group': subject_group,
                                'point': point,
                                'note': note,
                                'year': self.year
                            }
                            benchmark_data.append(cleaned_row)
                        else:
                            base_name += ' ' + part.strip()
                    benchmark_data.append({
                        'major_code': major_code,
                        'major_name': base_name,
                        'subject_group': subject_group,
                        'point': point,
                        'note': note,
                        'year': self.year
                    })
                else:
                    for subject_group in subjects_group:
                        benchmark_obj = {
                            'major_code': major_code,
                            'major_name': major_name,
                            'subject_group': subject_group,
                            'point': point,
                            'note': note,
                            'year': self.year
                        }
                        benchmark_data.append(benchmark_obj)
            return benchmark_data
        except Exception as e:
            logger.error(f'Error fetching data from {self.url}: {e}')
            return None

    def save_data(self, data):
        if data:
            self.dir_manager.save_data(data)
        else:
            logger.warning('No data to save.')

    def crawl(self):
        html = self.fetch_data()
        if html:
            data = self.extract_data(html)
            self.save_data(data)
        else:
            logger.warning('Failed to fetch HTML content.')


if __name__ == '__main__':
    universities_data_manager = BaseManagementCSVData(
        './raw/universities_information', 'universities_information.csv', os, pd)

    universities_data = universities_data_manager.load_data()

    for year in range(2019, 2024):
        for university_data in universities_data:
            benchmark_data_manager = BaseManagementCSVData(
                f'./raw/benchmark/benchmark{year}', f'{university_data["university_code"]}_{year}.csv', os, pd)
            url = f'{university_data.get('url')}?y={year}'
            benchmark_info = BenchmarkCrawler(
                requests, url, benchmark_data_manager, year)
            benchmark_info.crawl()
