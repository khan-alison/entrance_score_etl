from common.crawler_base import BaseCrawler
from helper.logger_helper import LoggerSimple
from common.management_data import BaseManagementCSVData
import requests
import os
import pandas as pd
from helper.multithread_helper import multithread_helper

logger = LoggerSimple.get_logger(__name__)


class ExamResultCrawler(BaseCrawler):
    def __init__(self, requests, url, dir_manager):
        self.requests = requests
        self.url = url
        self.dir_manager = dir_manager

    def fetch_data(self, url):
        self.url = url
        response = self.requests.get(self.url)
        return response.json()

    def extract_data(self, id_number):
        url_api = f'https://d3ewbr0j99hudd.cloudfront.net/search-exam-result/2021/result/{
            id_number}.json'
        data_exam = None
        self.url = url_api
        try:
            data = self.fetch_data(self.url)
            logger.info(f'data{data.get('studentCode')}')
            if data.get('studentCode') is not None:
                data_exam = data
        except Exception as e:
            logger.error(e)
            logger.error(f'ERROR: sbd={id_number}')
        return data_exam

    def save_data(self, data):
        self.dir_manager.save_data(data)

    def crawl(self, id_number):
        raw = self.extract_data(id_number)
        return raw

    def build_id_number(self, provide_id, post_id):
        prefix = ''.join(['0' for i in range(6 - len(str(post_id)))])
        return f'{provide_id}{prefix}{post_id}'

    def get_min_max_by_code(self, provide_id='64'):
        min = 1
        max = 999999

        should_find = True
        min = int((max - min) / 2) + min
        while (should_find):
            if ((min - max) ** 2) == 1:
                break
            mid = int((max - min) / 2) + min
            id_number = self.build_id_number(
                provide_id=provide_id, post_id=mid)
            if self.extract_data(id_number) is None:
                max = mid
                continue
            else:
                min = mid
                continue
        return mid


if __name__ == "__main__":
    csv_manager = BaseManagementCSVData(
        './raw/exam_result', 'exam_result.csv', os, pd)
    list_provide = ['{0:02}'.format(num) for num in range(1, 65)]
    for provide_id in list_provide:
        logger.info(provide_id)
        exam_info = ExamResultCrawler(
            requests, url='', dir_manager=csv_manager)
        max_id_number = exam_info.get_min_max_by_code(provide_id)
        batch_size = 500
        group_name = None
        all_data = []

        for batch_start in range(1, 500, batch_size):
            batch_end = min(batch_start + batch_size, max_id_number)
            id_numbers = [exam_info.build_id_number(provide_id, post_id)
                          for post_id in range(batch_start, batch_end)]

            def fetch_exam_data(id_number):
                return exam_info.crawl(id_number)
            batch_data = multithread_helper(id_numbers, fetch_exam_data, max_workers=10)
            all_data.extend(batch_data)

        if all_data:
            csv_manager = BaseManagementCSVData(
                './raw/exam_result', f'{all_data[0].get('groupName').strip()}.csv', os, pd)
            csv_manager.save_data(all_data)
