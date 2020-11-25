# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
from pymongo import MongoClient
from vacancies_parser.runner import search_input
import csv


class VacanciesParserPipeline(object):
    def __init__(self):
        client = MongoClient('localhost', 27017)
        self.mongobase = client.vacansies

    def process_item(self, item, spider):
        collection = self.mongobase[search_input]

        if spider.name == 'sjru':
            item['currency'] = item['vacancy_info_json']['baseSalary']['currency']
            try:
                item['salary_min'] = item['vacancy_info_json']['baseSalary']['value']['minValue']
            except KeyError:
                item['salary_min'] = None
            try:
                item['salary_max'] = item['vacancy_info_json']['baseSalary']['value']['maxValue']
            except KeyError:
                item['salary_max'] = None

        elif spider.name == 'hhru':
            if item['vacancy_info_json']['vac_salary_cur'] == '':
                item['currency'] = None
            elif item['vacancy_info_json']['vac_salary_cur'] == 'RUR':
                item['currency'] = 'RUB'
            else:
                item['currency'] = item['vacancy_info_json']['vac_salary_cur']

            if item['vacancy_info_json']['vac_salary_from'] == '':
                item['salary_min'] = None
            else:
                item['salary_min'] = int(item['vacancy_info_json']['vac_salary_from'])

            if item['vacancy_info_json']['vac_salary_to'] == '':
                item['salary_max'] = None
            else:
                item['salary_max'] = int(item['vacancy_info_json']['vac_salary_to'])

        collection.insert_one(item)
        return item


class CSVPipeline():
    def __init__(self):
        self.file = open(f'database_{search_input}.csv', 'w', encoding='utf-8').close()

        self.file = f'database_{search_input}.csv'
        with open(self.file, 'r', newline='') as csv_file:
            self.tmp_data = csv.DictReader(csv_file).fieldnames

        self.csv_file = open(self.file, 'a', newline='', encoding='UTF-8')

    def __del__(self):
        self.csv_file.close()

    def process_item(self, item, spider):
        columns = item.fields.keys()

        data = csv.DictWriter(self.csv_file, columns)
        if not self.tmp_data:
            data.writeheader()
            self.tmp_data = True
        data.writerow(item)
        return item
