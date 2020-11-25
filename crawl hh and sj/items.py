# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class VacanciesParserItem(scrapy.Item):
    # define the fields for your item here like:
    _id = scrapy.Field()
    site = scrapy.Field()
    vacancy_name = scrapy.Field()
    location = scrapy.Field()
    vacancy_link = scrapy.Field()
    company = scrapy.Field()
    vacancy_info_json = scrapy.Field()
    salary_min = scrapy.Field()
    salary_max = scrapy.Field()
    currency = scrapy.Field()