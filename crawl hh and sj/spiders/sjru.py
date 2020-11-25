# -*- coding: utf-8 -*-
import json
import scrapy
from scrapy.http import HtmlResponse
from vacancies_parser.items import VacanciesParserItem


class SjruSpider(scrapy.Spider):
    name = 'sjru'
    allowed_domains = ['superjob.ru']
    start_urls = ['https://russia.superjob.ru/vacancy/search/?keywords=' + 'Python']

    def __init__(self,search):
        self.start_urls = [f'https://russia.superjob.ru/vacancy/search/?keywords={search}']

    def parse(self, response: HtmlResponse):
        next_page = response.xpath(
            "//a[@class='icMQ_ _1_Cht _3ze9n f-test-button-dalshe f-test-link-Dalshe']/@href").extract_first()
        if next_page is None:
            yield
        yield response.follow(next_page, callback=self.parse)

        vacancy_list = response.xpath("//div[@class='_3zucV _2GPIV f-test-vacancy-item i6-sc _3VcZr']//div["
                                      "@class='_2g1F-']/a/@href").extract()

        for link in vacancy_list:
            yield response.follow(link, callback=self.vacancy_parse)

    def vacancy_parse(self, response: HtmlResponse):
        vacancy_name = response.xpath("//h1[@class='_3mfro rFbjy s1nFK _2JVkc']/text()").extract()[0]
        vacancy_link = response.url
        company = ''.join(response.xpath("//h2[@class='_3mfro PlM3e _2JVkc _2VHxz _3LJqf _15msI']/text()").extract())
        vacancy_info_json = json.loads(response.xpath("//script[@type='application/ld+json']/text()")[1].extract())
        location = ''.join(response.xpath("//span[@class='_6-z9f']//span[@class='_3mfro _1hP6a _2JVkc']/text()").extract())
        site = 'superjob'
        # print(vacancy_name, vacancy_link, company, location)
        yield VacanciesParserItem(
            vacancy_name=vacancy_name,
            vacancy_link=vacancy_link,
            company=company,
            vacancy_info_json=vacancy_info_json,
            location=location,
            site=site
        )
