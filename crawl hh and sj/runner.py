from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from vacancies_parser import settings
from vacancies_parser.spiders.hhru import HhruSpider
from vacancies_parser.spiders.sjru import SjruSpider

# search_input = input('Please enter the search: ')
search_input = 'Data analyst'

if __name__ == '__main__':
    crawler_settings = Settings()
    crawler_settings.setmodule(settings)
    process = CrawlerProcess(settings=crawler_settings)
    process.crawl(HhruSpider, search=search_input)
    process.crawl(SjruSpider, search=search_input)
    process.start()
