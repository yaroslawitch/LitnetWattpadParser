import scrapy
from scrapy.exceptions import CloseSpider
import json
import sys
sys.path.append('../..')
from Wattpad.selectors.selectors import SELECTORS
from Wattpad.spiders.__init__ import * # здесь функции для обработки данных
from domain.wp_config import COOKIES, HEADERS # импорт конфигурационного файла

class CheckSpider(scrapy.Spider):
    name = "checker"
    custom_settings = {
        'FEEDS': {
            'Wattpad/get_cookies/check.jsonl': {
                'format': 'jsonl',
                'overwrite': True,
                }
        }
    }
    start_urls = ['https://www.wattpad.com']
    
    headers = {
        'Referer': 'https://www.wattpad.com'
        } # заголовки запросов
    
    cookies={
        'lang': 1,
        'locale': 'en_US',
        'X-Time-Zone': 'Asia%2FTomsk'
        } # Куки языка и тайм зоны

    def __init__(self, headers=HEADERS, cookies=COOKIES):
        self.cookies = cookies
        self.headers = headers
    
    def start_requests(self):
        yield scrapy.Request(
            url=self.start_urls[0],
            headers=self.headers,
            cookies=self.cookies,
            callback=self.parse
            )
            
    def parse(self, response):
        button = response.css('button.Rer7C.transparent-button.iUT2X::text').get()
        yield {'button': button}