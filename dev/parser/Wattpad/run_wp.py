import sys
sys.path.append('../..')
from domain.wp_config import PARSER, COOKIES, HEADERS
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import json
from Wattpad.get_cookies import load_cookies_headers
import subprocess

def run_spider():
    '''Запуск выбранного паука'''
    cmd = f'scrapy crawl {PARSER}'
    return subprocess.run(cmd)

def check_log_in():
    '''Запуск паука, проверяющего авторизацию'''
    cmd = 'scrapy crawl checker'
    return subprocess.run(cmd)
    


if __name__ == '__main__':
    while True:
        check_log_in()
        with open('Wattpad/get_cookies/check.jsonl', 'r', encoding='utf8') as f:
            content = f.read()
        login_indicator = json.loads(content)
        if login_indicator['button'] == None:
            print('\n\nТокен входа работает.\nЗапуск парсера...')
            run_spider()
            break
        else:                   # Включение Селениума
            print('\n\nТокен нерабочий или отсутствует.\nЗапуск selenium...')
            from Wattpad.get_cookies.selenium_log_in import *