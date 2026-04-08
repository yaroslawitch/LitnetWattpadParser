# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.

# Здесь будут некоторые функции, нужные для преобразования данных прямо во время парсинга
from datetime import datetime
import re

def to_datetime(parsed_date: str):
    '''Приводит строку в datetime формат'''
    try:
        dt = re.search(r'(?<=,\s)[^,]+,[^,]+$', parsed_date).group()

        try:
            format_ = '%b %d, %Y'
            formatted_date = datetime.strptime(dt, format_)
        except:
            months = {'нояб.': '11', 'дек.': '12', 'янв.': '1', 'фев.': '2',
                      'мар.': '3', 'апр.': '4', 'мая': '5', 'июн.': '6',
                      'июл.': '7', 'авг.': '8', 'сент.': '9', 'окт.': '10'}
            k = list(months.keys())
            format_ = '%m %d, %Y'
            got_date = dt
            for m in k:
                got_date = re.sub(m, months[m], got_date)
            formatted_date = datetime.strptime(got_date, format_)
    except:
        dt = datetime.now() # если обновлено в течение 24 часов
        formatted_date = datetime(dt.year, dt.month, dt.day)
    return formatted_date
    
def drop_duplicates(raw_list: list):
    '''Удаляет дублеты с сохранением порядка'''
    cleaned_list = []
    for element in raw_list:
        if element not in cleaned_list:
            cleaned_list.append(element)
    return cleaned_list

def assemble_start_urls(TAGS: list):
    '''Собирает ссылки на выдачу книг по тегам'''
    START_URLS = []
    for tag in TAGS:
        START_URLS.append('https://www.wattpad.com/stories/' + tag)
    return START_URLS
    
    