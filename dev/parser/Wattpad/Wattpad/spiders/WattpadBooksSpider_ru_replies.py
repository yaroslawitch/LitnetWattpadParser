import scrapy
from scrapy.exceptions import CloseSpider
import json
import re
import sys
sys.path.append('../..')
from datetime import datetime
from Wattpad.spiders.__init__ import * # здесь функции для обработки данных
from domain.wp_config import OVERWRITE_FILE, TAGS_RU, MARGIN_DATE, COOKIES, HEADERS # импорт конфигурационного файла
from Wattpad.selectors.selectors import SELECTORS

class WattpadRepliesSpiderRU(scrapy.Spider):
    name = "WattpadParserRU_replies"
    
    custom_settings = {
        'JOBDIR': f'sessions/{name}',
        'FEEDS': {
            f'crawled_data/{name}.jsonl': {
                'format': 'jsonl',
                'overwrite': OVERWRITE_FILE
                }
        }
    }
    
    headers = {
        'Referer': 'https://www.wattpad.com'
        } # заголовки запросов
    
    cookies={
        'lang': 7,
        'locale': 'ru_RU',
        'X-Time-Zone': 'Asia%2FTomsk'
        } # Куки языка и тайм зоны

    def __init__(self, headers=HEADERS, cookies=COOKIES, seen_ajax=[], seen_books=[], margin_date=MARGIN_DATE):

        try:
            self.cookies.update(cookies)
            self.cookies['token'] = self.cookies['token']
        except KeyError:
            raise CloseSpider('Токен авторизации не найден')
        try:
            self.headers.update(headers)
            self.headers['User-Agent'] = self.headers['User-Agent']
        except KeyError:
            raise CloseSpider('Не найден UserAgent')        
        self.seen_ajax = seen_ajax
        self.seen_books = seen_books
        self.margin_date = margin_date
    
    def start_requests(self, tags=TAGS_RU):
    
        self.start_urls = assemble_start_urls(tags)
        for index, url in enumerate(self.start_urls):
            yield scrapy.Request(
                url=url,
                headers=self.headers,
                cookies=self.cookies,
                callback=self.parse,
                meta={'url_index': index} # индекс текущего start_urls
            )
    
    def parse(self, response):
        current_url = response.url
        self.headers['Referer'] = current_url
        genre = re.search(r'(?<=wattpad\.com/stories/).+$', current_url).group()
        ajax_url = f'https://api.wattpad.com/v5/hotlist?tags={genre}&language=7&offset=0&limit=20'
        if ajax_url not in self.seen_ajax:
            self.seen_ajax.append(ajax_url)
            yield scrapy.Request(         # отправка ajax запроса на получение количества книг в жанре
                            url=ajax_url,
                            method='GET',
                            headers=response.headers,
                            cookies=self.cookies,
                            callback=self.parse_ajax_response,
                            meta={'genre': genre}
                        )

    def parse_ajax_response(self, response):    # функция, которая подаётся в callback и работает "внутри" запроса
                                                # она распаршивает получаемую json структуру
        data = json.loads(response.body)
        for booka in data.get("stories",[]):
            book_data = {'title': booka.get('title'), # можно спарсить название книги и некоторое другое
                        'description': booka.get('description'),
                        'url': booka.get('url'), # но самое главное - спарсить ссылку/часть ссылки на книгу
                        'author': booka.get('user', {}).get('name'),
                        'completed': booka.get('completed'),
                        'tags': booka.get('tags'),
                        'mature': booka.get('mature'),
                        'voteCount': booka.get('voteCount'),
                        'readCount': booka.get('readCount'),
                        'numParts': booka.get('numParts'),
                        'id': booka.get('id')
                      }
            if book_data['id'] not in self.seen_books:
                self.seen_books.append(book_data['id'])
                yield scrapy.Request(
                            url=book_data['url'],
                            headers=response.headers,
                            cookies=self.cookies,
                            callback=self.parse_chapters_links,
                            meta={'book_data': book_data}
                            )
        try:
            next_url = data['nextUrl']
            yield scrapy.Request(
                        url=next_url,
                        headers=response.headers,
                        cookies=self.cookies,
                        callback=self.parse_ajax_response
                        )
        except KeyError: # выход из рекурсии
            pass
            
                        
    def parse_chapters_links(self, response):
        book_data = response.meta['book_data']
        
        chapters = response.css(SELECTORS['chapter_node'])
        chapters_info = []
        for chapter in chapters:
            name = chapter.css(SELECTORS['chapter_name']).get()
            link = chapter.css(SELECTORS['chapter_link']).get()
            publication_date = chapter.css(SELECTORS['ch_date']).get()
            chapters_info.append({'name': name, 'link': link, 'publication_date': publication_date})
        
        chapters_info = drop_duplicates(chapters_info)
                
        for i in range(len(chapters_info)):
            chapters_info[i]['publication_date'] = to_datetime(chapters_info[i]['publication_date'])
            
        if chapters_info[-1]['publication_date'] < datetime(2026, 1, 1): # если последняя глава книги
            pass                                                         # старше указанной даты
        
        else:
            book_data['chapters_info'] = chapters_info
            yield scrapy.Request(
                        url='https://www.wattpad.com/story/'+ str(book_data['id']) + '/rankings',
                        headers=response.headers,
                        cookies=self.cookies,
                        callback=self.parse_awards,
                        meta={'book_data': book_data}
                        )

    def parse_awards(self, response):
        common_elements = response.css(SELECTORS['award_node'])

        awards_info = []
        for element in common_elements:
            
            place = element.css(SELECTORS['place']).get()
            tag = element.css(SELECTORS['tag_name']).get()
            among = element.css(SELECTORS['among']).get()
            awards_info.append({'place': place,
                                'tag': tag,
                                'among': among
                                })
        book_data = response.meta['book_data']
        book_data['awarded_tags'] = awards_info
        
        yield scrapy.Request(
                    url=book_data['chapters_info'][0]['link'],
                    headers=self.headers,
                    cookies=self.cookies,
                    callback=self.parse_chapter_meta,
                    meta={'book_data': book_data, 'ch_indx': 0}
                    )
                    
    def parse_chapter_meta(self, response):
        book_data = response.meta['book_data']
        ch_indx = response.meta['ch_indx']
        
        
        reads, votes, comments = response.css(SELECTORS['chapter_meta'])
        
        readsCount = reads.css(SELECTORS['readsCount']).get() # в title находится точное число, если > 1000
        if readsCount == None:
            readsCount = reads.get()
            readsCount = re.search(r'</span>\s*\d+\s*</span>$', readsCount).group() #
        votesCount = votes.css(SELECTORS['votesCount']).get()               #эти элементы не вытаскиваются 
        if votesCount == None:                                      #всегда просто через ::text,
            votesCount = votes.get()                                #поэтому использован re
            votesCount = re.search(r'</span>\s*\d+\s*</span>$', votesCount).group() # 
        commentsCount = comments.css(SELECTORS['commentsCount']).get()
        if commentsCount == None:
            commentsCount = comments.css(SELECTORS['commentsCountAlt']).get()
        
        readsCount = int(re.sub(r'[^\d]', '', readsCount))
        votesCount = int(re.sub(r'[^\d]', '', votesCount))
        commentsCount = int(re.sub(r'[^\d]', '', commentsCount))
        
        book_data['chapters_info'][ch_indx]['votesCount'] = votesCount
        book_data['chapters_info'][ch_indx]['readsCount'] = readsCount
        book_data['chapters_info'][ch_indx]['commentsCount'] = commentsCount
        
        if ch_indx + 1 == len(book_data['chapters_info']): # когда последняя глава спаршена, выход из рекурсии
            yield next(self.parse_chapter_comms(response=response, book_data=book_data, ch_indx=0))# book_data
        
        else:
            ch_indx += 1
            yield scrapy.Request(
                        url=book_data['chapters_info'][ch_indx]['link'],
                        headers=self.headers,
                        cookies=self.cookies,
                        callback=self.parse_chapter_meta,
                        meta={'book_data': book_data, 'ch_indx': ch_indx}
                        )
        

    def parse_chapter_comms(self, response, book_data, ch_indx):
        #book_data = response.meta['book_data']
        
        if ch_indx == len(book_data['chapters_info']): # условие на завершение и сохранение информации о книге
            #yield book_data
            yield next(self.parse_chapter_replies(response=response, book_data=book_data, ch_indx=0, comm_indx=0))
        else:
            book_data['chapters_info'][ch_indx]['comments'] = []
            ch_id = re.search(r'(?<=https://www\.wattpad\.com/)\d+(?=-)', book_data['chapters_info'][ch_indx]['link']).group()
            yield scrapy.Request(
                        url=f'https://www.wattpad.com/v5/comments/namespaces/parts/resources/{ch_id}/comments?limit=100',
                        headers=self.headers,
                        cookies=self.cookies,
                        callback=self.parse_comments,
                        meta={'book_data': book_data, 'ch_indx': ch_indx, 'ch_id': ch_id}
                        )
    
    def parse_comments(self, response):
        book_data, ch_indx, ch_id = response.meta['book_data'], response.meta['ch_indx'], response.meta['ch_id']
        data = json.loads(response.body)
        
        comments = []
        for comment in data.get('comments', []):
            comments.append({
                'created': comment.get('created'),
                'modified': comment.get('modified'),
                'replyCount': comment.get('replyCount'),
                'text': comment.get('text'),
                'username': comment.get('user', {}).get('name'),
                'likes': comment.get('sentiments', {}).get(':like:', {}).get('count', 0),
                'commentId': comment.get('commentId', {}).get('resourceId')
                    })
        book_data['chapters_info'][ch_indx]['comments'].extend(comments)

        try:
            after_id = data.get('pagination', {}).get('after', {})['resourceId']
            yield scrapy.Request(
                        url=f'https://www.wattpad.com/v5/comments/namespaces/parts/resources/{ch_id}/comments?after={after_id}&limit=100',
                        headers=self.headers,
                        cookies=self.cookies,
                        callback=self.parse_comments,
                        meta={'book_data': book_data, 'ch_indx': ch_indx, 'ch_id': ch_id}
                        )
        except KeyError: # парсинг следующей главы
            yield next(self.parse_chapter_comms(response=response, book_data=book_data, ch_indx=ch_indx + 1))

    def parse_chapter_replies(self, response, book_data, ch_indx, comm_indx):
        while True:
           #book_data = response.meta['book_data']
            try:
                if ch_indx == len(book_data['chapters_info']):
                    yield book_data
                elif comm_indx == len(book_data['chapters_info'][ch_indx]['comments']):
                    ch_indx += 1
                    comm_indx = 0
                    #yield next(self.parse_chapter_replies(response=response, book_data=book_data, ch_indx=ch_indx, comm_indx=comm_indx))
                    continue
                else:
 
                    if  book_data['chapters_info'][ch_indx]['comments'][comm_indx]['replyCount'] > 0:
                        comm_id = book_data['chapters_info'][ch_indx]['comments'][comm_indx]['commentId']
                        yield scrapy.Request(
                                    url=f'https://www.wattpad.com/v5/comments/namespaces/comments/resources/{comm_id}/comments?limit=100',
                                    headers=self.headers,
                                    cookies=self.cookies,
                                    callback=self.parse_replies,
                                    meta={'book_data': book_data, 'ch_indx': ch_indx, 'comm_indx': comm_indx,
                                            'comm_id': comm_id
                                                }
                                    )
                        break
                    else:
                        comm_indx += 1
                        #yield next(self.parse_chapter_replies(response=response, book_data=book_data, ch_indx=ch_indx, comm_indx=comm_indx))
                        continue
            except IndexError: # если 0 комментов у главы, переходит к следующей
                ch_indx += 1
                comm_indx = 0
                #yield next(self.parse_chapter_replies(response=response, book_data=book_data, ch_indx=ch_indx, comm_indx=comm_indx))
                continue
             
    def parse_replies(self, response):
        book_data, ch_indx, comm_indx = response.meta['book_data'], response.meta['ch_indx'], response.meta['comm_indx']
        comm_id = response.meta['comm_id']
        data = json.loads(response.body)
        book_data['chapters_info'][ch_indx]['comments'][comm_indx]['replies'] = []
        replies = []
        for reply in data.get('comments', []):
            replies.append({
                'created': reply.get('created'),
                'modified': reply.get('modified'),
                'replyCount': reply.get('replyCount'),
                'text': reply.get('text'),
                'username': reply.get('user', {}).get('name'),
                'likes': reply.get('sentiments', {}).get(':like:', {}).get('count', 0),
                'commentId': reply.get('commentId', {}).get('resourceId')
                    })
        book_data['chapters_info'][ch_indx]['comments'][comm_indx]['replies'].extend(replies)
        
        try:
            after_id = data.get('pagination', {}).get('after', {})['resourceId']
            yield scrapy.Request(
                        url=f'https://www.wattpad.com/v5/comments/namespaces/comments/resources/{comm_id}/comments?after={after_id}&limit=100',
                        headers=self.headers,
                        cookies=self.cookies,
                        callback=self.parse_replies,
                        meta={'book_data': book_data, 'ch_indx': ch_indx, 'comm_indx': comm_indx, 'comm_id': comm_id}
                        )
        except KeyError: # парсинг следующего коммента
            yield next(self.parse_chapter_replies(response=response, book_data=book_data, ch_indx=ch_indx, comm_indx=comm_indx + 1))