from dataclasses import dataclass
from typing import Optional
from urllib.parse import unquote
from wattpad_models import Tag, Rank, User, Comment, Chapter, Book
import re
from datetime import datetime

### Здесь задаются функции для Предобработки значений

def clean_text(value: str | None) -> str | None:
    if type(value) != str:
        return value
    _ARTIFACTS_RE = re.compile(r"[\x00-\x1f\xa0]+")
    value = _ARTIFACTS_RE.sub(" ", value)
    value = re.sub(r"\s{2,}", " ", value)

    return value.strip()

def preproc(book_data):
    '''приводит данные к нужным классам и удаляет артефакты'''
    book_data['url'] = unquote(book_data['url']) # декодируем из url формата
    book_data['author_url'] = "https://www.wattpad.com/user/" + book_data['author'] # ссылка на профиль
    try:
        book_data['read_time'] = re.sub('h', ' hours', book_data['read_time']) # приведение в вид interval
        book_data['read_time'] = re.sub('m', ' minutes', book_data['read_time'])
    except KeyError:
        pass
    
    if len(book_data['awarded_tags']) > 0: # приведение рангов в int
        for i in range(len(book_data['awarded_tags'])):
            book_data['awarded_tags'][i]['place'] = int(book_data['awarded_tags'][i]['place'])
            if "K" in book_data['awarded_tags'][i]['among']:
                book_data['awarded_tags'][i]['among'] = int(float(re.sub(r'[^\d\.]', '', book_data['awarded_tags'][i]['among']))*1000)
            elif "M" in book_data['awarded_tags'][i]['among']:
                book_data['awarded_tags'][i]['among'] = int(float(re.sub(r'[^\d\.]', '', book_data['awarded_tags'][i]['among']))*1000000)
            else:
                try:
                    book_data['awarded_tags'][i]['among'] = int(re.sub(r'[^\d\.]', '', book_data['awarded_tags'][i]['among']))
                except ValueError:
                    book_data['awarded_tags'][i]['among'] = int(float(re.sub(r'[^\d\.]', '', book_data['awarded_tags'][i]['among']))*1000)
    
    for i in range(len(book_data['tags'])): # добавление ссылки на тег
        book_data['tags'][i] = {'name': book_data['tags'][i], 'link': 'https://www.wattpad.com/stories/'+book_data['tags'][i]}
        book_data['tags'][i]['link'] = unquote(book_data['tags'][i]['link'])
        book_data['tags'][i]['name'] = unquote(book_data['tags'][i]['name'])
    
    #dt_pattern = '%Y-%m-%d %H:%M:%S'
    for i in range(len(book_data['chapters_info'])): # у даты публикации главы оставляем только дату
        #book_data['chapters_info'][i]['publication_date'] = str(datetime.strptime(book_data['chapters_info'][i]['publication_date'], dt_pattern).date())
        book_data['chapters_info'][i]['publication_date'] = book_data['chapters_info'][i]['publication_date'].date()
        book_data['chapters_info'][i]['link'] = unquote(book_data['chapters_info'][i]['link'])
        if len(book_data['chapters_info'][i]['comments']) > 0: # Ссылки на профили комментаторов
            for j in range(len(book_data['chapters_info'][i]['comments'])):
                book_data['chapters_info'][i]['comments'][j]['user_link'] = "https://www.wattpad.com/user/"+book_data['chapters_info'][i]['comments'][j]['username']
                try:
                    if len(book_data['chapters_info'][i]['comments'][j]['replies']) > 0:
                        for m in range(len(book_data['chapters_info'][i]['comments'][j]['replies'])):
                            book_data['chapters_info'][i]['comments'][j]['replies'][m]['user_link'] = "https://www.wattpad.com/user/"+book_data['chapters_info'][i]['comments'][j]['replies'][m]['username']
                except KeyError:
                    pass
    return book_data

def recursive_clean(book_data):
    '''Пробегается по всем значениям и элементам и удаляет артефакты в str'''
    for k in list(book_data.keys()):
        if type(book_data[k]) == str:
            book_data[k] = clean_text(book_data[k])
        elif type(book_data[k]) == int:
            continue
        elif type(book_data[k]) == list and len(book_data[k]) > 0:
            for i in range(len(book_data[k])):
                book_data[k][i] = recursive_clean(book_data[k][i])
        elif type(book_data[k]) == dict:
            book_data[k] = recursive_clean(book_data[k])
        else:
            continue
    return book_data



def items_to_Book_class(book_data):
    '''Конвертирует данные о книге из словаря в класс Book'''
    tags = [Tag(tag['name'], tag['link']) for tag in book_data['tags']] # конвертирует теги
    ranks = [Rank(rank['tag'], rank['place'], rank['among']) for rank in book_data['awarded_tags']] # конвертирует топовые теги
    
    author = User(book_data['author'], book_data['author_url']) # конвертирует инфо об авторе
    
    chapters = []
    for ch in book_data['chapters_info']: # Конвертирует главу
        if len(ch['comments']) > 0:
            usual_comments = []
            for comm in ch['comments']: # комментарии
                comm_user = User(comm['username'], comm['user_link'])
                replies = None
                if 'replies' in list(comm.keys()):
                    replies = []
                    for repl in comm['replies']:
                        repl_user = User(repl['username'], repl['user_link']) # комментарии-ответы
                        replies.append(Comment(repl['text'], repl['created'], repl_user, repl['likes'], repl['replyCount'], None, repl['commentId']))
                usual_comments.append(Comment(comm['text'], comm['created'], comm_user, comm['likes'], comm['replyCount'], replies, comm['commentId']))
            chapters.append(Chapter(ch['name'], ch['link'], ch['votesCount'], ch['readsCount'], ch['commentsCount'], ch['publication_date'], usual_comments))
        else:
            chapters.append(Chapter(ch['name'], ch['link'], ch['votesCount'], ch['readsCount'], ch['commentsCount'], ch['publication_date'], None))
    
    # сбор книги 
    book = Book(book_data['title'], book_data['description'], book_data['url'], book_data['voteCount'], book_data['readCount'], book_data['mature'],
            book_data['completed'], author, tags, book_data['numParts'], chapters, ranks, book_data.get('read_time', None), book_data['id']
            )

    return book