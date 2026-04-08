from dataclasses import dataclass
from typing import Optional


@dataclass
class Tag:
    name: str
    link: str

@dataclass
class Rank:
    category: str
    position: int
    amount_of_books: int # количество книг в теге
    

@dataclass
class User:
    username: str
    link: str

@dataclass
class Comment:
    text: str
    published_at: str
    #modified_at: str Думаю без этого можно, но пока пусть будет просто написано
    user: User
    likes: int
    num_replies: int 
    replies: Optional[list['Comment']] = None ###
    comm_id: Optional[str] = None # id комментария на ваттпаде; в парсере scrapy он парсится всегда

@dataclass
class Chapter:
    name: str
    link: str
    votes: int
    views: int
    num_comments: int
    published_at: str
    comments: Optional[list[Comment]] = None

@dataclass
class Book:
    name: str
    description: str
    link: str
    votes: int
    views: int
    is_mature: bool
    is_finished: bool
    author: User # или list[User], но вроде везде один аккаунт
    tags: list[Tag]
    num_chapters: int
    chapters: list[Chapter]
    ranks: Optional[list[Rank]] = None
    duration: Optional[str] = None # на русских книгах нет его
    id: Optional[str] = None # id книги на ваттпаде; в парсере scrapy он парсится всегда