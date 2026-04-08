from dataclasses import dataclass
from typing import Optional

@dataclass
class Genre:
    name: str

    link: str
    position: str

@dataclass
class Tag: # Графа "В тексте есть"
    name: str

    link: Optional[str] = None

@dataclass
class User:
    username: str
    link: str

@dataclass
class Comment:
    text: str
    published_at: str
    user: User

@dataclass
class Chapter:
    name: str
    publication_date: Optional[str] = None

@dataclass
class Reward:
    type: str
    amount: int

@dataclass
class Book:
    name: str
    link: str
    rating: int
    likes: int
    views: int
    publication_start_date: str
    num_comments: int

    comments: list[Comment]
    authors: list[User]
    tags: list[Tag] 
    genres: list[Genre]
    rewards: list[Reward]
  #  chapters: list[Chapter]

    id: Optional[str] = None # id книги на litnet
    cycle: Optional[str] = None
    publication_end_date: Optional[str] = None
    cycle: Optional[str] = None
    times_saved_to_library: Optional[int] = None
    price: Optional[int] = None # 0 если бесплатная, >0 если платная, None если не спарсилось
    contains_profanity: Optional[bool] = None #True/false если спарсилось
    age_restriction: Optional[str] = None
    is_finished: Optional[bool] = None  #True/false если спарсилось
    description: Optional[str] = None



