# models.py
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Dict, Optional, Any
import uuid
import json


@dataclass
class Tag:
    """Класс для хранения информации о теге"""
    id: uuid.UUID
    name: str


@dataclass
class Chapter:
    """Класс для хранения информации о главе"""
    id: uuid.UUID
    name: str
    link: str
    votes: int
    views: int
    comments: int
    book_id: uuid.UUID
    published_at: datetime
    run_id: uuid.UUID

    def to_dict(self):
        return asdict(self)


@dataclass
class Book:
    """Класс для хранения информации о книге"""
    id: uuid.UUID
    name: str
    link: str
    votes: int
    views: int
    chapters: int
    duration: str
    is_mature: bool
    is_finished: bool
    author_id: uuid.UUID
    run_id: uuid.UUID
    rank_position: int
    rank_category: str
    tags: List[str] = None

    def to_dict(self):
        return asdict(self)


@dataclass
class Comment:
    """Класс для хранения информации о комментарии"""
    id: uuid.UUID
    parent_id: Optional[uuid.UUID]
    user_id: uuid.UUID
    chapter_id: uuid.UUID
    text: str
    published_at: datetime
    run_id: uuid.UUID

    def to_dict(self):
        return asdict(self)


@dataclass
class ParserRun:
    """Класс для хранения информации о запуске парсера"""
    id: uuid.UUID
    run_at: datetime
    run_time: int

    def to_dict(self):
        return asdict(self)


@dataclass
class StoryInfo:
    """Вспомогательный класс для парсера"""
    id: str
    title: str
    author: str
    description: str
    language: str
    url: str
    year_published: Optional[int] = None
    stats: Dict = None
    metadata: Dict = None
    parse_date: str = None
    chapters_count: int = 0
    words_count: int = 0
    read_count: int = 0
    vote_count: int = 0
    comment_count: int = 0
    mature: bool = False
    completed: bool = False
    tags: List[str] = None
    toc_parsed: bool = False
    comments_parsed: bool = False

    def to_dict(self):
        result = asdict(self)
        return result

    def to_book_model(self, run_id: uuid.UUID, author_id: uuid.UUID) -> Book:
        """Конвертирует в модель Book для БД"""
        return Book(
            id=uuid.uuid5(uuid.NAMESPACE_URL, self.url),
            name=self.title[:127],
            link=self.url[:255],
            votes=self.stats.get('votes', 0) if self.stats else 0,
            views=self.stats.get('reads', 0) if self.stats else 0,
            chapters=self.chapters_count,
            duration='PT0S',
            is_mature=self.mature,
            is_finished=self.completed,
            author_id=author_id,
            run_id=run_id,
            rank_position=0,
            rank_category='',
            tags=self.tags or []
        )


@dataclass
class ChapterInfo:
    """Вспомогательный класс для парсера"""
    id: str
    number: int
    title: str
    url: str
    content: str = None
    comment_count: int = 0
    comments_data: List[Dict] = None  # Обычные комментарии
    inline_comments_data: List[Dict] = None  # Комментарии привязанные к параграфам
    word_count: int = 0

    def to_dict(self):
        return asdict(self)

    def to_chapter_model(self, book_id: uuid.UUID, run_id: uuid.UUID) -> Chapter:
        """Конвертирует в модель Chapter для БД"""
        return Chapter(
            id=uuid.uuid5(uuid.NAMESPACE_URL, self.url),
            name=self.title[:127],
            link=self.url[:255],
            votes=0,
            views=0,
            comments=self.comment_count,
            book_id=book_id,
            published_at=datetime.now(),
            run_id=run_id
        )