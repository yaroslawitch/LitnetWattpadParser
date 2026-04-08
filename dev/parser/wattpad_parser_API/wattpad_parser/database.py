# database.py
import logging
from typing import List, Dict, Optional, Any
import uuid
from datetime import datetime
from models import Book, Chapter, Comment, Tag, ParserRun

# Настройка кодировки логгера
logging.basicConfig(level=logging.INFO, encoding='utf-8')
logger = logging.getLogger(__name__)


class DatabaseManager:
    """Менеджер для работы с файлами вместо БД"""

    def __init__(self):
        self.storage = {
            'books': [],
            'chapters': [],
            'comments': [],
            'runs': []
        }
        logger.info("Инициализирован файловый менеджер")

    def connect(self):
        """Устанавливает соединение (заглушка)"""
        logger.info("Подключение установлено (файловый режим)")

    def disconnect(self):
        """Закрывает соединение (заглушка)"""
        logger.info("Соединение закрыто")

    def save_parser_run(self, run_time: int) -> uuid.UUID:
        """Сохраняет информацию о запуске парсера"""
        run_id = uuid.uuid4()
        parser_run = ParserRun(
            id=run_id,
            run_at=datetime.now(),
            run_time=run_time
        )
        self.storage['runs'].append(parser_run)
        logger.info(f"Сохранен запуск парсера с ID: {run_id}")
        return run_id

    def save_book(self, book: Book) -> bool:
        """Сохраняет книгу в хранилище"""
        try:
            # Удаляем старую версию если есть
            self.storage['books'] = [b for b in self.storage['books'] if b.id != book.id]
            # Добавляем новую
            self.storage['books'].append(book)
            logger.debug(f"Сохранена книга в память: {book.name}")
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения книги {book.name}: {e}")
            return False

    def _save_book_tags(self, book_id: uuid.UUID, tags: List[str], run_id: uuid.UUID):
        """Сохраняет теги книги (в памяти)"""
        logger.debug(f"Сохранены теги для книги {book_id}: {tags}")

    def save_chapter(self, chapter: Chapter) -> bool:
        """Сохраняет главу в хранилище"""
        try:
            self.storage['chapters'].append(chapter)
            logger.debug(f"Сохранена глава в память: {chapter.name}")
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения главы {chapter.name}: {e}")
            return False

    def save_comment(self, comment: Comment) -> bool:
        """Сохраняет комментарий в хранилище"""
        try:
            self.storage['comments'].append(comment)
            logger.debug(f"Сохранен комментарий в память")
            return True
        except Exception as e:
            logger.error(f"Ошибка сохранения комментария: {e}")
            return False

    def get_existing_book(self, url: str) -> Optional[Book]:
        """Проверяет, существует ли книга в хранилище"""
        try:
            for book in self.storage['books']:
                if book.link == url:
                    return book
            return None
        except Exception as e:
            logger.error(f"Ошибка получения книги из хранилища: {e}")
            return None

    def get_all_books(self) -> List[Book]:
        """Возвращает все книги из хранилища"""
        return self.storage['books']

    def get_all_chapters(self) -> List[Chapter]:
        """Возвращает все главы из хранилища"""
        return self.storage['chapters']

    def get_all_comments(self) -> List[Comment]:
        """Возвращает все комментарии из хранилища"""
        return self.storage['comments']