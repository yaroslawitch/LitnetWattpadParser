# cache.py
"""
Простой файловый кэш для хранения уже спарсенных URL.
Позволяет избежать повторного парсинга одних и тех же книг.
"""

import json
import os
import logging
from typing import Set
from config import WEB_BASE_URL

logger = logging.getLogger(__name__)

CACHE_FILE = "parsed_urls_cache.json"


class URLCache:
    """Кэш для хранения уже обработанных URL"""
    
    def __init__(self, cache_file: str = CACHE_FILE):
        self.cache_file = cache_file
        self.cached_urls: Set[str] = set()
        self._load()
    
    def _load(self) -> None:
        """Загружает кэш из файла"""
        if os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.cached_urls = set(data.get('urls', []))
                logger.info(f"Загружено {len(self.cached_urls)} URL из кэша")
            except Exception as e:
                logger.warning(f"Ошибка загрузки кэша: {e}")
                self.cached_urls = set()
        else:
            self.cached_urls = set()
    
    def save(self) -> None:
        """Сохраняет кэш в файл"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump({'urls': list(self.cached_urls)}, f, ensure_ascii=False, indent=2)
            logger.debug(f"Кэш сохранён: {len(self.cached_urls)} URL")
        except Exception as e:
            logger.error(f"Ошибка сохранения кэша: {e}")
    
    def is_cached(self, url: str) -> bool:
        """Проверяет, есть ли URL в кэше"""
        # Нормализуем URL (убираем trailing slash)
        normalized = url.rstrip('/')
        return normalized in self.cached_urls
    
    def add(self, url: str) -> None:
        """Добавляет URL в кэш"""
        normalized = url.rstrip('/')
        self.cached_urls.add(normalized)
    
    def add_and_save(self, url: str) -> None:
        """Добавляет URL в кэш и сохраняет файл"""
        self.add(url)
        self.save()
    
    def clear(self) -> None:
        """Очищает кэш"""
        self.cached_urls = set()
        self.save()
        logger.info("Кэш очищен")
    
    def get_stats(self) -> dict:
        """Возвращает статистику кэша"""
        return {
            'total_urls': len(self.cached_urls),
            'cache_file': self.cache_file
        }


# Глобальный экземпляр кэша для удобства
_cache_instance = None

def get_cache() -> URLCache:
    """Возвращает глобальный экземпляр кэша"""
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = URLCache()
    return _cache_instance
