# async_client.py
"""
Асинхронный клиент Wattpad API на базе aiohttp.
Позволяет выполнять параллельные запросы для ускорения парсинга.
"""

import aiohttp
import asyncio
import logging
from typing import Dict, List, Optional, Any

from config import API_BASE_URL_V3, API_BASE_URL_V5

logger = logging.getLogger(__name__)


class AsyncWattpadClient:
    """Асинхронный клиент для Wattpad API"""
    
    def __init__(self, cookies: Dict = None):
        self.cookies = cookies or {}
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Возвращает или создаёт сессию"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                headers=self.headers,
                cookies=self.cookies
            )
        return self._session
    
    async def close(self) -> None:
        """Закрывает сессию"""
        if self._session and not self._session.closed:
            await self._session.close()
    
    async def _make_request(self, url: str, params: Dict = None) -> Dict:
        """Выполняет GET запрос"""
        session = await self._get_session()
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 429:
                    logger.warning("Rate limit, ждём 5 секунд...")
                    await asyncio.sleep(5)
                    return await self._make_request(url, params)
                
                if response.status == 200:
                    return await response.json()
                else:
                    logger.warning(f"HTTP {response.status} для {url}")
                    return {}
        except Exception as e:
            logger.error(f"Ошибка запроса {url}: {e}")
            return {}
    
    async def get_story_details(self, story_id: str) -> Dict:
        """Получение информации о книге"""
        url = f"{API_BASE_URL_V3}/stories/{story_id}"
        return await self._make_request(url)
    
    async def get_part_comments(self, part_id: str, limit: int = 20, offset: int = 0) -> List[Dict]:
        """Получение комментариев к главе"""
        url = f"{API_BASE_URL_V5}/comments/namespaces/parts/resources/{part_id}/comments"
        params = {'limit': limit, 'offset': offset}
        data = await self._make_request(url, params)
        return data.get('comments', [])
    
    async def get_comment_replies(self, comment_id: Any) -> List[Dict]:
        """Получение ответов на комментарий"""
        if isinstance(comment_id, dict):
            resource_id = comment_id.get('resourceId')
        else:
            resource_id = comment_id
        
        url = f"{API_BASE_URL_V5}/comments/namespaces/comments/resources/{resource_id}/comments"
        data = await self._make_request(url)
        return data.get('comments', [])
    
    async def fetch_multiple_stories(self, story_ids: List[str]) -> List[Dict]:
        """Параллельное получение информации о нескольких книгах"""
        tasks = [self.get_story_details(sid) for sid in story_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Фильтруем ошибки
        valid_results = []
        for r in results:
            if isinstance(r, dict) and r:
                valid_results.append(r)
            elif isinstance(r, Exception):
                logger.error(f"Ошибка при получении книги: {r}")
        
        return valid_results
    
    async def fetch_comments_for_parts(self, part_ids: List[str]) -> Dict[str, List[Dict]]:
        """Параллельное получение комментариев для нескольких глав"""
        async def fetch_one(part_id: str) -> tuple:
            comments = await self.get_part_comments(part_id)
            return (part_id, comments)
        
        tasks = [fetch_one(pid) for pid in part_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        comments_map = {}
        for r in results:
            if isinstance(r, tuple):
                part_id, comments = r
                comments_map[part_id] = comments
        
        return comments_map


# Утилита для запуска async функций из синхронного кода
def run_async(coro):
    """Запускает корутину из синхронного контекста"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Если уже есть запущенный loop (например, Jupyter)
            import nest_asyncio
            nest_asyncio.apply()
            return loop.run_until_complete(coro)
        else:
            return loop.run_until_complete(coro)
    except RuntimeError:
        # Нет активного loop
        return asyncio.run(coro)
