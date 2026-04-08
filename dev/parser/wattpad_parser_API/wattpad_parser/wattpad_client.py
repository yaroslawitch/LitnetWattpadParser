import requests
import time
from typing import Dict, List, Optional, Any

# Константы API
# Используем веб-прокси API, так как прямой api.wattpad.com требует ключей
API_BASE_URL = 'https://www.wattpad.com/api/v4'
API_BASE_URL_v2 = 'https://www.wattpad.com/api/v2'
API_BASE_URL_v3 = 'https://www.wattpad.com/api/v3'
API_BASE_URL_v5 = 'https://www.wattpad.com/v5'

class WattpadClient:
    def __init__(self, api_key: str = None, cookies: Dict = None):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        })
        if api_key:
             self.session.headers['Authorization'] = api_key
        if cookies:
            self.session.cookies.update(cookies)

    def _make_request(self, endpoint: str, method: str = 'GET', params: Dict = None, data: Dict = None, api_base_url: str = API_BASE_URL) -> Dict:
        # Убираем ведущий слэш если есть, чтобы корректно собрать URL
        if endpoint.startswith('/'):
            endpoint = endpoint[1:]
            
        url = f"{api_base_url}/{endpoint}"
        
        try:
            response = self.session.request(method, url, params=params, json=data, timeout=10)
            
            if response.status_code == 429:
                print("Rate limit exceeded, waiting...")
                time.sleep(5)
                return self._make_request(endpoint, method, params, data, api_base_url)
                
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"API Request Error ({url}): {e}")
            return {}

    def get_story_details(self, story_id: str) -> Dict:
        """Получение информации о книге по ID"""
        # Используем v3 как в примере
        return self._make_request(f'stories/{story_id}', api_base_url=API_BASE_URL_v3)

    def get_story_parts(self, story_id: str) -> List[Dict]:
        """Получение списка глав книги"""
        # Обычно детали истории содержат 'parts', но если нет - можно запросить отдельно
        data = self.get_story_details(story_id)
        return data.get('parts', [])

    def get_part_content(self, part_id: str) -> Dict:
        """Получение текста главы (v2)"""
        # fields=text_url,text_txt - v2 может вернуть ссылку на текст или сам текст
        params = {'fields': 'id,title,text_url,text_txt,word_count,comment_count,vote_count'}
        return self._make_request(f'parts/{part_id}', params=params, api_base_url=API_BASE_URL_v2)

    def get_part_comments(self, part_id: str, limit: int = 20, offset: int = 0) -> List[Dict]:
        """Получение комментариев к главе (v5)"""
        params = {'limit': limit, 'offset': offset}
        endpoint = f'comments/namespaces/parts/resources/{part_id}/comments'
        data = self._make_request(endpoint, params=params, api_base_url=API_BASE_URL_v5)
        return data.get('comments', [])

    def get_comment_replies(self, comment_id: Any) -> List[Dict]:
        """Получение ответов на комментарий (v5)"""
        # Если comment_id - словарь (как возвращает v5), достаем resourceId
        if isinstance(comment_id, dict):
            resource_id = comment_id.get('resourceId')
        else:
            resource_id = comment_id
            
        endpoint = f'comments/namespaces/comments/resources/{resource_id}/comments'
        data = self._make_request(endpoint, api_base_url=API_BASE_URL_v5)
        
        return data.get('comments', [])
        
    def get_stories_by_category(self, category_id: str = "4", limit: int = 10) -> List[Dict]:
        """Получение списка историй по ID жанра (v4)"""
        # 4 - Romance
        # Endpoint: /api/v3/stories?filter=hot&category={id}&limit={limit}
        params = {
            'filter': 'hot',
            'category': category_id,
            'limit': limit,
            'offset': 0
        }
        data = self._make_request('stories', params=params, api_base_url=API_BASE_URL_v3)
        return data.get('stories', [])

    def get_story_id_from_url(self, url: str) -> Optional[str]:
        """Извлекает ID из URL"""
        import re
        match = re.search(r'(?:story|kv)/(\d+)', url)
        if match:
            return match.group(1)
        return None
