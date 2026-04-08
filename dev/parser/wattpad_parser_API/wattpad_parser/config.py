# config.py
"""
Централизованная конфигурация парсера Wattpad
"""

# === Таймауты ===
DEFAULT_TIMEOUT = 120000  # 2 минуты
PAGE_LOAD_TIMEOUT = 60000  # 1 минута

# === Лимиты ===
MAX_CHAPTERS_TO_PARSE = 3  # Максимум глав для полного парсинга
MAX_COMMENTS_PER_CHAPTER = 50  # Максимум комментариев на главу
LIMIT_PER_GENRE_QUICK = 2  # Лимит книг на жанр в быстром режиме

# === Паузы между запросами (секунды) ===
DELAY_BETWEEN_PAGES = (0.5, 1.0)  # min, max
DELAY_BETWEEN_STORIES = (2.0, 4.0)
DELAY_BETWEEN_GENRES = (2.0, 3.0)

# === Русские теги для быстрого режима ===
RU_TAGS = [
    "романтика", "любовь", "фэнтези", "мистика", "драма",
    "приключения", "подростковаялитература", "фанфик", "оборотни", "вампиры",
    "школа", "мафия", "юмор", "лгбт", "слэш", 
    "детективы", "триллер", "ужасы", "поэзия", "классика"
]

# === CSS Селекторы ===
SELECTORS = {
    # Селекторы для заголовков
    'title': [
        'h1.sr-only',
        'h1[data-testid="story-title"]',
        '.story-header h1',
        '.story-info h1',
        'meta[property="og:title"]',
    ],
    # Селекторы для автора
    'author': [
        'a[href*="/user/"]',
        '[data-testid="author-name"]',
        '.author-info__username',
        '.author-name',
    ],
    # Селекторы для описания
    'description': [
        'meta[name="description"]',
        'meta[property="og:description"]',
        '[data-testid="description"]',
        '.description-text',
    ],
    # Селекторы для глав
    'chapters': [
        'a[href*="/story/"][href*="/part/"]',
        '[data-testid="part-link"]',
        '.story-parts a[href*="/part/"]',
        '.table-of-contents a[href*="/part/"]',
    ],
    # Селекторы для карточек историй
    'story_cards': [
        'a[href*="/story/"]',
        '[data-testid="story-card"] a',
        '.story-card a',
        '.story-item a',
    ],
}

# === API URLs ===
API_BASE_URL_V3 = 'https://www.wattpad.com/api/v3'
API_BASE_URL_V4 = 'https://www.wattpad.com/api/v4'
API_BASE_URL_V5 = 'https://www.wattpad.com/v5'
WEB_BASE_URL = 'https://www.wattpad.com'

# === Логирование ===
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
LOG_BACKUP_COUNT = 3  # Количество файлов ротации
