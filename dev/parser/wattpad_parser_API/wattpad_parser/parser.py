# parser.py
import json
import time
import random
from datetime import datetime
import os
import re
from urllib.parse import urljoin, urlparse
from typing import List, Dict, Optional, Any, Set
import logging
from bs4 import BeautifulSoup
import hashlib
import csv
from collections import defaultdict
import uuid

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import langdetect
from langdetect.lang_detect_exception import LangDetectException

from models import StoryInfo, ChapterInfo
from database import DatabaseManager
from wattpad_client import WattpadClient
from cache import get_cache

logger = logging.getLogger(__name__)

class WattpadParser:
    """Основной класс парсера Wattpad"""

    def __init__(self, db_manager: DatabaseManager, year: int, languages: List[str], 
                 headless: bool = True, timeout: int = 30000, 
                 max_stories: int = 100,
                 parse_chapters: bool = False,
                 parse_comments: bool = False,
                 username: str = None,
                 password: str = None):
        """
        Парсер для сбора книг определенного года на Wattpad

        Args:
            db_manager: Менеджер базы данных (файловый)
            year: Год для поиска
            languages: Список языков ['ru', 'en']
            headless: Запускать браузер в headless режиме
            timeout: Таймаут для операций
            max_stories: Максимальное количество книг для сбора (None - без ограничений)
            parse_chapters: Парсить ли оглавление и главы
            parse_comments: Парсить ли комментарии
        """
        self.WEB_BASE = "https://www.wattpad.com"
        self.db = db_manager
        self.target_year = year
        self.languages = languages
        self.target_languages = [lang.lower() for lang in languages] if languages else ['en', 'ru']
        self.headless = headless
        self.timeout = timeout
        self.max_stories = max_stories
        self.parse_chapters = parse_chapters
        self.parse_comments = parse_comments
        self.username = username
        self.password = password
        self.stats = {
            'stories_found': 0,
            'stories_parsed': 0,
            'chapters_parsed': 0,
            'comments_parsed': 0,
            'errors': 0,
            'pages_scanned': 0,
            'start_time': time.time(),
            'end_time': 0
        }
        self.api_client = WattpadClient()
        
        # Создаем папку для результатов
        self.results_dir = f"results_{year}"
        os.makedirs(self.results_dir, exist_ok=True)
        self.cookies = []  # Хранение кук для Playwright session

    def _login(self, page) -> bool:
        """Авторизация на Wattpad"""
        if not self.username or not self.password:
            return False
            
        try:
            logger.info("Попытка авторизации...")
            page.goto('https://www.wattpad.com/login', wait_until='domcontentloaded', timeout=self.timeout)
            time.sleep(2)
            
            # Проверяем не залогинены ли уже
            if page.query_selector('.user-profile-avatar') or page.query_selector('.avatar'):
                logger.info("Уже авторизованы")
                return True
                
            # Заполняем форму
            # Селекторы могут меняться, пробуем разные
            email_selectors = ['input[name="username"]', 'input[type="text"]', '#login-username']
            pass_selectors = ['input[name="password"]', 'input[type="password"]', '#login-password']
            
            for sel in email_selectors:
                if page.query_selector(sel):
                    page.fill(sel, self.username)
                    break
                    
            for sel in pass_selectors:
                if page.query_selector(sel):
                    page.fill(sel, self.password)
                    break
            
            # Кнопка входа
            submit_btn = page.query_selector('button[type="submit"], input[type="submit"], .login-btn')
            if submit_btn:
                submit_btn.click()
                
            logger.info("ВАЖНО: Если автоматический вход не сработал, пожалуйста, войдите ВРУЧНУЮ прямо сейчас!")
            logger.info("Скрипт ждет появления вашего аватара (успешного входа)...")
                
            # Ждем перенаправления или появления аватара (ДАЕМ 120 СЕКУНД)
            try:
                page.wait_for_selector('.user-profile-avatar, .avatar, [data-testid="profile-image"]', timeout=120000)
                logger.info("Авторизация успешна! Сканируем куки...")
                
                # Копируем куки в API клиент
                cookies = page.context.cookies()
                cookie_dict = {c['name']: c['value'] for c in cookies}
                
                # Передаем куки в сессию requests API клиента
                self.api_client.session.cookies.update(cookie_dict)
                # Добавляем User-Agent
                ua = page.evaluate("navigator.userAgent")
                self.api_client.session.headers.update({'User-Agent': ua})
                
                return True
            except:
                logger.error("Не удалось дождаться подтверждения входа")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при авторизации: {e}")
            return False
        # Создаем папку для результатов
        self.results_dir = f"results_{year}"
        os.makedirs(self.results_dir, exist_ok=True)

    def collect_stories_for_year(self, override_genres: List[str] = None, limit_per_genre: int = None, strict_mode: bool = True) -> List[StoryInfo]:
        """
        Основной метод сбора всех книг за указанный год
        """
        logger.info(f"Начало сбора книг за {self.target_year} год")
        logger.info(f"Языки: {', '.join(self.languages)}")
        logger.info(f"Ограничение по количеству книг: {'нет' if self.max_stories is None else self.max_stories}")

        self.stats['start_time'] = time.time()

        run_id = self.db.save_parser_run(0)

        try:
            # 0. Авторизация (если есть данные)
            if self.username and self.password:
                logger.info("Шаг 0: Авторизация...")
                self._perform_login()
            
            # 1. Получаем список жанров
            if override_genres:
                logger.info(f"Используем заданный список жанров ({len(override_genres)} шт.)")
                genres = override_genres
            else:
                logger.info("Шаг 1: Получение списка жанров с главной страницы...")
                genres = self._get_genres_from_homepage()
                logger.info(f"Найдено жанров: {len(genres)}")

            if not genres:
                logger.error("Не удалось получить список жанров")
                return []

            # 2. Сбор URL книг через жанры
            logger.info("Шаг 2: Сбор URL книг через все жанры...")
            all_urls = self._collect_story_urls_via_genres(genres, limit_per_genre=limit_per_genre)
            logger.info(f"Собрано {len(all_urls)} уникальных URL книг")

            if not all_urls:
                logger.warning("Не найдено URL книг.")
                return []

            # 3. Парсинг книг (всех найденных)
            logger.info(f"Шаг 3: Парсинг информации о {len(all_urls)} книгах...")
            stories = self._parse_stories(all_urls, run_id)
            logger.info(f"Успешно спарсено {len(stories)} книг")

            # 4. Фильтрация по году и языку
            logger.info("Шаг 4: Фильтрация по году и языку...")
            filtered_stories = self._filter_stories_by_year_and_language(stories, strict_mode=strict_mode)
            logger.info(f"После фильтрации осталось {len(filtered_stories)} книг")

            # 5. Парсинг глав (если нужно)
            if self.parse_chapters and filtered_stories:
                logger.info(f"Шаг 5: Парсинг глав для найденных книг...")
                self._parse_chapters_for_stories(filtered_stories, run_id)

            self.stats['end_time'] = time.time()

            # 6. Сохранение результатов в файлы
            logger.info("Шаг 6: Сохранение результатов в файлы...")
            self._save_results_to_files(filtered_stories)

            return filtered_stories

        except Exception as e:
            logger.error(f"Критическая ошибка при сборе книг: {e}", exc_info=True)
            return []

    def _perform_login(self) -> None:
        """Выполняет вход и сохраняет сессию"""
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=False, # Видимый для надежности или отладки, можно True
                    args=['--disable-blink-features=AutomationControlled']
                )
                context = browser.new_context(
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )
                page = context.new_page()
                
                if self._login(page):
                    logger.info("Сессия успешно сохранена в API клиент")
                    # Сохраняем куки для будущих сессий Playwright
                    self.cookies = context.cookies()
                    logger.info(f"Сохранено {len(self.cookies)} кук для Playwright")
                else:
                    logger.warning("Авторизация не удалась, продолжаем как гость")
                    
                browser.close()
        except Exception as e:
            logger.error(f"Ошибка в _perform_login: {e}")

    def _get_genres_from_homepage(self) -> List[str]:
        """
        Получает список жанров с главной страницы Wattpad
        """
        genres = []

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=self.headless,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox'
                ]
            )

            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='en-US,en;q=0.9,ru;q=0.8'
            )

            page = context.new_page()

            try:
                # Загружаем главную страницу
                logger.info(f"Загрузка главной страницы: {self.WEB_BASE}")
                response = page.goto(self.WEB_BASE, wait_until='networkidle', timeout=self.timeout)

                if not response or response.status != 200:
                    logger.error(f"Не удалось загрузить главную страницу")
                    return []

                time.sleep(random.uniform(2, 3))

                # Прокручиваем немного для загрузки контента
                page.evaluate("window.scrollTo(0, 500)")
                time.sleep(1)

                # Ищем кнопку поиска или меню жанров
                search_selectors = [
                    'input[type="search"]',
                    'input[placeholder*="search"]',
                    'input[placeholder*="поиск"]',
                    '.search-input',
                    '[data-testid="search-input"]'
                ]

                # Сначала пробуем найти поле поиска
                search_input = None
                for selector in search_selectors:
                    search_input = page.query_selector(selector)
                    if search_input:
                        logger.info(f"Найдено поле поиска: {selector}")
                        break

                if not search_input:
                    # Если поле поиска не найдено, ищем ссылки на жанры
                    logger.info("Поиск ссылок на жанры...")

                    # Ищем все ссылки, которые могут вести к жанрам
                    all_links = page.query_selector_all('a')
                    genre_patterns = [
                        r'/discover/', r'/browse/', r'/genre/', r'/category/',
                        r'/stories/', r'/tag/', r'/explore/'
                    ]

                    for link in all_links:
                        try:
                            href = link.get_attribute('href')
                            if href:
                                href_lower = href.lower()
                                # Проверяем, похоже ли на жанр
                                if any(pattern in href_lower for pattern in genre_patterns):
                                    text = link.inner_text().strip()
                                    if text and len(text) < 50 and text not in genres:
                                        genres.append(text)
                                        logger.debug(f"Найден возможный жанр: {text} -> {href}")
                        except:
                            continue

                    # Также пробуем найти выпадающие меню
                    dropdown_selectors = [
                        '.dropdown-menu', '.nav-dropdown', '.menu-list',
                        '[data-testid="dropdown"]', '.category-list'
                    ]

                    for selector in dropdown_selectors:
                        try:
                            dropdown = page.query_selector(selector)
                            if dropdown:
                                dropdown_links = dropdown.query_selector_all('a')
                                for link in dropdown_links:
                                    try:
                                        text = link.inner_text().strip()
                                        if text and len(text) < 50 and text not in genres:
                                            genres.append(text)
                                            logger.debug(f"Найден жанр в dropdown: {text}")
                                    except:
                                        continue
                        except:
                            continue

                # Если жанры не найдены, используем популярные жанры Wattpad
                if not genres:
                    logger.info("Жанры не найдены, используем стандартный список")
                    # Английские жанры
                    genres = [
                        "romance", "fantasy", "mystery", "teen-fiction", "fanfiction",
                        "horror", "paranormal", "science-fiction", "short-story",
                        "humor", "poetry", "historical-fiction", "adventure", "drama",
                        "action", "new-adult", "vampire", "werewolf", "spiritual"
                    ]
                else:
                    # Фильтруем жанры
                    filtered_genres = []
                    for genre in genres:
                        if 2 <= len(genre) <= 30:
                            filtered_genres.append(genre.lower())

                    genres = list(set(filtered_genres))
                
                # ВСЕГДА добавляем русские теги если нужен русский язык
                if 'ru' in self.languages:
                    russian_tags = [
                        "романтика", "фэнтези", "любовь", "драма", "приключения",
                        "мистика", "ужасы", "фанфик", "школа", "подростки",
                        "комедия", "боевик", "история", "поэзия", "вампиры",
                        "оборотни", "магия", "русский", "книга", "рассказ"
                    ]
                    genres.extend(russian_tags)
                    logger.info(f"Добавлены русские теги для поиска: {len(russian_tags)} тегов")

                logger.info(f"Найдено жанров: {len(genres)}")
                return genres

            except Exception as e:
                logger.error(f"Ошибка при получении жанров: {e}")
                return []

            finally:
                browser.close()

    def _collect_story_urls_via_genres(self, genres: List[str], limit_per_genre: int = None) -> Set[str]:
        """
        Сбор URL книг через жанры
        """
        urls = set()

        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=self.headless,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox'
                ]
            )

            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                locale='en-US,en;q=0.9,ru;q=0.8'
            )

            page = context.new_page()

            try:
                page.set_default_timeout(self.timeout)
                page.set_default_navigation_timeout(self.timeout)

                for genre_index, genre in enumerate(genres, 1):
                    try:
                        logger.info(f"[{genre_index}/{len(genres)}] Поиск книг по жанру: {genre}")

                        # Используем поиск по жанру
                        search_url = f"{self.WEB_BASE}/search/{genre}/stories"
                        logger.info(f"Переход на: {search_url}")

                        response = page.goto(search_url, wait_until='networkidle', timeout=self.timeout)

                        if not response or response.status != 200:
                            logger.warning(f"Не удалось загрузить страницу поиска для жанра: {genre}")
                            continue

                        time.sleep(random.uniform(2, 3))

                        # Прокручиваем страницу для загрузки контента
                        for scroll_num in range(3):
                            page.evaluate(f"window.scrollTo(0, {1000 * (scroll_num + 1)})")
                            time.sleep(random.uniform(1, 2))

                        # Получаем HTML страницы
                        content = page.content()
                        soup = BeautifulSoup(content, 'html.parser')

                        # Ищем все ссылки на истории
                        story_links = []

                        # Пробуем разные селекторы для карточек историй
                        card_selectors = [
                            'a[href*="/story/"]',
                            '[data-testid="story-card"] a',
                            '.story-card a',
                            '.story-item a',
                            '.story-preview a'
                        ]

                        for selector in card_selectors:
                            links = soup.select(selector)
                            if links:
                                story_links.extend(links)

                        # Обрабатываем найденные ссылки
                        new_urls_count = 0
                        for link in story_links:
                            try:
                                href = link.get('href', '')
                                if href and '/story/' in href:
                                    # Формируем полный URL
                                    full_url = urljoin(self.WEB_BASE, href)

                                    # Очищаем URL
                                    parsed = urlparse(full_url)
                                    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                                    clean_url = clean_url.split('?')[0].split('#')[0]

                                    # Проверяем, что это действительно ссылка на историю
                                    if re.match(r'.*/story/\d+[-a-zA-Z0-9]*$', clean_url):
                                        if clean_url not in urls:
                                            urls.add(clean_url)
                                            new_urls_count += 1
                                            
                                            if limit_per_genre and new_urls_count >= limit_per_genre:
                                                break
                            except Exception as e:
                                continue
                            
                            if limit_per_genre and new_urls_count >= limit_per_genre:
                                break

                        logger.info(f"По жанру '{genre}' найдено {new_urls_count} новых книг, всего: {len(urls)}")
                        
                        # Проверяем лимит на жанр
                        if limit_per_genre and new_urls_count >= limit_per_genre:
                            logger.info(f"Достигнут лимит книг для жанра '{genre}' ({limit_per_genre})")
                            continue

                        # Пробуем перейти на следующие страницы поиска (без ограничений по страницам)
                        page_num = 2
                        max_empty_pages = 3  # Максимум 3 пустых страницы подряд
                        empty_pages_count = 0

                        while empty_pages_count < max_empty_pages:
                            # Дополнительная проверка лимита внутри пагинации
                            if limit_per_genre and new_urls_count >= limit_per_genre:
                                break
                                
                            try:
                                next_page_url = f"{self.WEB_BASE}/search/{genre}/stories?p={page_num}"
                                logger.debug(f"Переход на страницу {page_num}: {next_page_url}")

                                response = page.goto(next_page_url, wait_until='networkidle', timeout=self.timeout)

                                if not response or response.status != 200:
                                    logger.debug(f"Страница {page_num} не загрузилась, прекращаем")
                                    break

                                time.sleep(random.uniform(1, 2))

                                # Прокручиваем
                                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                                time.sleep(1)

                                # Получаем новые ссылки
                                content = page.content()
                                soup = BeautifulSoup(content, 'html.parser')

                                page_new_urls = 0
                                for selector in card_selectors:
                                    links = soup.select(selector)
                                    for link in links:
                                        try:
                                            href = link.get('href', '')
                                            if href and '/story/' in href:
                                                full_url = urljoin(self.WEB_BASE, href)
                                                parsed = urlparse(full_url)
                                                clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
                                                clean_url = clean_url.split('?')[0].split('#')[0]

                                                if re.match(r'.*/story/\d+[-a-zA-Z0-9]*$', clean_url):
                                                    if clean_url not in urls:
                                                        urls.add(clean_url)
                                                        page_new_urls += 1
                                                        new_urls_count += 1
                                                        
                                                        if limit_per_genre and new_urls_count >= limit_per_genre:
                                                            break
                                                            
                                        except:
                                            continue
                                    if limit_per_genre and new_urls_count >= limit_per_genre:
                                        break

                                logger.debug(f"На странице {page_num} найдено {page_new_urls} новых книг")

                                if page_new_urls == 0:
                                    empty_pages_count += 1
                                    logger.debug(f"Пустая страница #{empty_pages_count}")
                                else:
                                    empty_pages_count = 0

                                page_num += 1

                                # Пауза между страницами
                                time.sleep(random.uniform(0.5, 1))

                            except Exception as e:
                                logger.debug(f"Ошибка загрузки страницы {page_num}: {e}")
                                break

                        # Пауза между жанрами
                        time.sleep(random.uniform(2, 3))

                    except Exception as e:
                        logger.error(f"Ошибка при поиске по жанру '{genre}': {e}")
                        continue

            except Exception as e:
                logger.error(f"Ошибка при сборе URL: {e}", exc_info=True)

            finally:
                browser.close()

        logger.info(f"Всего собрано {len(urls)} уникальных URL книг")
        return urls

    def _parse_stories(self, urls: Set[str], run_id: uuid.UUID) -> List[StoryInfo]:
        """Парсинг всех найденных книг"""
        stories = []
        urls_list = list(urls)  # Берем все URL без ограничений
        
        # Получаем кэш
        cache = get_cache()
        
        # Фильтруем уже спарсенные URL
        new_urls = [url for url in urls_list if not cache.is_cached(url)]
        cached_count = len(urls_list) - len(new_urls)
        
        if cached_count > 0:
            logger.info(f"Пропущено {cached_count} URL из кэша")
        
        logger.info(f"Начало парсинга {len(new_urls)} новых книг")

        total_books = len(new_urls)

        for i, url in enumerate(new_urls, 1):
            try:
                if i % 10 == 0:
                    logger.info(f"[{i}/{total_books}] Парсинг книг...")

                story = self._parse_single_story(url, run_id)
                if story:
                    stories.append(story)
                    # Добавляем в кэш после успешного парсинга
                    cache.add(url)

                    if i % 10 == 0:
                        logger.info(f"[{i}/{total_books}] Успешно: {story.title}")
                else:
                    if i % 10 == 0:
                        logger.warning(f"[{i}/{total_books}] Не удалось спарсить")
                    self.stats['errors'] += 1

                # Пауза между запросами
                time.sleep(random.uniform(2, 4))

            except Exception as e:
                if i % 10 == 0:
                    logger.error(f"[{i}/{total_books}] Ошибка парсинга: {e}")
                self.stats['errors'] += 1

        # Сохраняем кэш
        cache.save()
        
        logger.info(f"Парсинг завершен: {len(stories)} книг успешно спарсено")
        return stories

    def _parse_single_story(self, url: str, run_id: uuid.UUID) -> Optional[StoryInfo]:
        """Парсинг одной книги"""
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=['--disable-blink-features=AutomationControlled']
                )

                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    locale='en-US,en;q=0.9,ru;q=0.8'
                )

                # Внедряем куки если есть
                if self.cookies:
                    try:
                        context.add_cookies(self.cookies)
                    except Exception as e:
                        logger.warning(f"Не удалось добавить куки: {e}")

                page = context.new_page()
                page.set_default_timeout(self.timeout)

                try:
                    # Загружаем страницу книги
                    response = page.goto(url, wait_until='domcontentloaded', timeout=self.timeout)

                    if not response or response.status != 200:
                        return None

                    time.sleep(random.uniform(2, 3))

                    # Прокручиваем для загрузки всего контента
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                    time.sleep(1)

                    # Получаем HTML
                    content = page.content()
                    soup = BeautifulSoup(content, 'html.parser')

                    # Извлекаем данные
                    story_id = self._extract_story_id(url)
                    title = self._extract_title(soup, page)
                    author = self._extract_author(soup, page)
                    author_id = uuid.uuid5(uuid.NAMESPACE_URL, f"author:{author}")
                    description = self._extract_description(soup, page)
                    language = self._detect_language(soup, title, description)

                    # Получаем год публикации (None если не удалось определить)
                    year_published = self._extract_year_from_page(page)
                    if not year_published:
                        year_published = self._extract_year(soup)  # None если не найден

                    stats = self._extract_stats(soup, page)
                    metadata = self._extract_metadata(soup, page)
                    tags = self._extract_tags(soup, page)

                    # Создаем объект StoryInfo
                    story = StoryInfo(
                        id=story_id,
                        title=title,
                        author=author,
                        description=description,
                        language=language,
                        year_published=year_published,
                        url=url,
                        stats=stats,
                        metadata=metadata,
                        parse_date=datetime.now().isoformat(),
                        chapters_count=stats.get('parts', 0),
                        words_count=stats.get('words', 0),
                        read_count=stats.get('reads', 0),
                        vote_count=stats.get('votes', 0),
                        comment_count=stats.get('comments', 0),
                        mature=metadata.get('mature', False),
                        completed=metadata.get('completed', False),
                        tags=tags
                    )

                    # Сохраняем в хранилище
                    book_model = story.to_book_model(run_id, author_id)
                    self.db.save_book(book_model)

                    self.stats['stories_parsed'] += 1
                    return story

                finally:
                    browser.close()

        except Exception as e:
            return None

    def _parse_chapters_for_stories(self, stories: List[StoryInfo], run_id: uuid.UUID):
        """Парсинг глав для списка книг"""
        if not self.parse_chapters:
            return

        logger.info(f"Начало парсинга глав для {len(stories)} книг")

        total_books = len(stories)

        for i, story in enumerate(stories, 1):
            try:
                if i % 5 == 0:
                    logger.info(f"[{i}/{total_books}] Парсинг глав...")

                self._parse_chapters_for_single_story(story, run_id)

                if i % 5 == 0:
                    logger.info(f"[{i}/{total_books}] Успешно: {story.title}")

                time.sleep(random.uniform(2, 3))

            except Exception as e:
                if i % 5 == 0:
                    logger.error(f"[{i}/{total_books}] Ошибка парсинга глав: {e}")

    def _parse_chapters_for_single_story(self, story: StoryInfo, run_id: uuid.UUID):
        """Парсинг глав для одной книги"""
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                )

                # Внедряем куки для доступа к главам
                if self.cookies:
                    try:
                        context.add_cookies(self.cookies)
                        logger.debug("Куки добавлены в контекст парсинга глав")
                    except Exception as e:
                        logger.warning(f"Не удалось добавить куки: {e}")
                page = context.new_page()
                page.set_default_timeout(self.timeout)

                try:
                    # Загружаем страницу книги
                    response = page.goto(story.url, wait_until='domcontentloaded', timeout=self.timeout)

                    if not response or response.status != 200:
                        return

                    time.sleep(random.uniform(2, 3))

                    # Парсим оглавление (Сначала пробуем API)
                    chapters = self._get_chapters_via_api(story.id)
                    
                    if not chapters:
                        logger.info("API не вернул главы, пробуем парсинг страницы")
                        chapters = self._extract_table_of_contents(page, story.id)

                    if chapters:
                        # Обновляем количество глав в story на основе реального TOC
                        story.chapters_count = len(chapters)
                        story.toc_parsed = True
                        logger.debug(f"Найдено {len(chapters)} глав в TOC для: {story.title}")
                        
                        book_id = uuid.uuid5(uuid.NAMESPACE_URL, story.url)
                        
                        # Храним данные глав для сохранения в JSON
                        chapters_data = []

                        for chapter_info in chapters[:3]:  # Парсим первые 3 главы
                            try:
                                # Создаем модель главы
                                chapter_model = chapter_info.to_chapter_model(book_id, run_id)

                                # Парсим содержимое главы
                                chapter_content = self._extract_chapter_content(page, chapter_info.url)
                                if chapter_content:
                                    chapter_info.content = chapter_content['text']
                                    chapter_info.word_count = chapter_content['word_count']

                                # Сохраняем в хранилище
                                self.db.save_chapter(chapter_model)

                                self.stats['chapters_parsed'] += 1
                                
                                # Собираем комментарии если нужно
                                if self.parse_comments:
                                    comments = self._extract_comments_for_chapter(page, chapter_info.url)
                                    
                                    # Разделяем inline и обычные комментарии
                                    inline_comments = [c for c in comments if c.get('type') == 'inline']
                                    regular_comments = [c for c in comments if c.get('type') == 'regular']
                                    
                                    chapter_info.comments_data = regular_comments
                                    chapter_info.inline_comments_data = inline_comments
                                    chapter_info.comment_count = len(comments)
                                    
                                    self.stats['comments_parsed'] += len(comments)
                                    
                                    if comments:
                                        logger.info(f"Собрано {len(comments)} комментариев для главы: {chapter_info.title} (inline: {len(inline_comments)}, обычных: {len(regular_comments)})")
                                
                                # Добавляем данные главы
                                chapters_data.append(chapter_info.to_dict())

                                time.sleep(random.uniform(1, 2))

                            except Exception as e:
                                logger.debug(f"Ошибка парсинга главы: {e}")
                                continue
                        
                        # Сохраняем данные глав в story metadata
                        if not story.metadata:
                            story.metadata = {}
                        story.metadata['chapters_data'] = chapters_data
                        story.comments_parsed = True

                finally:
                    browser.close()

        except Exception as e:
            logger.debug(f"Ошибка парсинга глав для {story.title}: {e}")
            return

    # Методы извлечения данных (без изменений)
    def _extract_story_id(self, url: str) -> str:
        """Извлечение ID истории"""
        try:
            match = re.search(r'/story/(\d+)', url)
            if match:
                return match.group(1)
            return hashlib.md5(url.encode('utf-8')).hexdigest()[:10]
        except:
            return str(uuid.uuid4())[:10]

    def _extract_title(self, soup: BeautifulSoup, page) -> str:
        """Извлечение заголовка"""
        try:
            selectors = [
                'h1[data-testid="story-title"]',
                'h1.story-title',
                'h1.title',
                'meta[property="og:title"]',
                'title'
            ]

            for selector in selectors:
                try:
                    if selector.startswith('meta'):
                        element = soup.select_one(selector)
                        if element:
                            title = element.get('content', '').strip()
                            if title:
                                title = re.sub(r'\s*[-–—]\s*Wattpad$', '', title, flags=re.IGNORECASE)
                                return title[:300]
                    else:
                        element = soup.select_one(selector)
                        if element:
                            title = element.get_text(strip=True)
                            if title:
                                title = re.sub(r'\s*[-–—]\s*Wattpad$', '', title, flags=re.IGNORECASE)
                                return title[:300]
                except:
                    continue

            return "Без названия"
        except Exception as e:
            logger.debug(f"Ошибка извлечения заголовка: {e}")
            return "Без названия"

    def _extract_author(self, soup: BeautifulSoup, page) -> str:
        """Извлечение автора"""
        try:
            selectors = [
                'a[data-testid="author"]',
                '.author-name',
                '.author',
                'a[href*="/user/"]',
                '.story-by a',
                'meta[name="author"]'
            ]

            for selector in selectors:
                try:
                    if selector.startswith('meta'):
                        element = soup.select_one(selector)
                        if element:
                            author = element.get('content', '').strip()
                            if author:
                                return author[:100]
                    else:
                        element = soup.select_one(selector)
                        if element:
                            author = element.get_text(strip=True)
                            if author:
                                return author[:100]
                except:
                    continue

            return "Неизвестный автор"
        except Exception as e:
            logger.debug(f"Ошибка извлечения автора: {e}")
            return "Неизвестный автор"

    def _extract_description(self, soup: BeautifulSoup, page) -> str:
        """Извлечение описания"""
        try:
            selectors = [
                'meta[name="description"]',
                'meta[property="og:description"]',
                '[data-testid="description"]',
                '.description-text',
                '.description',
                '.story-description'
            ]

            for selector in selectors:
                try:
                    if selector.startswith('meta'):
                        element = soup.select_one(selector)
                        if element:
                            desc = element.get('content', '').strip()
                            if desc and len(desc) > 10:
                                return desc[:500]
                    else:
                        element = soup.select_one(selector)
                        if element:
                            desc = element.get_text(strip=True)
                            if desc and len(desc) > 10:
                                return desc[:500]
                except:
                    continue

            return ""
        except Exception as e:
            logger.debug(f"Ошибка извлечения описания: {e}")
            return ""

    def _detect_language(self, soup: BeautifulSoup, title: str, description: str) -> str:
        """Определение языка"""
        try:
            html_tag = soup.find('html')
            if html_tag and html_tag.get('lang'):
                lang_attr = html_tag.get('lang', '').lower()
                if 'ru' in lang_attr:
                    return 'RU'
                elif 'en' in lang_attr:
                    return 'EN'

            text_to_analyze = f"{title} {description}"
            if len(text_to_analyze) > 20:
                try:
                    detected = langdetect.detect(text_to_analyze)
                    if detected == 'ru':
                        return 'RU'
                    elif detected == 'en':
                        return 'EN'
                except LangDetectException:
                    pass

            page_text = soup.get_text()[:1000]
            ru_count = len(re.findall(r'[а-яёА-ЯЁ]', page_text))
            en_count = len(re.findall(r'[a-zA-Z]', page_text))

            if ru_count > en_count * 2:
                return 'RU'
            elif en_count > ru_count * 2:
                return 'EN'

            return 'UNKNOWN'
        except Exception as e:
            logger.debug(f"Ошибка определения языка: {e}")
            return 'UNKNOWN'

    def _extract_year_from_page(self, page) -> Optional[int]:
        """Извлекает год публикации со страницы"""
        try:
            date_selectors = [
                '[data-testid="published-date"]',
                '.published-date',
                '.date-published',
                'time[datetime]'
            ]

            for selector in date_selectors:
                try:
                    element = page.query_selector(selector)
                    if element:
                        date_text = element.inner_text() or element.get_attribute('datetime') or ''
                        if date_text:
                            match = re.search(r'\b(20\d{2})\b', date_text)
                            if match:
                                year = int(match.group(1))
                                if 2000 <= year <= datetime.now().year:
                                    return year
                except:
                    continue

            return None
        except Exception as e:
            logger.debug(f"Ошибка извлечения года со страницы: {e}")
            return None

    def _extract_year(self, soup: BeautifulSoup) -> Optional[int]:
        """Извлечение года публикации из HTML"""
        current_year = datetime.now().year

        try:
            page_text = soup.get_text()[:5000]

            patterns = [
                r'published.*?(20\d{2})',
                r'created.*?(20\d{2})',
                r'©.*?(20\d{2})',
                r'(20\d{2})\s*[год|year]',
                r'\b(20\d{2})\b.*wattpad'
            ]

            for pattern in patterns:
                matches = re.findall(pattern, page_text, re.IGNORECASE)
                for match in matches:
                    try:
                        year = int(match)
                        if 2000 <= year <= current_year:
                            return year
                    except:
                        continue

            return None
        except Exception as e:
            logger.debug(f"Ошибка извлечения года: {e}")
            return None

    def _extract_stats(self, soup: BeautifulSoup, page) -> Dict:
        """Извлечение статистики"""
        stats = {}

        try:
            page_text = soup.get_text()

            patterns = {
                'reads': [
                    r'([\d,\.]+[KMB]?)\s*(?:reads|прочтений|прочитано)',
                    r'Reads[:\s]*([\d,\.]+[KMB]?)',
                    r'Прочтений[:\s]*([\d,\.]+)'
                ],
                'votes': [
                    r'([\d,\.]+[KMB]?)\s*(?:votes|голосов|лайков)',
                    r'Votes[:\s]*([\d,\.]+[KMB]?)',
                    r'Голосов[:\s]*([\d,\.]+)'
                ],
                'comments': [
                    r'([\d,\.]+[KMB]?)\s*(?:comments|комментари)',
                    r'Comments[:\s]*([\d,\.]+[KMB]?)',
                    r'Комментариев[:\s]*([\d,\.]+)'
                ],
                'parts': [
                    r'(\d{1,3})\s*(?:parts|частей|глав)',  # Ограничиваем 1-999 глав
                    r'Parts[:\s]*(\d{1,3})',
                    r'Частей[:\s]*(\d{1,3})'
                ],
                'words': [
                    r'([\d,\.]+[KMB]?)\s*(?:words|слов)',
                    r'Words[:\s]*([\d,\.]+[KMB]?)',
                    r'Слов[:\s]*([\d,\.]+)'
                ]
            }
            
            def parse_number(value_str: str) -> int:
                """Парсит число с учетом K, M, B суффиксов"""
                value_str = value_str.strip().upper()
                multiplier = 1
                if value_str.endswith('K'):
                    multiplier = 1000
                    value_str = value_str[:-1]
                elif value_str.endswith('M'):
                    multiplier = 1000000
                    value_str = value_str[:-1]
                elif value_str.endswith('B'):
                    multiplier = 1000000000
                    value_str = value_str[:-1]
                
                value_str = value_str.replace(',', '').replace('.', '')
                return int(float(value_str) * multiplier)

            for stat_name, pattern_list in patterns.items():
                for pattern in pattern_list:
                    match = re.search(pattern, page_text, re.IGNORECASE)
                    if match:
                        try:
                            value = parse_number(match.group(1))
                            # Валидация: parts должен быть разумным (1-999)
                            if stat_name == 'parts' and (value < 1 or value > 999):
                                continue
                            stats[stat_name] = value
                            break
                        except:
                            continue

            return stats
        except Exception as e:
            logger.debug(f"Ошибка извлечения статистики: {e}")
            return {}

    def _extract_metadata(self, soup: BeautifulSoup, page) -> Dict:
        """Извлечение метаданных"""
        metadata = {'mature': False, 'completed': False}

        try:
            page_text = soup.get_text().lower()

            mature_keywords = ['mature', '18+', 'взрослый', 'для взрослых', 'adult']
            if any(keyword in page_text for keyword in mature_keywords):
                metadata['mature'] = True

            completed_keywords = ['completed', 'завершено', 'finished', 'закончено']
            if any(keyword in page_text for keyword in completed_keywords):
                metadata['completed'] = True

            return metadata
        except Exception as e:
            logger.debug(f"Ошибка извлечения метаданных: {e}")
            return metadata

    def _extract_tags(self, soup: BeautifulSoup, page) -> List[str]:
        """Извлечение тегов"""
        tags = []

        try:
            tag_selectors = [
                '.tags a',
                '.tag',
                '[data-testid="tag"]',
                '.story-tags a',
                '.tag-item'
            ]

            for selector in tag_selectors:
                elements = soup.select(selector)
                for element in elements:
                    try:
                        tag_text = element.get_text(strip=True)
                        if tag_text and len(tag_text) > 1:
                            tags.append(tag_text)
                    except:
                        continue

                if tags:
                    break

            return list(set(tags))[:15]
        except Exception as e:
            logger.debug(f"Ошибка извлечения тегов: {e}")
            return []

    def _get_chapters_via_api(self, story_id: str) -> List[ChapterInfo]:
        """Получение списка глав через API (быстро и надежно)"""
        try:
            # Извлекаем цифровой ID если был передан строковый
            if not story_id.isdigit() and '_' in story_id:
                # Пытаемся получить из URL если ID недостоверен
                # Но лучше использовать тот что есть
                pass
                
            logger.debug(f"Запрос глав через API для story_id: {story_id}")
            parts_data = self.api_client.get_story_parts(story_id)
            
            chapters = []
            if parts_data:
                logger.info(f"API API вернул {len(parts_data)} глав")
                for idx, part in enumerate(parts_data, 1):
                    chapter_id = str(part.get('id', ''))
                    title = part.get('title', f"Глава {idx}")
                    url = part.get('url', '')
                    
                    if not url and chapter_id:
                        url = f"{self.WEB_BASE}/{chapter_id}"
                        
                    if chapter_id:
                        chapter = ChapterInfo(
                            id=chapter_id,
                            number=idx,
                            title=title,
                            url=url
                        )
                        chapters.append(chapter)
            
            return chapters
        except Exception as e:
            logger.warning(f"Ошибка API при получении глав: {e}")
            return []

    def _extract_table_of_contents(self, page, story_id: str) -> List[ChapterInfo]:
        """Извлекает оглавление книги"""
        chapters = []

        try:
            # Сначала пробуем открыть dropdown с оглавлением
            toc_button_selectors = [
                'button[aria-label*="Table of Contents"]',
                'button[aria-label*="оглавлени"]',
                '[data-testid="toc-button"]',
                '.table-of-contents-button',
                '.toc-button',
                'button:has-text("Table of Contents")',
                'button:has-text("Parts")',
                '.story-parts button',
                '[data-testid="story-parts"]'
            ]
            
            for btn_sel in toc_button_selectors:
                try:
                    btn = page.query_selector(btn_sel)
                    if btn and btn.is_visible():
                        btn.click()
                        time.sleep(1)
                        logger.debug(f"Нажата кнопка TOC: {btn_sel}")
                        break
                except:
                    continue
            
            # Обновленные селекторы для ссылок на главы Wattpad 2024+
            chapter_selectors = [
                # Современные селекторы Wattpad
                'a[href*="/story/"][href*="/part/"]',
                '[data-testid="part-link"]',
                '.story-parts a[href*="/part/"]',
                '.table-of-contents a[href*="/part/"]',
                '.toc-list a',
                '.story-part-link',
                # Старые селекторы
                'a[href*="/part/"]',
                'a[href*="/chapter/"]',
                '.table-of-contents a',
                '.chapter-list a',
                # Fallback: любые ссылки с номерами частей
                'a[href*="wattpad.com"][href*="part"]'
            ]

            for selector in chapter_selectors:
                try:
                    links = page.query_selector_all(selector)
                    if links and len(links) > 0:
                        logger.debug(f"Найдено {len(links)} ссылок по селектору: {selector}")
                        
                        for idx, link in enumerate(links, 1):
                            try:
                                href = link.get_attribute('href')
                                title = link.inner_text().strip() or f"Глава {idx}"

                                if href and ('/part/' in href or '/chapter/' in href):
                                    full_url = urljoin(self.WEB_BASE, href)

                                    chapter_id_match = re.search(r'/(part|chapter)/(\d+)', full_url)
                                    if chapter_id_match:
                                        chapter_id = chapter_id_match.group(2)
                                    else:
                                        chapter_id = f"{story_id}_{idx}"

                                    # Проверяем что это уникальная глава
                                    if not any(c.url == full_url for c in chapters):
                                        chapter = ChapterInfo(
                                            id=chapter_id,
                                            number=len(chapters) + 1,
                                            title=title[:200],
                                            url=full_url
                                        )
                                        chapters.append(chapter)

                                    if len(chapters) >= 20:
                                        break
                            except:
                                continue

                        if chapters:
                            logger.info(f"Найдено {len(chapters)} глав в оглавлении")
                            break
                except:
                    continue

            if not chapters:
                logger.warning(f"Оглавление не найдено для story_id: {story_id}")
                
            return chapters
        except Exception as e:
            logger.error(f"Ошибка извлечения оглавления: {e}")
            return []

    def _extract_chapter_content(self, page, chapter_url: str) -> Optional[Dict]:
        """Извлекает содержимое главы"""
        try:
            page.goto(chapter_url, wait_until='domcontentloaded', timeout=self.timeout)
            time.sleep(random.uniform(1, 2))

            content_selectors = [
                '[data-testid="story-part-text"]',
                '.story-part-text',
                '.panel-reading',
                'article',
                '.chapter-content',
                '.reader-content'
            ]

            for selector in content_selectors:
                try:
                    content_element = page.query_selector(selector)
                    if content_element:
                        text = content_element.inner_text()

                        lines = text.split('\n')
                        cleaned_lines = [line.strip() for line in lines if line.strip()]
                        cleaned_text = '\n'.join(cleaned_lines)
                        word_count = len(cleaned_text.split())

                        return {
                            'text': cleaned_text[:10000],
                            'word_count': word_count
                        }
                except:
                    continue

            return None
        except Exception as e:
            return None

    def _extract_comments_for_chapter(self, page, chapter_url: str) -> List[Dict]:
        """Извлекает ВСЕ комментарии для главы, включая inline-комментарии (привязанные к параграфам)"""
        inline_comments_data = []
        
        try:
            # Переходим на страницу главы если нужно
            if page.url != chapter_url:
                page.goto(chapter_url, wait_until='domcontentloaded', timeout=self.timeout)
            
            # Ждем загрузки
            time.sleep(random.uniform(2, 3))
            
            # Прокручиваем страницу для загрузки контента
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            
            # === 1. Сбор inline-комментариев (привязанных к параграфам) ===
            try:
                # Ищем параграфы с комментариями
                paragraph_selectors = [
                    'p[data-comment-count]',  # Параграфы с атрибутом количества комментов
                    '.paragraph-container',
                    '[data-testid="paragraph"]',
                    '.story-text p'
                ]
                
                for selector in paragraph_selectors:
                    paragraphs = page.query_selector_all(selector)
                    for p_idx, para in enumerate(paragraphs):
                        try:
                            # Проверяем есть ли комментарии у параграфа
                            comment_count_attr = para.get_attribute('data-comment-count')
                            if comment_count_attr and int(comment_count_attr) > 0:
                                # Кликаем чтобы открыть inline-комментарии
                                para.click()
                                time.sleep(1)
                                
                                # Ищем появившиеся inline-комментарии
                                inline_selectors = [
                                    '.inline-comment',
                                    '[data-testid="inline-comment"]',
                                    '.paragraph-comment'
                                ]
                                
                                for inline_sel in inline_selectors:
                                    inline_comments = page.query_selector_all(inline_sel)
                                    for ic in inline_comments:
                                        try:
                                            text_el = ic.query_selector('.comment-text, .inline-comment-text, [data-testid="comment-text"]')
                                            author_el = ic.query_selector('.comment-author, .inline-comment-author, [data-testid="comment-author"], a[href*="/user/"]')
                                            date_el = ic.query_selector('time, .timestamp, .comment-date')
                                            likes_el = ic.query_selector('.like-count, .likes-count, [data-testid="likes"]')
                                            
                                            text = text_el.inner_text().strip() if text_el else ""
                                            author = author_el.inner_text().strip() if author_el else "Unknown"
                                            date = ""
                                            if date_el:
                                                date = date_el.get_attribute('datetime') or date_el.inner_text().strip()
                                            
                                            likes = 0
                                            if likes_el:
                                                likes_text = likes_el.inner_text().strip()
                                                likes_match = re.search(r'(\d+)', likes_text)
                                                if likes_match:
                                                    likes = int(likes_match.group(1))
                                            
                                            if text:
                                                inline_comments_data.append({
                                                    'text': text,
                                                    'author': author,
                                                    'date': date,
                                                    'likes': likes,
                                                    'type': 'inline',
                                                    'paragraph_index': p_idx
                                                })
                                        except:
                                            continue
                                    
                                    if inline_comments:
                                        break
                                        
                                # Закрываем попап если открыт
                                try:
                                    close_btn = page.query_selector('.close-btn, [data-testid="close"], .modal-close')
                                    if close_btn:
                                        close_btn.click()
                                        time.sleep(0.5)
                                except:
                                    pass
                                    
                        except:
                            continue
                    
                    if inline_comments_data:
                        break
                        
            except Exception as e:
                logger.debug(f"Ошибка при сборе inline-комментариев: {e}")
            
            # === 2. Сбор обычных комментариев через API ===
            regular_comments_data = []
            try:
                # Извлекаем ID главы из URL
                part_id = self.api_client.get_story_id_from_url(chapter_url)
                # Если не удалось или это ID истории, пробуем найти part_id в HTML
                if not part_id or len(part_id) < 6: # ID главы обычно длинный
                     try:
                         # wattpad.com/12345-title -> 12345
                         match = re.search(r'wattpad.com/(\d+)', chapter_url)
                         if match:
                             part_id = match.group(1)
                     except:
                         pass

                if part_id:
                    logger.info(f"Запрос комментариев через API для главы {part_id}...")
                    api_comments = self.api_client.get_part_comments(part_id, limit=50) # Лимит можно настроить
                    
                    for c in api_comments:
                        try:
                            # Основной комментарий
                            user = c.get('user', {})
                            author_name = user.get('name') or user.get('username') or "Unknown"
                            text = c.get('text', '')
                            date = c.get('created', '')
                            
                            comment_obj = {
                                'text': text,
                                'author': author_name,
                                'date': date,
                                'likes': c.get('voteCount', 0),
                                'type': 'regular',
                                'replies': []
                            }
                            
                            # Ответы
                            if c.get('replyCount', 0) > 0:
                                try:
                                    # Используем client.get_comment_replies
                                    replies = self.api_client.get_comment_replies(c['commentId'])
                                    for r in replies:
                                        r_user = r.get('user', {})
                                        r_author = r_user.get('name') or r_user.get('username') or "Unknown"
                                        comment_obj['replies'].append({
                                            'text': r.get('text', ''),
                                            'author': r_author,
                                            'date': r.get('created', ''),
                                            'likes': r.get('voteCount', 0)
                                        })
                                except Exception as e:
                                    logger.debug(f"Ошибка получения ответов: {e}")
                                    
                            regular_comments_data.append(comment_obj)
                            
                        except Exception as e:
                            logger.error(f"Ошибка обработки комментария API: {e}")
                            continue
                            
                    logger.info(f"API вернул {len(regular_comments_data)} комментариев")
                    
                else:
                    logger.warning(f"Не удалось определить Part ID для API комментариев: {chapter_url}")
            
            except Exception as e:
                logger.error(f"Ошибка при сборе комментариев через API: {e}")

            # Объединяем результаты
            all_comments = inline_comments_data + regular_comments_data
            if all_comments:
                logger.info(f"Собрано комментариев: {len(all_comments)} (inline: {len(inline_comments_data)}, обычных: {len(regular_comments_data)})")
            
            return all_comments
            
        except Exception as e:
            logger.error(f"Критическая ошибка при сборе комментариев: {e}")
            return []

    def _filter_stories_by_year_and_language(self, stories: List[StoryInfo], strict_mode: bool = True) -> List[StoryInfo]:
        """Фильтрация книг по году и языку"""
        filtered = []
        stats = {'year_mismatch': 0, 'lang_mismatch': 0, 'passed': 0, 'year_unknown': 0}

        for story in stories:
            # === Фильтр по году ===
            year_ok = False
            
            if not strict_mode:
                # В мягком режиме верим поиску (если искали в 2024, то считаем, что результаты релевантны)
                # Но если год явно указан и очень старый (например, < 2020), можно отсеять? 
                # Для упрощения: в мягком режиме пропускаем всё, кроме явного несоответствия
                if story.year_published and story.year_published < (self.target_year - 5): # Старее 5 лет
                     year_ok = False
                else:
                     year_ok = True
                     stats['year_unknown'] += 1 # В мягком режиме считаем как "пропущенные"
            else:
                # Строгий режим
                if story.year_published is None or story.year_published == 0:
                    year_ok = True # Год не определён - даем шанс
                    stats['year_unknown'] += 1
                elif story.year_published == self.target_year:
                    year_ok = True
                else:
                    year_ok = False

            if not year_ok:
                stats['year_mismatch'] += 1
                if strict_mode: # Логируем только в строгом, чтобы не спамить
                    logger.debug(f"Отсеяно по году: {story.title} (год: {story.year_published}, нужен: {self.target_year})")
                continue

            # === Фильтр по языку ===
            story_lang = story.language.upper() if story.language else 'UNKNOWN'
            allowed_langs = [lang.upper() for lang in self.languages]

            lang_ok = False
            if story_lang == 'UNKNOWN':
                lang_ok = True
            elif story_lang in allowed_langs:
                lang_ok = True
            else:
                # Попробуем мягкое сравнение (ru != RUSSIAN, но вдруг)
                if not strict_mode:
                     # В мягком режиме мы доверяем тегам поиска (мы искали "романтика", значит это RU)
                     # Детектор языка может ошибаться из-за интерфейса
                     lang_ok = True
                else:
                    lang_ok = False

            if not lang_ok:
                stats['lang_mismatch'] += 1
                logger.debug(f"Отсеяно по языку: {story.title} (язык: {story_lang}, нужны: {allowed_langs})")
                continue

            stats['passed'] += 1
            filtered.append(story)

        logger.info(f"Фильтрация (strict={strict_mode}): прошло {stats['passed']}, отсеяно по году: {stats['year_mismatch']}, по языку: {stats['lang_mismatch']}")
        return filtered

    def _save_results_to_files(self, stories: List[StoryInfo]):
        """Сохранение результатов в файлы"""
        if not stories:
            logger.warning("Нет книг для сохранения")

            json_path = f"{self.results_dir}/wattpad_{self.target_year}.json"
            try:
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump([], f, ensure_ascii=False, indent=2)
                logger.info(f"Создан пустой JSON: {json_path}")
            except Exception as e:
                logger.error(f"Ошибка создания JSON: {e}")

            return

        # Сохраняем в JSON
        json_path = f"{self.results_dir}/wattpad_{self.target_year}.json"
        try:
            with open(json_path, 'w', encoding='utf-8') as f:
                # Очистка данных перед сохранением
                cleaned_data = []
                for story in stories:
                    story_dict = story.to_dict()
                    cleaned_story = self._clean_data_recursive(story_dict)
                    cleaned_data.append(cleaned_story)
                    
                json.dump(cleaned_data, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"JSON сохранен: {json_path}")
        except Exception as e:
            logger.error(f"Ошибка JSON: {e}")

        # Сохраняем в CSV
        csv_path = f"{self.results_dir}/wattpad_{self.target_year}.csv"
        try:
            if stories:
                fieldnames = list(stories[0].to_dict().keys())
                with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                    # Для CSV берем ключи первого элемента (возможно не все поля если структура сложная)
                    # Плоская структура лучше для CSV, но у нас вложенность (chapters -> comments)
                    # Поэтому в CSV сохраняем только основные данные книги
                    
                    # Очищаем данные
                    cleaned_stories = [self._clean_data_recursive(s.to_dict()) for s in stories]
                    
                    # Убираем сложные поля для CSV
                    for s in cleaned_stories:
                        s.pop('chapters_data', None) # Если такое поле есть
                        # Данные глав и комментариев в CSV одной таблицей не сохранить красиво
                    
                    fieldnames = list(cleaned_stories[0].keys())
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    for story_dict in cleaned_stories:
                        writer.writerow(story_dict)
                logger.info(f"CSV сохранен: {csv_path}")
        except Exception as e:
            logger.error(f"Ошибка CSV: {e}")

        # Сохраняем статистику
        self._save_statistics(stories)

    def _save_statistics(self, stories: List[StoryInfo]):
        """Сохранение статистики"""
        stats_path = f"{self.results_dir}/statistics.txt"

        try:
            with open(stats_path, 'w', encoding='utf-8') as f:
                f.write(f"{'=' * 60}\n")
                f.write(f"СТАТИСТИКА СБОРА КНИГ WATTPAD {self.target_year} ГОДА\n")
                f.write(f"{'=' * 60}\n\n")

                f.write(f"ОБЩАЯ ИНФОРМАЦИЯ:\n")
                f.write(f"  • Год поиска: {self.target_year}\n")
                f.write(f"  • Языки: {', '.join(self.languages)}\n")
                f.write(f"  • Найдено книг: {len(stories)}\n")
                f.write(f"  • Глав спарсено: {self.stats['chapters_parsed']}\n")
                f.write(f"  • Комментариев спарсено: {self.stats['comments_parsed']}\n")
                f.write(f"  • Ошибок: {self.stats['errors']}\n")

                total_time = self.stats['end_time'] - self.stats['start_time']
                hours = int(total_time // 3600)
                minutes = int((total_time % 3600) // 60)
                seconds = int(total_time % 60)
                f.write(f"  • Время выполнения: {hours:02d}:{minutes:02d}:{seconds:02d}\n\n")

                # Статистика по языкам
                from collections import defaultdict
                lang_stats = defaultdict(int)
                for story in stories:
                    lang_stats[story.language] += 1

                f.write(f"РАСПРЕДЕЛЕНИЕ ПО ЯЗЫКАМ:\n")
                for lang, count in sorted(lang_stats.items(), key=lambda x: x[1], reverse=True):
                    percentage = (count / len(stories)) * 100 if stories else 0
                    f.write(f"  • {lang}: {count} книг ({percentage:.1f}%)\n")

                if stories:
                    f.write(f"\nТОП-10 КНИГ ПО ПРОСМОТРАМ:\n")
                    sorted_stories = sorted(stories, key=lambda x: x.read_count, reverse=True)[:10]
                    for i, story in enumerate(sorted_stories, 1):
                        f.write(f"  {i}. {story.title[:50]}... - {story.read_count:,} прочтений\n")

            logger.info(f"Статистика сохранена: {stats_path}")

        except Exception as e:
            logger.error(f"Ошибка статистики: {e}")

    def _clean_data_recursive(self, data):
        """Рекурсивная очистка данных от суррогатных символов и проблемных Unicode"""
        if isinstance(data, dict):
            return {self._clean_string(k) if isinstance(k, str) else k: self._clean_data_recursive(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._clean_data_recursive(item) for item in data]
        elif isinstance(data, str):
            return self._clean_string(data)
        else:
            return data
    
    def _clean_string(self, text: str) -> str:
        """Очистка строки от суррогатных символов и проблемных Unicode"""
        if not text:
            return text
        try:
            # Сначала пробуем закодировать с заменой суррогатов
            cleaned = text.encode('utf-8', 'surrogatepass').decode('utf-8', 'ignore')
            # Затем удаляем оставшиеся проблемные символы
            result = []
            for char in cleaned:
                try:
                    # Проверяем, что символ можно закодировать
                    char.encode('utf-8')
                    result.append(char)
                except (UnicodeEncodeError, UnicodeDecodeError):
                    # Заменяем проблемный символ на пробел или пропускаем
                    pass
            return ''.join(result)
        except Exception:
            # Крайний случай - удаляем все не-ASCII
            return ''.join(char for char in text if ord(char) < 128)