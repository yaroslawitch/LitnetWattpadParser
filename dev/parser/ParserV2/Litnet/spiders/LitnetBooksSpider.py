import scrapy
import re
import os
import json
import zlib
from domain.models import Book, User, Comment, Genre, Tag, Reward


class LitnetBooksSpider(scrapy.Spider):
    name = "LitnetBooksParser"
    allowed_domains = ["litnet.com"]

    # Константы для режимов
    MODE_SIMPLE = 'simple'
    MODE_JOBDIR = 'jobdir'
    MODE_OFFSET_FILE = 'offset_file'

    def __init__(self, mode=None, offset_file_path='last_offset.txt', *args, **kwargs):
        """
        Инициализация парсера с разными режимами.

        Args:
            mode: Режим работы ('simple', 'jobdir', 'offset_file')
            offset_file_path: Путь к файлу offset (только для mode='offset_file')
        """
        super().__init__(*args, **kwargs)

        self.limit = 10
        self.offset_file_path = offset_file_path

        # Определяем режим работы
        if mode:
            self.mode = mode
        else:
            # Автоматически определяем режим на основе настроек и аргументов
            if self.crawler.settings.get('JOBDIR'):
                self.mode = self.MODE_JOBDIR
            elif hasattr(self, 'mode'):
                self.mode = self.mode
            else:
                self.mode = self.MODE_SIMPLE

        # Инициализируем offset в зависимости от режима
        if self.mode == self.MODE_JOBDIR:
            # Для JOBDIR используем атрибут spider, который сохранится автоматически
            if not hasattr(self, 'current_offset'):
                self.current_offset = 0
        elif self.mode == self.MODE_OFFSET_FILE:
            # Для файлового режима читаем из файла
            try:
                if os.path.exists(self.offset_file_path):
                    with open(self.offset_file_path, 'r') as f:
                        self.current_offset = int(f.read().strip())
                        self.logger.info(f"Loaded offset from file: {self.current_offset}")
                else:
                    self.current_offset = 0
                    self.logger.info("Offset file not found, starting from 0")
            except (ValueError, Exception) as e:
                self.logger.warning(f"Failed to read offset file: {e}. Starting from 0")
                self.current_offset = 0
        else:
            # Простой режим - всегда с 0
            self.current_offset = 0

        self.logger.info(f"Spider initialized in '{self.mode}' mode with offset={self.current_offset}")

    def start_requests(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://litnet.com/',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        }

        # Генерируем первый запрос
        yield self._create_api_request(self.current_offset, headers)

    def _create_api_request(self, offset, headers):
        """Создает запрос к API с заданным offset"""
        url = (
            'https://superapi.litnet.com/v2/genres/top'
            f'?limit={self.limit}&offset={offset}&createdAt=365'
            '&sort=rate&sortDirection=DESC'
        )

        return scrapy.Request(
            url=url,
            headers=headers,
            callback=self.parse_ajax,
            meta={'offset': offset}
        )

    def _save_offset(self, offset):
        """Сохраняет offset в зависимости от режима"""
        if self.mode == self.MODE_OFFSET_FILE:
            try:
                with open(self.offset_file_path, 'w') as f:
                    f.write(str(offset))
                self.logger.info(f"Saved offset to file: {offset}")
            except Exception as e:
                self.logger.error(f"Failed to save offset to file: {e}")

        elif self.mode == self.MODE_JOBDIR:
            # Для JOBDIR offset сохраняется автоматически в атрибуте spider
            pass

        # Для простого режима ничего не сохраняем

    def parse_ajax(self, response):
        data = response.json()
        items = data.get("items", [])

        current_offset = response.meta['offset']

        if not items:
            self.logger.info("No more books found. Stopping spider.")

            # Очищаем файл offset при завершении
            if self.mode == self.MODE_OFFSET_FILE and os.path.exists(self.offset_file_path):
                os.remove(self.offset_file_path)
                self.logger.info("Offset file removed - parsing completed")

            return

        for book in items:
            alias = book.get('alias')
            if alias:
                yield response.follow(
                    f'https://litnet.com/ru/book/{alias}',
                    callback=self.parse_book
                )

        # Генерируем следующий запрос
        next_offset = current_offset + self.limit
        self.current_offset = next_offset

        # Сохраняем offset
        self._save_offset(next_offset)

        # Создаем следующий запрос к API
        yield self._create_api_request(next_offset, response.request.headers)

    def __safe_int(self, value):
        """
        Безопасно извлекает целое число из строки.

        Возвращает:
        - int, если в строке есть цифры
        - None, если value = None, пусто или цифр нет
        """

        if value is None:
            return None

        # Приводим к строке (на случай, если пришёл int / float)
        value = str(value).strip()

        if not value:
            return None

        # Убираем все типы пробелов (включая неразрывные)
        value = re.sub(r'\s+', '', value)

        # Ищем первую непрерывную группу цифр
        match = re.search(r'\d+', value)

        if not match:
            return None

        try:
            return int(match.group())
        except ValueError:
            return None

    def parse_book(self, response):
        self.logger.info(f"Parsing book page: {response.url}")

        id = int(response.url.split("-b")[-1])

        book_name = response.css("div.book-view-info h1::text").get()
        views = self.__safe_int(
            response.css("span.count-views::text").get()
        )
        rating = self.__safe_int(
            response.css("span.book-rating-info-value span::text").get()
        )
        likes = self.__safe_int(
            response.css("div.book-rating-buttons span::text").get()
        )
        cycle = response.css("span.meta-name + a::text").get()

        dates = response.css("div.book-view-info-coll p span::text").getall()
        publication_start_date = None
        publication_end_date = None
        is_finished = None

        if dates:
            parts = dates[-1].split("—")
            publication_start_date = parts[0].strip()
            if parts[-1].strip() == '...' or parts[-1].strip() == '':
                publication_end_date = None
                is_finished = False
            else:
                publication_end_date = parts[-1].strip()
                is_finished = True

        description = "".join(
            response.css("div.tab-pane::text").getall()
        ).strip()

        age = dates[2] if len(dates) > 2 else ""
        match = re.search(r"\d+\+", age)
        age_restriction = match.group() if match else None

        contains_profanity = bool(
            response.css("span.content-mark-label::text").get()
        )

        # Собираем авторов как объекты User
        authors = []
        author_names = response.css("a.author span::text").getall()
        author_links = response.css("a.author::attr(href)").getall()

        for name, link in zip(author_names, author_links):
            if name and link:
                authors.append(User(
                    username=name.strip(),
                    link=link
                ))

        price = None
        price_texts = response.css(
            'div.ln_btn_get_link span.ln_btn-get-text::text'
        ).getall()

        if price_texts:
            text = price_texts[-1].strip()
            match = re.search(r'\d+(?:[.,]\d+)?', text)
            if match:
                price = float(match.group().replace(',', '.'))

        # Собираем Genre как объекты
        genres = []
        genre_names = response.css('p a span::text').getall()
        genre_links = response.css('div.book-view-info-coll p').css('a::attr(href)').getall()
        glinks = [l for l in genre_links if '/top/' in l]
        texts = response.css('div.book-view-info-coll p:contains("Текущий рейтинг")').css('::text').getall()
        ranks = [
            int(m.group())
            for t in texts
            if (m := re.search(r'\d+', t))
        ]

        for name, link, rank in zip(genre_names, glinks, ranks):
            genres.append(Genre(
                name=name.strip(),
                link='https://litnet.com' + link,
                position=rank
            ))

        # Собираем Tag как объекты
        tags = []
        tag_nodes = response.css('div.book-view_fx p a')
        for node in tag_nodes:
            name = node.css("::text").get()
            href = node.attrib.get("href", "")

            # фильтруем только ссылки на теги
            if "/tag/" not in href:
                continue

            # создаем полный URL
            link = "https://litnet.com" + href

            tags.append(Tag(
                name=name.strip(),
                link=link
            ))

        times_saved_to_library = self.__safe_int(response.css('div span.count-favourites::text').get())
        num_comments = self.__safe_int(response.css('div.content div.comments-head h3::text').get())

        # Создаем временный объект Book без комментариев и наград
        book = Book(
            id=id,
            name=book_name,
            link=response.url,
            rating=rating,
            likes=likes,
            views=views,
            publication_start_date=publication_start_date,
            num_comments=num_comments,
            comments=[],  # Будем заполнять позже
            authors=authors,
            tags=tags,
            genres=genres,
            rewards=[],  # Будем заполнять позже
            cycle=cycle,
            publication_end_date=publication_end_date,
            price=price,
            contains_profanity=contains_profanity,
            age_restriction=age_restriction,
            description=description,
            is_finished=is_finished,
            times_saved_to_library=times_saved_to_library
        )

        # Если комментариев нет, сразу парсим награды и возвращаем книгу
        if num_comments == 0:
            rewards_url = f"https://litnet.com/ru/book/rewards-tab?id={id}"
            yield scrapy.Request(
                url=rewards_url,
                callback=self.parse_rewards_no_comments,
                meta={'book': book}
            )
        else:
            # Если комментарии есть, получаем количество страниц и начинаем парсинг
            last_page = response.css(
                'div.pagination-wrapper ul.pagination li.last a::text'
            ).get()
            last_page = int(last_page) if last_page else 1

            # Сначала парсим награды
            rewards_url = f"https://litnet.com/ru/book/rewards-tab?id={id}"
            yield scrapy.Request(
                url=rewards_url,
                callback=self.parse_rewards,
                meta={'book': book, 'last_page': last_page}
            )

    def parse_rewards_no_comments(self, response):
        """Обработка наград для книг без комментариев"""
        book = response.meta['book']
        self.logger.info(f"Parsing rewards for book_id: {book.id}")

        # Получаем все <li> наград
        reward_nodes = response.css("ul#rewards-list-showcase > li")
        rewards = []

        for node in reward_nodes:
            # Название награды
            reward_type = node.css("div.payment-link > p::text").get()
            if reward_type:
                reward_type = reward_type.strip()
            else:
                reward_type = None

            # Место награды (первое число в ul > li)
            amount_text = node.css("div.payment-link ul li::text").get()
            amount = self.__safe_int(amount_text)

            if amount is not None:
                rewards.append(Reward(
                    type=reward_type,
                    amount=amount
                ))

        # Обновляем book с наградами
        book.rewards = rewards
        yield book

    def parse_rewards(self, response):
        """Обработка наград для книг с комментариями"""
        book = response.meta['book']
        last_page = response.meta['last_page']
        self.logger.info(f"Parsing rewards for book_id: {book.id}")

        # Получаем все <li> наград
        reward_nodes = response.css("ul#rewards-list-showcase > li")
        rewards = []

        for node in reward_nodes:
            # Название награды
            reward_type = node.css("div.payment-link > p::text").get()
            if reward_type:
                reward_type = reward_type.strip()
            else:
                reward_type = None

            # Место награды (первое число в ul > li)
            amount_text = node.css("div.payment-link ul li::text").get()
            amount = self.__safe_int(amount_text)

            if amount is not None:
                rewards.append(Reward(
                    type=reward_type,
                    amount=amount
                ))

        # Обновляем book с наградами
        book.rewards = rewards

        # СТАРТУЕМ С ПЕРВОЙ СТРАНИЦЫ КОММЕНТАРИЕВ
        comments_url = book.link.split('?')[0] + "?cpage=1"

        yield scrapy.Request(
            url=comments_url,
            callback=self.parse_comments,
            meta={
                'book': book,
                'current_page': 1,
                'last_page': last_page
            }
        )

    def parse_comments(self, response):
        """
        Парсим одну страницу комментариев, добавляем их в book.comments,
        и если есть следующая страница — запрашиваем её.
        Когда дошли до последней, yield'им полный book.
        """
        meta = response.meta or {}
        book = meta.get('book')

        # Приводим current_page и last_page к int
        try:
            current_page = int(meta.get('current_page', 1))
        except (ValueError, TypeError):
            current_page = 1

        try:
            last_page = int(meta.get('last_page', 1))
        except (ValueError, TypeError):
            last_page = 1

        self.logger.info(
            f"Parsing comments page {current_page}/{last_page} for book_id={book.id}"
        )

        # Проходим по всем комментариям на странице
        comment_nodes = response.css('div.comment-item')
        for node in comment_nodes:
            # текст комментария
            text_parts = node.css('p.comment-text::text').getall()
            text = " ".join([t.strip() for t in text_parts if t and t.strip()])

            # дата
            published_at = node.css(
                'div.comment-head-text span.comment-date::text'
            ).get()
            if published_at:
                published_at = published_at.strip()

            # username
            username = node.css('a.comment-author-name span::text').get()
            username = username.strip() if username else "unknown"

            # user link
            href = node.css('a.comment-author-name::attr(href)').get()
            user_link = f'https://litnet.com/ru/{href}' if href else None

            # Создаём User и Comment (вложенные объекты по модели)
            user_obj = User(username=username, link=user_link)
            comment_obj = Comment(text=text, published_at=published_at, user=user_obj)

            # Добавляем комментарий в книгу
            book.comments.append(comment_obj)

            # ОТВЕТЫ НА КОММЕНТАРИЙ (вложенные)
            reply_nodes = node.css('.comment-children div.comment-item')
            for reply in reply_nodes:
                r_text_parts = reply.css('p.comment-text::text').getall()
                r_text = " ".join(t.strip() for t in r_text_parts if t and t.strip())

                r_published_at = reply.css('div.comment-head-text span.comment-date::text').get()
                if r_published_at:
                    r_published_at = r_published_at.strip()

                r_username = reply.css('a.comment-author-name span::text').get()
                r_username = r_username.strip() if r_username else "unknown"

                r_href = reply.css('a.comment-author-name::attr(href)').get()
                r_user_link = f'https://litnet.com/ru/{r_href}' if r_href else None

                r_user_obj = User(username=r_username, link=r_user_link)
                r_comment_obj = Comment(text=r_text, published_at=r_published_at, user=r_user_obj)

                book.comments.append(r_comment_obj)

        # Переходим на следующую страницу, если есть
        if current_page < last_page:
            next_page = current_page + 1
            base = response.url.split('?')[0]
            next_url = f"{base}?cpage={next_page}"

            self.logger.info(f"Requesting comments page {next_page} for book_id={book.id}")

            yield scrapy.Request(
                url=next_url,
                callback=self.parse_comments,
                meta={
                    'book': book,
                    'current_page': next_page,
                    'last_page': last_page
                }
            )
        else:
            # все страницы пройдены — возвращаем полный book
            self.logger.info(
                f"All comments collected for book_id={book.id}. Total comments: {len(book.comments)}"
            )
            yield book