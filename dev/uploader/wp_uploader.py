from parser.Wattpad.wattpad_models import Tag, Rank, User, Comment, Chapter, Book
from domain.wp_uploader_config import wp_Config
import psycopg2
from datetime import datetime
from uuid import UUID


class WattpadUploader:
    def __init__(self, config=wp_Config):
        self.config=config

    def items_to_Book_class(self, book_data):
        '''Конвертирует данные о книге из словаря в класс Book'''
        tags = [Tag(tag['name'], tag['link']) for tag in book_data['tags']] # конвертирует теги
        ranks = [Rank(rank['category'], rank['position'], rank['amount_of_books']) for rank in book_data['ranks']] # конвертирует топовые теги
        
        author = User(**book_data['author']) # конвертирует инфо об авторе
        
        chapters = []
        for ch in book_data['chapters']: # Конвертирует главу
            if ch['comments'] is not None:
                usual_comments = []
                for comm in ch['comments']: # комментарии
                    comm_user = User(**comm['user'])
                    replies = None
                    if comm['replies'] is not None:
                        replies = []
                        for repl in comm['replies']:
                            repl_user = User(**repl['user']) # комментарии-ответы
                            replies.append(Comment(repl['text'], repl['published_at'], repl_user, repl['likes'], repl['num_replies'], None, repl['comm_id']))
                    usual_comments.append(Comment(comm['text'], comm['published_at'], comm_user, comm['likes'], comm['num_replies'], replies, comm['comm_id']))
                chapters.append(Chapter(ch['name'], ch['link'], ch['votes'], ch['views'], ch['num_comments'], ch['published_at'], usual_comments))
            else:
                chapters.append(Chapter(ch['name'], ch['link'], ch['votes'], ch['views'], ch['num_comments'], ch['published_at'], None))
        
        # сбор книги 
        book = Book(book_data['name'], book_data['description'], book_data['link'], book_data['votes'], book_data['views'], book_data['is_mature'],
                book_data['is_finished'], author, tags, book_data['num_chapters'], chapters, ranks, book_data.get('duration', None), book_data['id']
                )
            
        return book

    def load(self, books: list[Book]) -> UUID:
        """
        Загружает список книг как один снашпот (parser_run).
        """
        with psycopg2.connect(
                                dbname= self.config.dbname,
                                user=self.config.dbuser,
                                password=self.config.dbpassword,
                                host=self.config.dbhost,
                                port=self.config.dbport
                            ) as conn:
            
            with conn.cursor() as cur:
                run_id = self._get_or_create_parser_run(cur, False)
                print('Запись в базу данных...')
                for book in books:
                    book = self.items_to_Book_class(book)
                    self._load_book(cur, book, run_id)
                print('Запись завершена.')
                return run_id


    #
    # Создание записи о запуске парсера
    #
    def _get_or_create_parser_run(self, cur, reuse_run) -> UUID:
        """
        Если reuse_run = True  возвращает последний parser_run.id
        Если reuse_run = False или parser_run еще не существует создаёт новый parser_run
        """

        if reuse_run:
            cur.execute(
                """
                SELECT id
                FROM parser_run
                ORDER BY run_at DESC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            if row is not None:
                return row[0]
            # если run-ов ещё нет — создаем новый

        cur.execute(
            """
            INSERT INTO parser_run (run_at)
            VALUES (%s)
            RETURNING id
            """,
            (str(datetime.utcnow()),)
        )
        return cur.fetchone()[0]


    #
    # Загрузка справочных сущностей. Если такой юзер / тег / жанр в БД уже имеется, то БД возвращает его id; если нет, то создает и возвращает id. Далее этот id
    # будет использоваться для создания связей между книгой и справочной сущностью: авторство, комменты, позиция в топах жанров и т.д.


    def _get_or_create_user(self, cur, user: User, run_id) -> UUID:
        cur.execute(
            """
            INSERT INTO "user" (name, link, run_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (link, run_id)
            DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            (user.username, user.link, run_id)
        )
        return cur.fetchone()[0]


    def _get_or_create_tag(self, cur, tag: Tag, run_id) -> UUID:
        cur.execute(
            """
            INSERT INTO tag (name, link, run_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (name, run_id)
            DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            (tag.name, tag.link, run_id)
        )
        return cur.fetchone()[0]
        
    def _get_or_create_book_info(self, cur, book: Book, author_id, run_id) -> UUID:
        
        cur.execute(
            """
            INSERT INTO book (name, link, votes, views, chapters, is_mature, is_finished, description, duration, author, run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (link, run_id)
            DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            (book.name, book.link, book.votes, book.views, book.num_chapters, book.is_mature, book.is_finished,
                book.description, book.duration, author_id, run_id)
        )
        return cur.fetchone()[0]
    
    def _get_or_create_chapter(self, cur, chapter: Chapter, book_id, run_id):
        cur.execute(
            """
            INSERT INTO chapter (name, link, votes, views, comments, published_at, book, run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (link, run_id)
            DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            (chapter.name, chapter.link, chapter.votes, chapter.views, chapter.num_comments,
                chapter.published_at, book_id, run_id)
        )
        return cur.fetchone()[0]
    
    def _get_or_create_comment(self, cur, comment: Comment, chapter_id, user_id, run_id, parent_id=None):
        cur.execute(
            """
            INSERT INTO comment (text, published_at, likes, replies, chapter, "user", run_id, parent_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (text, published_at, "user", chapter, run_id)
            DO UPDATE SET text = EXCLUDED.text
            RETURNING id
            """,
            (comment.text, comment.published_at, comment.likes,
                comment.num_replies, chapter_id, user_id, run_id, parent_id)
        )
        return cur.fetchone()[0]

    # 
    # Загрузка связей. Создание связующих сущностей между книгой и всем остальным
    # 
    
    def _load_book_info(self, cur, book: Book, author_id, run_id):
        cur.execute(
            """
            INSERT INTO book (name, link, votes, views, chapters, is_mature, is_finished, description, duration, author, run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (book.name, book.link, book.votes, book.views, book.num_chapters, book.is_mature, book.is_finished,
                book.description, book.duration, author_id, run_id)
        )
        
    def _load_ranks(self, cur, ranks: list[Rank], book_id) -> UUID: #####
        for rank in ranks:
            cur.execute(
                """
                INSERT INTO rank (category, position, amount_of_books, book)
                VALUES (%s, %s, %s, %s)
                """,
                (rank.category, rank.position, rank.amount_of_books, book_id)
            )
        
    def _load_tags(self, cur, tags, run_id):
        for tag in tags:
            cur.execute(
                """
                INSERT INTO tag (name, link, run_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (name, run_id)
                DO NOTHING
                """,
                (tag.name, tag.link, run_id)
            )
    
    def _load_book_by_tag(self, cur, book_id, tag_id, run_id):
        cur.execute(
            """
            INSERT INTO book_by_tag (book, tag, run_id)
            VALUES (%s, %s, %s)
            """,
            (book_id, tag_id, run_id)
        )
        
    def _load_chapter(self, cur, chapter: Chapter, book_id, run_id):
        cur.execute(
            """
            INSERT INTO chapter (name, link, votes, views, comments, published_at, book, run_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (chapter.name, chapter.link, chapter.votes, chapter.views, chapter.num_comments,
                chapter.published_at, book_id, run_id)
        )
        
    def _load_comment(self, cur, comment: Comment, chapter_id, user_id, run_id, parent_id=None):
        
        cur.execute(
            """
            INSERT INTO comment (text, published_at, likes, replies, chapter, "user", run_id, parent_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (text, published_at, "user", chapter, run_id)
            DO NOTHING
            """, #            ON CONFLICT (text, published_at, "user", chapter)
            #DO NOTHING
            (comment.text, comment.published_at, comment.likes,
                comment.num_replies, chapter_id, user_id, run_id, parent_id)
        )
    
    def _load_book(self, cur, book: Book, run_id: UUID): # -> None
        """Заполняет базу данных одной книгой"""
        author_id = self._get_or_create_user(cur, book.author, run_id)
        
        self._load_book_info(cur, book, author_id, run_id) # загрузка инфо о книге
        book_id = self._get_or_create_book_info(cur, book, author_id, run_id)
        
        self._load_ranks(cur, book.ranks, book_id)
        self._load_tags(cur, book.tags, run_id) # Загрузка тегов
        for tag in book.tags:
            tag_id = self._get_or_create_tag(cur, tag, run_id)
            
            self._load_book_by_tag(cur, book_id, tag_id, run_id) # заполнение book_by_tag
        
        for chapter in book.chapters:
            self._load_chapter(cur, chapter, book_id, run_id) # заполнение главы
            chapter_id = self._get_or_create_chapter(cur, chapter, book_id, run_id)
            
            
            if chapter.comments is not None:
                for comment in chapter.comments:
                    user_id = self._get_or_create_user(cur, comment.user, run_id)
                    self._load_comment(cur, comment, chapter_id, user_id, run_id) # заполнение коммента
                    parent_id = self._get_or_create_comment(cur, comment, chapter_id, user_id, run_id)
                    
                    if comment.replies is not None:
                        for reply in comment.replies:               # заполнение комментов-ответов
                            self._load_comment(cur, reply, chapter_id, user_id, run_id, parent_id)
            
        
        
