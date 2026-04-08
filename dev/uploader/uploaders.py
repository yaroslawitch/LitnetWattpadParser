import psycopg2
from datetime import datetime
from uuid import UUID


from domain.models import (
    Book, Genre, Tag, User, # Chapter,
     Comment, Reward
)

from domain.config import Config


# Использование класса:
# uploader = LitnetUploader(config)
# run_id = uploader.load(books, reuse run)
# print(run_id)
# Загрузчик возвращает id созданного снапшота инфы, собранной парсером

# config = Config(
#     dbname = "",
#     dbuser = "",
#     dbpassword = "",
#     dbhost = "",
#     dbport = 
# )

class LitnetUploader:
    def __init__(self, config: Config):

        self.config = config

    # Главная функция, которая осуществляет загрузку книг. Получает список Books.

    def load(self, books: list[Book], reuse_run) -> UUID:
        """
        Загружает список книг как один снашпот (parser_run).
        """
        with psycopg2.connect(
                                dbname=self.config.dbname,
                                user=self.config.dbuser,
                                password=self.config.dbpassword,
                                host=self.config.dbhost,
                                port=self.config.dbport
                            ) as conn:
            
            with conn.cursor() as cur:
                run_id = self._get_or_create_parser_run(cur, reuse_run)

                for book in books:
                    self._load_book(cur, book, run_id)

                return run_id

    #
    # Создание записи о запуске парсера
    #
    def _get_or_create_parser_run(self, cur, reuse_run: bool) -> UUID:
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
            (datetime.utcnow(),)
        )
        return cur.fetchone()[0]

    #
    # Загрузка одной книги, а также всех данных о ней
    #
    def _load_book(self, cur, book: Book, run_id: UUID) -> None:
        cur.execute(
            """
            INSERT INTO book (
                name, link, rating, likes, views, cycle,
                times_saved_to_library,
                publication_start_date, publication_end_date,
                price, contains_profanity, is_finished,
                age_restriction, description, run_id
            )
            VALUES (%s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s)
            RETURNING id
            """,
            (
                book.name,
                book.link,
                book.rating,
                book.likes,
                book.views,
                book.cycle,
                book.times_saved_to_library,
                book.publication_start_date,
                book.publication_end_date,
                book.price,
                book.contains_profanity,
                book.is_finished,
                book.age_restriction,
                book.description,
                run_id
            )
        )
        book_id = cur.fetchone()[0]

        self._load_authors(cur, book_id, book.authors, run_id)
        self._load_genres(cur, book_id, book.genres, run_id)
        self._load_tags(cur, book_id, book.tags, run_id)
        self._load_rewards(cur, book_id, book.rewards, run_id)
     #   self._load_chapters(cur, book_id, book.chapters, run_id)
        self._load_comments(cur, book_id, book.comments, run_id)

    #
    # Загрузка справочных сущностей. Если такой юзер / тег / жанр в БД уже имеется, то БД возвращает его id; если нет, то создает и возвращает id. Далее этот id
    # будет использоваться для создания связей между книгой и справочной сущностью: авторство, комменты, позиция в топах жанров и т.д.

    def _get_or_create_user(self, cur, user: User) -> UUID:
        cur.execute(
            """
            INSERT INTO "user" (name, link, created_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (link)
            DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            (user.username, user.link, datetime.utcnow())
        )
        return cur.fetchone()[0]

    def _get_or_create_genre(self, cur, genre: Genre) -> UUID:
        cur.execute(
            """
            INSERT INTO genre (name, link, created_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (name)
            DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            (genre.name, genre.link, datetime.utcnow())
        )
        return cur.fetchone()[0]

    def _get_or_create_tag(self, cur, tag: Tag) -> UUID:
        cur.execute(
            """
            INSERT INTO tag (name, link, created_at)
            VALUES (%s, %s, %s)
            ON CONFLICT (name)
            DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            """,
            (tag.name, tag.link, datetime.utcnow())
        )
        return cur.fetchone()[0]

    # 
    # Загрузка связей. Создание связующих сущностей между книгой и всем остальным
    # 

    def _load_authors(self, cur, book_id, authors, run_id):
        for author in authors:
            user_id = self._get_or_create_user(cur, author)
            cur.execute(
                """
                INSERT INTO books_users (book_id, user_id, run_id)
                VALUES (%s, %s, %s)
                """,
                (book_id, user_id, run_id)
            )

    def _load_genres(self, cur, book_id, genres, run_id):
        for genre in genres:
            genre_id = self._get_or_create_genre(cur, genre)
            cur.execute(
                """
                INSERT INTO books_genres
                    (book_id, genre_id, run_id, top_position)
                VALUES (%s, %s, %s, %s)
                """,
                (book_id, genre_id, run_id, genre.position)
            )

    def _load_tags(self, cur, book_id, tags, run_id):
        for tag in tags:
            tag_id = self._get_or_create_tag(cur, tag)
            cur.execute(
                """
                INSERT INTO books_tags (book_id, tag_id, run_id)
                VALUES (%s, %s, %s)
                """,
                (book_id, tag_id, run_id)
            )

    def _load_rewards(self, cur, book_id, rewards, run_id):
        for reward in rewards:
            cur.execute(
                """
                INSERT INTO reward (type, amount, book_id, run_id)
                VALUES (%s, %s, %s, %s)
                """,
                (reward.type, reward.amount, book_id, run_id)
            )

    def _load_chapters(self, cur, book_id, chapters, run_id):
        for chapter in chapters:
            cur.execute(
                """
                INSERT INTO chapter
                    (name, publication_date, book_id, run_id)
                VALUES (%s, %s, %s, %s)
                """,
                (
                    chapter.name,
                    chapter.publication_date,
                    book_id,
                    run_id,
                )
            )

    def _load_comments(self, cur, book_id, comments, run_id):
        for comment in comments:
            user_id = self._get_or_create_user(cur, comment.user)
            cur.execute(
                """
                INSERT INTO comment
                    (text, published_at, user_id, book_id, run_id)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    comment.text,
                    comment.published_at,
                    user_id,
                    book_id,
                    run_id,
                )
            )
