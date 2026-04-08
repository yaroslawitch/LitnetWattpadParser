import json
from dataclasses import is_dataclass, fields

from domain.config import Config
from domain.models import Book
from uploader.uploaders import LitnetUploader


# Универсальная функция dict → dataclass (включая вложенные структуры)
def from_dict(cls, data):
    if not is_dataclass(cls):
        return data

    kwargs = {}
    for f in fields(cls):
        value = data.get(f.name)

        # Список вложенных объектов
        if isinstance(value, list):
            # например list[User] → User
            inner_type = f.type.__args__[0]
            kwargs[f.name] = [from_dict(inner_type, v) for v in value]

        # Вложенный объект
        elif is_dataclass(f.type) and isinstance(value, dict):
            kwargs[f.name] = from_dict(f.type, value)

        else:
            kwargs[f.name] = value

    return cls(**kwargs)


# Конфиг подключения
config = Config(
    dbname="reviewsdb",
    dbuser="reviews_user",
    dbpassword="super_secret_password",
    dbhost="localhost",
    dbport=5002
)

# Загружаем JSON
with open("./parser/ParserV2/books.json", "r", encoding="utf-8") as f:
    raw_books = json.load(f)

# Преобразуем dict → Book (автоматически)
books = [from_dict(Book, b) for b in raw_books]

# Гарантируем, что chapters всегда список
# for book in books:
#     if book.chapters is None:
#         book.chapters = []


# Загружаем в базу
uploader = LitnetUploader(config)
run_id = uploader.load(books, reuse_run=False)

print("Loaded run:", run_id)
