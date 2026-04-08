# quick_main.py
import sys
import os
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler

# Настройка кодировки для Windows
if os.name == 'nt':
    sys.stdout.reconfigure(encoding='utf-8')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# Настройка логирования с ротацией
log_handler = RotatingFileHandler(
    'quick_parser.log', 
    maxBytes=10*1024*1024,  # 10 MB
    backupCount=3,
    encoding='utf-8'
)
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[log_handler, logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def main():
    print("\n" + "=" * 70)
    print("БЫСТРЫЙ WATTPAD ПАРСЕР (RU, 20 ТЕГОВ, 2 КНИГИ НА ТЕГ)")
    print("=" * 70)

    try:
        from database import DatabaseManager
        from parser import WattpadParser
    except ImportError as e:
        print(f"Ошибка импорта: {e}")
        return

    # Настройки по умолчанию для быстрой версии
    year_input = input("\nВведите год (Enter = 2024): ").strip()
    year = int(year_input) if year_input else 2024
    
    # Жестко заданные настройки
    languages = ['ru']
    
    # Импортируем настройки из конфига
    from config import RU_TAGS, LIMIT_PER_GENRE_QUICK
    ru_tags = RU_TAGS
    limit_per_tag = LIMIT_PER_GENRE_QUICK
    
    print("\nНАСТРОЙКИ:")
    print(f"  • Год: {year}")
    print(f"  • Язык: ru (только русские теги)")
    print(f"  • Тегов: {len(ru_tags)}")
    print(f"  • Лимит книг на тег: {limit_per_tag}")
    print(f"  • Ожидается книг (макс): {len(ru_tags) * limit_per_tag}")
    
    confirm = input("\nНачать сканирование? (y/n): ").strip().lower()
    if confirm != 'y':
        return

    # Инициализация БД
    db_manager = DatabaseManager()
    db_manager.connect()

    # Инициализация парсера
    # max_stories ставим с запасом, так как лимит будет действовать внутри жанров
    parser = WattpadParser(
        db_manager=db_manager,
        year=year,
        languages=languages,
        headless=True,
        max_stories=100, 
        parse_chapters=True,
        parse_comments=True
    )

    # Запускаем сбор с переопределенными жанрами и лимитом
    # ВНИМАНИЕ: Мы изменили метод в parser.py, чтобы он принимал эти аргументы
    print(f"\nЗапуск...")
    stories = parser.collect_stories_for_year(
        override_genres=ru_tags,
        limit_per_genre=limit_per_tag,
        strict_mode=False # Отключаем строгую проверку года, чтобы не отсеивать результаты
    )
    
    print("\n" + "=" * 70)
    print(f"Завершено. Найдено книг: {len(stories)}")
    print("Результаты сохранены в папку results_" + str(year))
    
    db_manager.disconnect()

if __name__ == "__main__":
    main()
