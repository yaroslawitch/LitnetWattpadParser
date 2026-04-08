# main.py
import sys
import os
from datetime import datetime
import logging

# Настройка кодировки для Windows
if os.name == 'nt':
    sys.stdout.reconfigure(encoding='utf-8')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('wattpad_parser.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def main():
    """Главная функция"""

    print("\n" + "=" * 70)
    print("WATTPAD ПАРСЕР КНИГ ПО ГОДУ И ЯЗЫКУ")
    print("=" * 70)

    try:
        # Сначала проверяем зависимости
        print("Проверка зависимостей...")
        required_packages = {
            'playwright': 'playwright',
            'bs4': 'beautifulsoup4',
            'langdetect': 'langdetect'
        }

        missing_packages = []
        for package, pip_name in required_packages.items():
            try:
                __import__(package)
                print(f"[OK] {package}")
            except ImportError:
                missing_packages.append(pip_name)
                print(f"[X] {package} - отсутствует")

        if missing_packages:
            print(f"\nУстановите необходимые библиотеки:")
            print(f"pip install {' '.join(missing_packages)}")
            if 'playwright' in missing_packages:
                print("playwright install chromium")

            install_now = input("\nУстановить сейчас? (y/n): ").strip().lower()
            if install_now == 'y':
                import subprocess
                try:
                    subprocess.run([sys.executable, "-m", "pip", "install"] + missing_packages,
                                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    if 'playwright' in missing_packages:
                        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"],
                                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    print("Библиотеки успешно установлены!")
                except Exception as e:
                    print(f"Ошибка установки: {e}")
                    sys.exit(1)
            else:
                sys.exit(1)

        print("\nЗависимости проверены успешно!")

        # Импортируем модули
        print("Импорт модулей...")
        try:
            from database import DatabaseManager
            from parser import WattpadParser
            print("Модули импортированы успешно")
        except ImportError as e:
            print(f"Ошибка импорта модулей: {e}")
            import traceback
            traceback.print_exc()
            print("\nУбедитесь, что все файлы в одной папке:")
            print("  • database.py")
            print("  • parser.py")
            print("  • models.py")
            print("  • main.py")
            input("\nНажмите Enter для выхода...")
            return

        # Инициализация менеджера
        print("\n" + "=" * 50)
        print("НАСТРОЙКА СОХРАНЕНИЯ")
        print("=" * 50)
        print("Режим работы: Сохранение в файлы (JSON, CSV, TXT)")

        db_manager = DatabaseManager()
        db_manager.connect()
        print("[OK] Файловый менеджер готов")

        # Получаем год
        current_year = datetime.now().year

        while True:
            try:
                year_input = input(f"\nВведите год для поиска (2000-{current_year}): ").strip()
                if not year_input:
                    year = current_year
                else:
                    year = int(year_input)

                if 2000 <= year <= current_year:
                    break
                else:
                    print(f"Год должен быть между 2000 и {current_year}")
            except ValueError:
                print("Пожалуйста, введите корректный год")

        # Языки
        print("\nВыбор языков для поиска:")
        print("1. Только русский (ru)")
        print("2. Только английский (en)")
        print("3. Русский и английский (ru, en)")

        while True:
            lang_choice = input("Выберите вариант (1-3, Enter=3): ").strip()
            if not lang_choice:
                languages = ['ru', 'en']
                break
            elif lang_choice == '1':
                languages = ['ru']
                break
            elif lang_choice == '2':
                languages = ['en']
                break
            elif lang_choice == '3':
                languages = ['ru', 'en']
                break
            else:
                print("Пожалуйста, выберите 1, 2 или 3")

        # Режим сбора (полный или быстрый)
        use_quick_mode = False
        override_genres = None
        limit_per_genre = None
        strict_mode = True
        
        if languages == ['ru']:
            print("\n" + "=" * 50)
            print("РЕЖИМ СБОРА")
            print("=" * 50)
            print("1. Полный сбор (все жанры, без ограничений)")
            print("2. Быстрый сбор (20 русских тегов, 2 книги на тег = ~40 книг)")
            
            while True:
                mode_choice = input("Выберите режим (1-2, Enter=1): ").strip()
                if not mode_choice or mode_choice == '1':
                    use_quick_mode = False
                    break
                elif mode_choice == '2':
                    use_quick_mode = True
                    from config import RU_TAGS, LIMIT_PER_GENRE_QUICK
                    override_genres = RU_TAGS
                    limit_per_genre = LIMIT_PER_GENRE_QUICK
                    strict_mode = False
                    break
                else:
                    print("Пожалуйста, выберите 1 или 2")

        # Настройки парсинга
        print("\n" + "=" * 50)
        print("НАСТРОЙКИ ПАРСИНГА")
        print("=" * 50)
        print("1. Только информация о книгах (без глав)")
        print("2. Полный парсинг (книги + первые 3 главы)")

        while True:
            mode_input = input("Выберите режим (1-2, Enter=1): ").strip()
            if not mode_input:
                parse_chapters = False
                break
            elif mode_input == '1':
                parse_chapters = False
                break
            elif mode_input == '2':
                parse_chapters = True
                break
            else:
                print("Пожалуйста, выберите 1 или 2")

        # Настройка парсинга комментариев
        print("\nНастройка сбора комментариев:")
        print("1. Без комментариев (быстрее)")
        print("2. С комментариями (медленнее, собирает все комментарии)")

        while True:
            comments_input = input("Выберите вариант (1-2, Enter=1): ").strip()
            if not comments_input:
                parse_comments = False
                break
            elif comments_input == '1':
                parse_comments = False
                break
            elif comments_input == '2':
                parse_comments = True
                break
            else:
                print("Пожалуйста, выберите 1 или 2")

        # Настройка браузера
        print("\nНастройка браузера:")
        print("1. Headless (невидимый, быстрее)")
        print("2. Видимый (для отладки)")

        while True:
            browser_choice = input("Выберите вариант (1-2, Enter=1): ").strip()
            if not browser_choice:
                headless = True
                break
            elif browser_choice == '1':
                headless = True
                break
            elif browser_choice == '2':
                headless = False
                break
            else:
                print("Пожалуйста, выберите 1 или 2")

        # Подтверждение
        print("\n" + "=" * 70)
        print("ПОДТВЕРЖДЕНИЕ НАСТРОЕК")
        print("=" * 70)
        print(f"  • Год: {year}")
        print(f"  • Языки: {', '.join(languages)}")
        print(f"  • Парсинг глав: {'Да' if parse_chapters else 'Нет'}")
        print(f"  • Парсинг комментариев: {'Да' if parse_comments else 'Нет'}")
        print(f"  • Headless браузер: {'Да' if headless else 'Нет'}")
        print(f"  • Ограничение по количеству книг: Нет")
        print(f"  • Сохранение в файлы: Да (JSON, CSV, TXT)")
        if use_quick_mode:
            print(f"  • Быстрый режим: Да (20 тегов, 2 книги/тег)")
        else:
            print(f"  • Быстрый режим: Нет (полный сбор)")
        print("\nВАЖНО: Парсер будет собирать книги согласно выбранным настройкам.")
        if not use_quick_mode:
            print("Полный сбор может занять много времени.")
        print("=" * 70)

        confirm = input("\nНачать сбор ВСЕХ книг за указанный год? (y/n): ").strip().lower()
        if confirm != 'y':
            print("Отменено")
            db_manager.disconnect()
            return

        # Создание парсера БЕЗ ограничений по количеству книг
        print(f"\nИнициализация парсера...")
        parser = WattpadParser(
            db_manager=db_manager,
            year=year,
            languages=languages,
            headless=headless,
            timeout=120000,  # 2 минуты таймаут
            max_stories=None,  # БЕЗ ОГРАНИЧЕНИЙ
            parse_chapters=parse_chapters,
            parse_comments=parse_comments
        )

        # Запуск сбора
        print(f"\nНачало сбора ВСЕХ книг за {year} год...")
        print(f"Время: {datetime.now().strftime('%H:%M:%S')}")
        print("=" * 50)

        print("\nПРОЦЕСС СБОРА:")
        print("1. Загрузка главной страницы Wattpad...")
        print("2. Получение списка всех жанров...")
        print("3. Поиск книг по КАЖДОМУ жанру...")
        print("4. Просмотр ВСЕХ страниц поиска...")
        print("5. Парсинг информации о ВСЕХ найденных книгах...")
        print("6. Фильтрация по году и языку...")
        if parse_chapters:
            print("7. Парсинг первых 3 глав каждой книги...")
            if parse_comments:
                print("   + Сбор комментариев к главам...")
        print("8. Сохранение результатов...")

        print("\n" + "=" * 50)
        print("ПАРСЕР ЗАПУЩЕН БЕЗ ОГРАНИЧЕНИЙ ПО КОЛИЧЕСТВУ КНИГ")
        print("Процесс может занять ОЧЕНЬ много времени (часы или дни)")
        print("=" * 50)

        stories = parser.collect_stories_for_year(
            override_genres=override_genres,
            limit_per_genre=limit_per_genre,
            strict_mode=strict_mode
        )

        # Вывод итогов
        print("\n" + "=" * 70)
        print("СБОР КНИГ ЗАВЕРШЕН!")
        print("=" * 70)

        if stories:
            print(f"\nНАЙДЕНО КНИГ ЗА {year} ГОД: {len(stories)}")

            # Статистика по языкам
            from collections import defaultdict
            lang_stats = defaultdict(int)
            for story in stories:
                lang_stats[story.language] += 1

            for lang, count in sorted(lang_stats.items(), key=lambda x: x[1], reverse=True):
                percentage = (count / len(stories)) * 100 if stories else 0
                print(f"  • {lang}: {count} книг ({percentage:.1f}%)")

            total_time = parser.stats['end_time'] - parser.stats['start_time']
            hours = int(total_time // 3600)
            minutes = int((total_time % 3600) // 60)
            seconds = int(total_time % 60)
            print(f"\nВремя выполнения: {hours:02d}:{minutes:02d}:{seconds:02d}")
            print(f"Ошибок: {parser.stats['errors']}")
            print(f"Страниц просмотрено: {parser.stats['pages_scanned']}")
            print(f"Глав спарсено: {parser.stats['chapters_parsed']}")

            print(f"\nРЕЗУЛЬТАТЫ СОХРАНЕНЫ:")
            print(f"  • В папке: results_{year}/")
            print(f"  • Файлы:")
            print(f"    - wattpad_{year}.json (полные данные, {len(stories)} книг)")
            print(f"    - wattpad_{year}.csv (табличные данные)")
            print(f"    - statistics.txt (статистика)")

            if parse_chapters:
                print(f"    - chapters_{year}.json (данные глав)")
                print(f"    - books_{year}.json (данные книг в формате моделей)")

        else:
            print(f"\nНе найдено книг за {year} год на указанных языках.")
            print("\nВозможные причины:")
            print("1. Нет книг за указанный год на выбранных языках")
            print("2. Проблемы с подключением к Wattpad")
            print("3. Слишком строгая фильтрация")

        print(f"\nЗавершено в {datetime.now().strftime('%H:%M:%S')}")

        # Закрываем менеджер
        db_manager.disconnect()
        print("Сохранение завершено")

    except KeyboardInterrupt:
        print("\n\n[ИНФО] Сбор книг прерван пользователем")
    except Exception as e:
        print(f"\n[ОШИБКА] Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()

    input("\nНажмите Enter для выхода...")


if __name__ == "__main__":
    main()