"""
Litnet CLI + оркестратор (simple + jobdir)
Запуск из \dev\
python orchestrator/run_litnet.py simple                                           для режима simple
python orchestrator/run_litnet.py jobdir --jobdir crawls/litnet_session            для режима jobdir
"""

import sys
import argparse
import subprocess
import signal
import os
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEV = ROOT / "dev"

sys.path.append(str(ROOT))
sys.path.append(str(DEV))

from domain.litnet_config import LitnetConfig
from domain.config import Config
from domain.models import Book, User, Genre, Tag, Reward, Comment
from uploader.uploaders import LitnetUploader


# ---------------------------------------------------------
# dict → Book (включая вложенные сущности)
# ---------------------------------------------------------
def dict_to_book(d: dict) -> Book:
    comments_raw = d.get("comments", [])

    return Book(
        name=d.get("name"),
        link=d.get("link"),
        rating=d.get("rating"),
        likes=d.get("likes"),
        views=d.get("views"),
        cycle=d.get("cycle"),
        times_saved_to_library=d.get("times_saved_to_library"),
        publication_start_date=d.get("publication_start_date"),
        publication_end_date=d.get("publication_end_date"),
        price=d.get("price"),
        contains_profanity=d.get("contains_profanity"),
        is_finished=d.get("is_finished"),
        age_restriction=d.get("age_restriction"),
        description=d.get("description"),

        num_comments=len(comments_raw),

        authors=[User(**u) for u in d.get("authors", [])],
        genres=[Genre(**g) for g in d.get("genres", [])],
        tags=[Tag(**t) for t in d.get("tags", [])],
        rewards=[Reward(**r) for r in d.get("rewards", [])],

        comments=[
            Comment(
                text=c.get("text"),
                published_at=c.get("published_at"),
                user=User(**c.get("user"))
            )
            for c in comments_raw
        ]
    )


# ---------------------------------------------------------
# Чтение JSON Lines → Book
# ---------------------------------------------------------
def load_books_from_json(path: str) -> list[Book]:
    books = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                books.append(dict_to_book(data))
            except json.JSONDecodeError:
                continue
    return books

def load_new_books(path: str, offset_path: str) -> list[Book]:
    # читаем старый offset
    if os.path.exists(offset_path):
        with open(offset_path, "r") as f:
            old_offset = int(f.read().strip() or 0)
    else:
        old_offset = 0

    books = []
    new_offset = 0

    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            new_offset = i + 1
            if i < old_offset:
                continue  # пропускаем старые строки
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                books.append(dict_to_book(data))
            except json.JSONDecodeError:
                continue

    # сохраняем новый offset
    with open(offset_path, "w") as f:
        f.write(str(new_offset))

    return books


class LitnetCLI:

    def __init__(self):
        self.config = LitnetConfig()
        self.parser = self._build_parser()

    def _build_parser(self):
        parser = argparse.ArgumentParser(description='Litnet Scrapy CLI')

        parser.add_argument(
            'mode',
            choices=LitnetConfig.MODES,
            help="Режим запуска парсера"
        )

        parser.add_argument(
            '--output', '-o',
            default=LitnetConfig.DEFAULT_OUTPUT,
            help="Имя выходного файла (json/jl)"
        )

        parser.add_argument(
            '--offset-file',
            default=LitnetConfig.DEFAULT_OFFSET_FILE
        )

        parser.add_argument(
            '--jobdir',
            default=LitnetConfig.DEFAULT_JOBDIR
        )

        parser.add_argument(
            '--format', '-f',
            default='json',
            choices=LitnetConfig.FORMATS
        )

        return parser

    # ---------------------------------------------------------
    # SIMPLE MODE: Scrapy → JSON → Upload
    # ---------------------------------------------------------
    def _run_simple_with_upload(self, output_file: str):
        print("\n=== SIMPLE MODE: PARSE → UPLOAD ===")

        cmd = [
            'scrapy', 'crawl', 'LitnetBooksParser',
            '-o', output_file,
            '-a', 'mode=simple'
        ]

        print("[CMD]", " ".join(cmd))

        env = dict(os.environ)
        env["PYTHONPATH"] = f"{ROOT};{DEV}"

        proc = subprocess.Popen(cmd, env=env)

        try:
            returncode = proc.wait()
        except KeyboardInterrupt:
            print("[WARN] Ctrl+C → ждём корректного завершения Scrapy...")
            try:
                proc.send_signal(signal.CTRL_C_EVENT)
            except Exception:
                proc.terminate()
            try:
                returncode = proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print("[WARN] Принудительное завершение Scrapy.")
                proc.terminate()
                returncode = 130

        if returncode not in (0, 130):
            print(f"[ERROR] Парсер завершился с ошибкой ({returncode}), загрузка отменена.")
            return returncode

        print(f"[UPLOAD] Загружаем данные из {output_file}")

        books = load_books_from_json(output_file)

        db_config = Config(
            dbname="reviewsdb",
            dbuser="reviews_user",
            dbpassword="super_secret_password",
            dbhost="localhost",
            dbport=5002
        )

        uploader = LitnetUploader(db_config)
        uploader.load(books, reuse_run=False)

        print("=== SIMPLE MODE FINISHED ===\n")
        return 0

    # ---------------------------------------------------------
    # JOBDIR MODE: Scrapy → JSON → Upload (CONTINUATION)
    # ---------------------------------------------------------
    def _run_jobdir_with_upload(self, output_file: str, jobdir: str):
        print("\n=== JOBDIR MODE: PARSE → UPLOAD (CONTINUATION) ===")

        cmd = [
            'scrapy', 'crawl', 'LitnetBooksParser',
            '-o', output_file,
            '-s', f'JOBDIR={jobdir}',
            '-a', 'mode=jobdir'
        ]

        print("[CMD]", " ".join(cmd))

        env = dict(os.environ)
        env["PYTHONPATH"] = f"{ROOT};{DEV}"

        proc = subprocess.Popen(cmd, env=env)

        try:
            returncode = proc.wait()
        except KeyboardInterrupt:
            print("[WARN] Ctrl+C → ждём корректного завершения Scrapy...")
            try:
                proc.send_signal(signal.CTRL_C_EVENT)
            except Exception:
                proc.terminate()
            try:
                returncode = proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print("[WARN] Принудительное завершение Scrapy.")
                proc.terminate()
                returncode = 130

        if returncode not in (0, 130):
            print(f"[ERROR] Парсер завершился с ошибкой ({returncode}), загрузка отменена.")
            return returncode

        print(f"[UPLOAD] Загружаем данные из {output_file}")

        offset_file = "jobdir_offset.txt"
        books = load_new_books(output_file, offset_file)


        db_config = Config(
            dbname="reviewsdb",
            dbuser="reviews_user",
            dbpassword="super_secret_password",
            dbhost="localhost",
            dbport=5002
        )

        uploader = LitnetUploader(db_config)
        uploader.load(books, reuse_run=True)

        print("=== JOBDIR MODE FINISHED ===\n")
        return 0

    # ---------------------------------------------------------
    # MAIN RUN
    # ---------------------------------------------------------
    def run(self):
        args = self.parser.parse_args()

        self.config.prepare_env()

        if not self.config.check_scrapy_cfg():
            print("scrapy.cfg не найден")
            return 1

        output_file = self.config.build_output_filename(
            args.output,
            args.format
        )

        print("\n" + "=" * 50)
        print(f"MODE: {args.mode}")
        print(f"OUTPUT FILE: {output_file}")
        print("=" * 50)

        if args.mode == 'simple':
            return self._run_simple_with_upload(output_file)

        elif args.mode == 'offset':
            res = self.config.run_offset(output_file, args.offset_file)
            return res.returncode

        elif args.mode == 'jobdir':
            return self._run_jobdir_with_upload(output_file, args.jobdir)


if __name__ == '__main__':
    sys.exit(LitnetCLI().run())
