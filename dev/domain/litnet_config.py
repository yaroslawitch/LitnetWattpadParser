import subprocess
import os
from pathlib import Path


class LitnetConfig:
    MODES = ['simple', 'offset', 'jobdir']
    FORMATS = ['json', 'jsonlines', 'jl', 'csv', 'xml']

    DEFAULT_OUTPUT = 'books.json'
    DEFAULT_OFFSET_FILE = 'last_offset.txt'
    DEFAULT_JOBDIR = 'crawls/litnet_session'

    def __init__(self):
        self.scrapy_dir = self._find_scrapy_dir()

    def _find_scrapy_dir(self) -> Path:
        """
        Ищем папку где лежит scrapy.cfg
        """
        base = Path(__file__).resolve().parents[1]  # dev/

        candidate = base / 'parser' / 'ParserV2'

        if not (candidate / 'scrapy.cfg').exists():
            raise FileNotFoundError(
                f"scrapy.cfg не найден в {candidate}"
            )

        return candidate

    def prepare_env(self):
        """
        Переходим в папку scrapy
        """
        os.chdir(self.scrapy_dir)

    def check_scrapy_cfg(self) -> bool:
        return os.path.exists('scrapy.cfg')

    def build_output_filename(self, output: str, fmt: str) -> str:
        if fmt != 'json' and '.' not in output:
            if fmt in ['jsonlines', 'jl']:
                return output.rsplit('.', 1)[0] + '.jl'
            return output.rsplit('.', 1)[0] + f'.{fmt}'
        return output

    # ---------------- INTERNAL ---------------- #

    def _run(self, cmd):
        env = os.environ.copy()

        # dev/
        dev_path = Path(__file__).resolve().parents[1]
        env['PYTHONPATH'] = str(dev_path)

        print("WORKDIR =", os.getcwd())
        print("PYTHONPATH =", env['PYTHONPATH'])
        print("CMD:", " ".join(cmd))

        return subprocess.run(cmd, env=env)

    # ---------------- MODES ---------------- #

    def run_simple(self, output_file: str):
        cmd = [
            'scrapy', 'crawl', 'LitnetBooksParser',
            '-o', output_file,
            '-a', 'mode=simple'
        ]
        return self._run(cmd)

    def run_offset(self, output_file: str, offset_file: str):
        cmd = [
            'scrapy', 'crawl', 'LitnetBooksParser',
            '-o', output_file,
            '-a', 'mode=offset_file',
            '-a', f'offset_file_path={offset_file}'
        ]
        return self._run(cmd)

    def run_jobdir(self, output_file: str, jobdir: str):
        os.makedirs(jobdir, exist_ok=True)

        cmd = [
            'scrapy', 'crawl', 'LitnetBooksParser',
            '-o', output_file,
            '-s', f'JOBDIR={jobdir}',
            '-a', 'mode=jobdir'
        ]
        return self._run(cmd)
