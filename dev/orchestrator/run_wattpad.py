import sys
import os
sys.path.append('..')
from uploader.wp_uploader import WattpadUploader
import json
from domain.wp_config import PARSER, PARSE
import subprocess


def read_jsonl(file=f'./crawled_data/{PARSER}.jsonl'):

    with open(file, 'r', encoding='utf8') as f:
        books = f.readlines()
    
    for i in range(len(books)):
        books[i] = json.loads(books[i])
    
    return books
    
def start_parsing():
    try:
        cmd = 'py run_wp.py'
        return subprocess.run(cmd)
    except:
        cmd = 'python3 run_wp.py'
        return subprocess.run(cmd)
        
if __name__ == '__main__':
    uploader = WattpadUploader()
    os.chdir('../parser/Wattpad')
    if PARSE:
        start_parsing()
    if __name__ == '__main__':
        books = read_jsonl()
        if __name__ == '__main__':
            uploader.load(books)