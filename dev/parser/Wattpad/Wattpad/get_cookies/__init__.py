import json

def load_cookies_headers(c_f='Wattpad/get_cookies/cookies.json', h_f='Wattpad/get_cookies/headers.json'):
    '''получает заголовки и куки'''
    with open(c_f, 'r', encoding='utf8') as f:
        cookies = json.loads(f.read())
    with open(h_f, 'r', encoding='utf8') as f:
        headers = json.loads(f.read())
    return (cookies, headers)