from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
import json
import time

options = Options()
driver = webdriver.Chrome(options=options)
options.page_load_strategy = 'eager'

driver.get("https://www.wattpad.com/")

input('Войдите в аккаунт на wattpad, а потом нажмите ENTER в окне этого терминала.')
cookies_list = driver.get_cookies()
print(cookies_list)
user_agent = driver.execute_script("return navigator.userAgent;")
print(user_agent)
cookies = {}
for cookie in cookies_list:
    if cookie['name'] == 'token' or cookie['name'] == 'X-Time-Zone':
        cookies.update({cookie['name']: cookie['value']})

with open('Wattpad/get_cookies/cookies.json', 'w', encoding='utf8') as f:
    json.dump(cookies, f, indent=1)
with open('Wattpad/get_cookies/headers.json', 'w', encoding='utf8') as f:
    json.dump({'User-Agent': user_agent}, f, indent=1)
print("Окно браузера закроется через:")
for i in range(10, 0, -1):
    time.sleep(1)
    print(i)
driver.quit()