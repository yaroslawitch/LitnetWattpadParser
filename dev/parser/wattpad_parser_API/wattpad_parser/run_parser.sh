#!/bin/bash

echo "========================================"
echo "    WATTPAD ПАРСЕР КНИГ"
echo "========================================"
echo ""

echo "Проверка Python..."
python3 --version
if [ $? -ne 0 ]; then
    echo "Ошибка: Python не установлен"
    echo ""
    read -p "Нажмите Enter для выхода..."
    exit 1
fi

echo ""
echo "Обновление pip..."
python3 -m pip install --upgrade pip

echo ""
echo "Установка зависимостей..."
pip3 install playwright beautifulsoup4 langdetect python-dateutil requests lxml
if [ $? -ne 0 ]; then
    echo "Ошибка установки зависимостей"
    echo ""
    read -p "Нажмите Enter для выхода..."
    exit 1
fi

echo ""
echo "Установка браузера..."
playwright install chromium
if [ $? -ne 0 ]; then
    echo "Ошибка установки браузера"
    echo ""
    read -p "Нажмите Enter для выхода..."
    exit 1
fi

echo ""
echo "Запуск парсера..."
echo "========================================"
python3 main.py

echo ""
echo "========================================"
echo "    Завершено"
echo "========================================"
read -p "Нажмите Enter для выхода..."