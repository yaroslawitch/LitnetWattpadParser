@echo off
chcp 65001 > nul
cls

echo ========================================
echo    WATTPAD ПАРСЕР КНИГ
echo ========================================
echo.

echo Проверка Python...
python --version
if errorlevel 1 (
    echo Ошибка: Python не установлен или не добавлен в PATH
    echo.
    pause
    exit /b 1
)

echo.
echo Обновление pip...
python -m pip install --upgrade pip

echo.
echo Установка зависимостей...
pip install playwright beautifulsoup4 langdetect python-dateutil requests lxml
if errorlevel 1 (
    echo Ошибка установки зависимостей
    echo.
    pause
    exit /b 1
)

echo.
echo Установка браузера...
playwright install chromium
if errorlevel 1 (
    echo Ошибка установки браузера
    echo.
    pause
    exit /b 1
)

echo.
echo Запуск парсера...
echo ========================================
python main.py

echo.
echo ========================================
echo    Завершено
echo ========================================
pause