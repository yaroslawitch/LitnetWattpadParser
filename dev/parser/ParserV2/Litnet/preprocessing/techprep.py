# Litnet/preprocessing/techprep.py

import re
from datetime import datetime, timedelta
from dataclasses import is_dataclass, fields

# ---------------- text cleaning ----------------

_ARTIFACTS_RE = re.compile(r"[\x00-\x1f\xa0]+")


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None

    value = _ARTIFACTS_RE.sub(" ", value)
    value = re.sub(r"\s{2,}", " ", value)

    return value.strip()


# ---------------- date normalization ----------------

_DATE_FORMATS = [
    # Основные форматы с Litnet
    "%d.%m.%Y",  # 01.01.2023
    "%d.%m.%Y %H:%M",  # 01.01.2023 12:30
    "%d.%m.%Y %H:%M:%S",  # 01.01.2023 12:30:45
    "%d.%m.%Y, %H:%M",  # 01.01.2023, 12:30
    "%d.%m.%Y, %H:%M:%S",  # 01.01.2023, 12:30:45 - ВАЖНО!

    # ISO форматы
    "%Y-%m-%d",  # 2023-01-01
    "%Y-%m-%d %H:%M:%S",  # 2023-01-01 12:30:45
    "%Y-%m-%dT%H:%M:%S",  # 2023-01-01T12:30:45
    "%Y-%m-%dT%H:%M:%SZ",  # 2023-01-01T12:30:45Z
    "%Y-%m-%dT%H:%M:%S.%fZ",  # 2023-01-01T12:30:45.123Z

    # Форматы без ведущих нулей
    "%-d.%-m.%Y",  # 1.1.2023
    "%-d.%-m.%Y %H:%M",  # 1.1.2023 12:30
    "%-d.%-m.%Y, %H:%M",  # 1.1.2023, 12:30
    "%-d.%-m.%Y, %H:%M:%S",  # 1.1.2023, 12:30:45
]


def _try_parse_date(value: str) -> datetime | None:
    """Пытаемся распарсить дату разными форматами"""
    if not value:
        return None

    # Чистим текст перед парсингом
    value = clean_text(value) or ""

    # Пробуем каждый формат
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    # Пробуем распознать относительное время
    today = datetime.now()

    # "сегодня в HH:MM"
    match = re.match(r"сегодня в (\d{1,2}):(\d{2})", value.lower())
    if match:
        hour, minute = int(match.group(1)), int(match.group(2))
        return today.replace(hour=hour, minute=minute, second=0, microsecond=0)

    # "сегодня в HH:MM:SS"
    match = re.match(r"сегодня в (\d{1,2}):(\d{2}):(\d{2})", value.lower())
    if match:
        hour, minute, second = int(match.group(1)), int(match.group(2)), int(match.group(3))
        return today.replace(hour=hour, minute=minute, second=second, microsecond=0)

    # "вчера в HH:MM"
    match = re.match(r"вчера в (\d{1,2}):(\d{2})", value.lower())
    if match:
        hour, minute = int(match.group(1)), int(match.group(2))
        yesterday = today - timedelta(days=1)
        return yesterday.replace(hour=hour, minute=minute, second=0, microsecond=0)

    # "вчера в HH:MM:SS"
    match = re.match(r"вчера в (\d{1,2}):(\d{2}):(\d{2})", value.lower())
    if match:
        hour, minute, second = int(match.group(1)), int(match.group(2)), int(match.group(3))
        yesterday = today - timedelta(days=1)
        return yesterday.replace(hour=hour, minute=minute, second=second, microsecond=0)

    # "X дней/часов/минут назад"
    match = re.match(r"(\d+)\s+(день|дня|дней|час|часа|часов|минут|минуты|минуту)\s+назад", value.lower())
    if match:
        amount = int(match.group(1))
        unit = match.group(2)

        if "день" in unit or "дня" in unit or "дней" in unit:
            delta = timedelta(days=amount)
        elif "час" in unit:
            delta = timedelta(hours=amount)
        elif "минут" in unit:
            delta = timedelta(minutes=amount)
        else:
            return None

        return today - delta

    return None


def normalize_date(value: str | None) -> str | None:
    if not value:
        return None

    # Пытаемся распарсить
    dt = _try_parse_date(value)

    if dt:
        # Форматируем в нужный формат
        if dt.hour == 0 and dt.minute == 0 and dt.second == 0:
            return dt.date().isoformat()  # гггг-мм-дд
        else:
            # Если есть время, добавляем Z (UTC)
            return dt.isoformat() + "Z"

    # Если не удалось распарсить, возвращаем очищенный оригинал
    return clean_text(value)


# ---------------- recursive preprocessing ----------------

def preprocess(obj):
    if obj is None:
        return None

    # строки (НЕ даты!)
    if isinstance(obj, str):
        return clean_text(obj)

    # списки → tuple
    if isinstance(obj, list):
        return tuple(preprocess(x) for x in obj)

    if isinstance(obj, tuple):
        return tuple(preprocess(x) for x in obj)

    # dataclass — КЛЮЧЕВОЕ МЕСТО
    if is_dataclass(obj):
        for f in fields(obj):
            value = getattr(obj, f.name)

            # Обрабатываем все поля, которые могут содержать даты
            # Проверяем по названию поля или типу значения
            field_name_lower = f.name.lower()
            if isinstance(value, str) and any(keyword in field_name_lower
                                              for keyword in ['date', 'published', 'at']):
                value = normalize_date(value)
            else:
                value = preprocess(value)

            setattr(obj, f.name, value)

        return obj

    return obj

