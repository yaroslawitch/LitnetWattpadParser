import json
from collections import defaultdict
from datetime import datetime
import pandas as pd
import torch


def load_data(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ------------------ ЖАНРЫ ------------------
# Оставляем название метода get_genre_stats, но расширяем функционал:
# возвращаем кортеж (series_count, series_views)
def get_genre_stats(data):
    """
    Возвращает:
      - series_count: pandas.Series(index=genre_name, value=book_count)
      - series_views: pandas.Series(index=genre_name, value=sum_views)
    """
    counts = defaultdict(int)
    views = defaultdict(int)

    for b in data:
        b_views = b.get("views") or 0
        for g in b.get("genres", []):
            # жанр может быть строкой или словарём
            if isinstance(g, dict):
                name = g.get("name") or g.get("title")
            else:
                name = g
            if not name:
                continue
            counts[name] += 1
            try:
                views[name] += int(b_views)
            except Exception:
                views[name] += 0

    series_count = pd.Series(counts).sort_values(ascending=False)
    series_views = pd.Series(views).sort_values(ascending=False)
    return series_count, series_views


# ------------------ ВОВЛЕЧЕННОСТЬ ------------------
def calculate_additional_metrics(data):
    """
    Возвращает DataFrame с колонками:
      - name
      - просмотры (views)
      - лайки (likes)
      - comments_count
      - вовлечённость (вовлечённость аудитории) = (likes + comments)/views
    """
    rows = []
    for b in data:
        name = b.get("name") or b.get("title") or "Unknown"
        views = b.get("views") or 0
        likes = b.get("likes") or 0
        comments_cnt = len(b.get("comments", []))
        engagement = (likes + comments_cnt) / views if views else 0.0
        rows.append({
            "name": name,
            "views": views,
            "likes": likes,
            "comments_count": comments_cnt,
            "engagement": engagement
        })

    df = pd.DataFrame(rows).sort_values("engagement", ascending=False)
    return df


# ------------------ ТОП АВТОРОВ ------------------
def get_top_authors(data, n=10):
    """
    Возвращает два DataFrame:
      - top_by_books: authors with count of books
      - top_by_views: authors with sum of views
    """
    books_count = defaultdict(int)
    views_sum = defaultdict(int)

    for b in data:
        b_views = b.get("views") or 0
        for a in b.get("authors", []):
            # автор может быть строкой или dict
            if isinstance(a, dict):
                author_name = a.get("username") or a.get("name")
            else:
                author_name = a
            if not author_name:
                continue
            books_count[author_name] += 1
            try:
                views_sum[author_name] += int(b_views)
            except Exception:
                views_sum[author_name] += 0

    top_by_books = pd.Series(books_count).sort_values(ascending=False).head(n).reset_index()
    top_by_books.columns = ["author", "books_count"]

    top_by_views = pd.Series(views_sum).sort_values(ascending=False).head(n).reset_index()
    top_by_views.columns = ["author", "views_sum"]

    return top_by_books, top_by_views


# ------------------ ПЕРИОД (минимальная / максимальная дата публикации) ------------------
def get_publication_period(data):
    """
    Возвращает кортеж строк (min_date_str, max_date_str) в формате YYYY-MM-DD.
    Если дат нет — возвращаем (None, None).
    Поля, которые проверяем: publication_start_date, published_at, date
    """
    dates = []
    for b in data:
        cand = b.get("publication_start_date") or b.get("publication_date") or b.get("published_at")
        if cand:
            try:
                # иногда это полный ISO timestamp; берём дату-парсинг гибко
                d = pd.to_datetime(cand, errors="coerce")
                if not pd.isna(d):
                    dates.append(d.date())
            except Exception:
                continue

    if not dates:
        return None, None

    mn = min(dates)
    mx = max(dates)
    return mn.isoformat(), mx.isoformat()


# ------------------ ПРОДУКТИВНЫЕ МЕСЯЦЫ ------------------
def get_productive_months(data, top_n=12):
    """
    Считает количество выпущенных книг по месяцу (YYYY-MM).
    Возвращает pandas.Series(index=YYYY-MM, value=count) отсортированную по убыванию (top_n)
    """
    months = defaultdict(int)
    for b in data:
        cand = b.get("publication_start_date") or b.get("publication_date") or b.get("published_at")
        if not cand:
            continue
        try:
            d = pd.to_datetime(cand, errors="coerce")
            if pd.isna(d):
                continue
            m = d.strftime("%Y-%m")
            months[m] += 1
        except Exception:
            continue

    s = pd.Series(months).sort_values(ascending=False)
    return s.head(top_n)


# ------------------ ТРЕНДЫ ПО ВРЕМЕНИ ------------------
def get_trends_over_time(data, freq="M"):
    """
    Возвращает DataFrame с индексом периода (например, YYYY-MM) и колонками:
      - books_published: количество книг, у которых publication_start_date в периоде
      - views_sum: суммарные просмотры книг, опубликованных в периоде
    freq: 'M' по месяцам, 'W' по неделям, 'D' по дням
    """
    rows = []
    for b in data:
        cand = b.get("publication_start_date") or b.get("publication_date") or b.get("published_at")
        if not cand:
            continue
        try:
            d = pd.to_datetime(cand, errors="coerce")
            if pd.isna(d):
                continue
            rows.append({
                "date": d,
                "views": b.get("views") or 0
            })
        except Exception:
            continue

    if not rows:
        return pd.DataFrame(columns=["books_published", "views_sum"])

    df = pd.DataFrame(rows)
    # агрегируем по частоте
    df = df.set_index("date")
    books_count = df.resample(freq).size().rename("books_published")
    views_sum = df.resample(freq)["views"].sum().rename("views_sum")
    result = pd.concat([books_count, views_sum], axis=1).fillna(0).astype(int)
    # индекс преобразуем в строковый YYYY-MM for charts
    result.index = result.index.to_period("M").astype(str) if freq == "M" else result.index.astype(str)
    return result


# ------------------ НУЖНЫЕ УТИЛИТАРЫ ------------------
def get_book_by_name(data, name):
    for b in data:
        if (b.get("name") or b.get("title")) == name:
            return b
    return None


def get_comment_activity(book):
    """
    Возвращает pandas.Series indexed by date (YYYY-MM-DD) with counts.
    Обрабатывает несколько форматов даты в комментариях.
    """
    dates = []
    for c in book.get("comments", []):
        # try several common keys
        cand = c.get("published_at") or c.get("published") or c.get("date") or c.get("created_at") or c.get("time")
        if not cand:
            continue
        try:
            d = pd.to_datetime(cand, errors="coerce")
            if pd.isna(d):
                # maybe it's already a date string "YYYY-MM-DD"
                continue
            dates.append(d.date())
        except Exception:
            continue

    if not dates:
        return pd.Series(dtype=int)

    s = pd.Series(dates).value_counts().sort_index()
    s.index = pd.to_datetime(s.index).date
    return s


def get_awards(book):
    """
    Возвращает список кортежей (title, date)
    Поддерживает разные поля: rewards, awards, gift_rewards
    """
    awards = book.get("awards") or book.get("rewards") or book.get("gift_rewards") or []
    out = []
    for a in awards:
        if isinstance(a, dict):
            title = a.get("type") or a.get("title") or a.get("name")
            date = a.get("date") or a.get("published_at") or ""
            out.append((title, date))
        else:
            out.append((str(a), ""))
    return out


def get_genre_positions(book):
    """
    Возвращает список (genre_name, position)
    Поддерживает несколько возможных форматов
    """
    positions = book.get("genre_positions") or book.get("positions") or book.get("genres_positions") or []
    out = []
    for p in positions:
        if isinstance(p, dict):
            # possible shapes: {'name': '...', 'position': 5} or {'genre': {'name': '...'}, 'position': 5}
            if "genre" in p and isinstance(p["genre"], dict):
                g_name = p["genre"].get("name")
            else:
                g_name = p.get("name") or p.get("genre") or p.get("title")
            pos = p.get("position") or p.get("place") or p.get("rank")
            out.append((g_name, pos))
        else:
            out.append((str(p), None))
    return out


# ------------------ СЕНТИМЕНТ (на основе transformers) ------------------
def sentiment_analysis_comments(book, model, tokenizer, preprocessor):
    """
    Выполняет инференс модели для всех комментариев книги.
    Возвращает список меток: 'negative'|'neutral'|'positive'
    """
    texts = []
    for c in book.get("comments", []):
        txt = c.get("text") or c.get("content") or c.get("message")
        if txt:
            texts.append(preprocessor(txt))

    if not texts:
        return []

    # batch-tokenize with explicit max_length to avoid warnings
    inputs = tokenizer(
        texts,
        padding=True,
        truncation=True,
        max_length=128,
        return_tensors="pt"
    )

    with torch.no_grad():
        out = model(**inputs)

    preds = torch.argmax(out.logits, dim=1)
    # mapping for the model used: 0->negative,1->neutral,2->positive
    mapping = {0: "negative", 1: "neutral", 2: "positive"}
    return [mapping.get(int(p.item()), "neutral") for p in preds]
