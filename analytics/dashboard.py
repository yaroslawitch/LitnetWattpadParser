import pandas as pd
from collections import Counter
import plotly.express as px
from dash import Dash, dcc, html, Input, Output

from analytics.analysis import (
    get_genre_stats,
    calculate_additional_metrics,
    get_book_by_name,
    get_comment_activity,
    get_awards,
    get_genre_positions,
    sentiment_analysis_comments,
    get_top_authors,
    get_publication_period,
    get_productive_months,
    get_trends_over_time
)

from analytics.export_html import save_summary
from dev.preprocessor.preprocess import preprocess

from transformers import AutoTokenizer, AutoModelForSequenceClassification

# ===== MODEL =====

tokenizer = AutoTokenizer.from_pretrained(
    "blanchefort/rubert-base-cased-sentiment"
)
model = AutoModelForSequenceClassification.from_pretrained(
    "blanchefort/rubert-base-cased-sentiment"
)


def create_dashboard(data):

    app = Dash(__name__)

    df_all = pd.DataFrame(data)

    # ---------- ТОП ПРОСМОТРЫ ----------

    top_views = df_all.sort_values(
        "views", ascending=False
    ).head(10)

    fig_views = px.bar(
        top_views,
        x="name",
        y="views",
        title="Топ-10 по просмотрам"
    )

    # ---------- ТОП ЛАЙКИ ----------

    top_likes = df_all.sort_values(
        "likes", ascending=False
    ).head(10)

    fig_likes = px.bar(
        top_likes,
        x="name",
        y="likes",
        title="Топ-10 по лайкам"
    )

    # ---------- ЖАНРЫ ----------

    genre_count, genre_views = get_genre_stats(data)

    df_gc = pd.DataFrame({
        "genre": genre_count.index,
        "count": genre_count.values
    }).head(15)

    fig_genre_count = px.bar(
        df_gc,
        x="genre",
        y="count",
        title="Популярность жанров (кол-во книг)"
    )

    df_gv = pd.DataFrame({
        "genre": genre_views.index,
        "views": genre_views.values
    }).head(15)

    fig_genre_views = px.bar(
        df_gv,
        x="genre",
        y="views",
        title="Популярность жанров (по просмотрам)"
    )

    # ---------- ВОВЛЕЧЕННОСТЬ ----------

    df_eng = calculate_additional_metrics(data)

    fig_eng = px.bar(
        df_eng.head(10),
        x="name",
        y="engagement",
        title="Вовлечённость аудитории"
    )

    # ---------- ТОП АВТОРОВ ----------

    top_auth_books, top_auth_views = get_top_authors(data)

    fig_auth_books = px.bar(
        top_auth_books,
        x="author",
        y="books_count",
        title="Топ авторов по количеству книг"
    )

    fig_auth_views = px.bar(
        top_auth_views,
        x="author",
        y="views_sum",
        title="Топ авторов по просмотрам"
    )

    # ---------- ПЕРИОД ----------

    min_date, max_date = get_publication_period(data)
    period_text = f"{min_date} — {max_date}"

    # ---------- ПРОДУКТИВНЫЕ МЕСЯЦЫ ----------

    prod = get_productive_months(data)

    df_prod = pd.DataFrame({
        "month": prod.index,
        "books": prod.values
    })

    fig_prod = px.bar(
        df_prod,
        x="month",
        y="books",
        title="Самые продуктивные месяцы"
    )

    # ---------- ТРЕНДЫ ----------

    trends = get_trends_over_time(data)

    df_tr = trends.reset_index()

    fig_tr_books = px.line(
        df_tr,
        x="date",
        y="books_published",
        title="Тренд: выпуск книг"
    )

    fig_tr_views = px.line(
        df_tr,
        x="date",
        y="views_sum",
        title="Тренд: просмотры"
    )

    # ---------- SAVE SUMMARY ----------

    save_summary({
        "Топ просмотры": fig_views,
        "Топ лайки": fig_likes,
        "Жанры (книги)": fig_genre_count,
        "Жанры (просмотры)": fig_genre_views,
        "Вовлечённость": fig_eng,
        "Топ авторов (книги)": fig_auth_books,
        "Топ авторов (просмотры)": fig_auth_views,
        "Продуктивные месяцы": fig_prod,
        "Тренд книги": fig_tr_books,
        "Тренд просмотры": fig_tr_views
    })

    # ---------- UI ----------

    badge_style = {
        "position": "absolute",
        "right": "20px",
        "top": "20px",
        "backgroundColor": "#fff3cd",
        "padding": "12px 18px",
        "borderRadius": "8px",
        "fontWeight": "600",
        "zIndex": "1000"
    }

    app.layout = html.Div([

        html.Div([
            html.Div("Период данных"),
            html.Div(period_text)
        ], style=badge_style),

        html.H1("Litnet Analytics"),

        dcc.Dropdown(
            id="book",
            options=[
                {"label": b["name"], "value": b["name"]}
                for b in data
            ],
            placeholder="Выберите книгу"
        ),

        dcc.Graph(id="sentiment"),
        dcc.Graph(id="activity"),

        html.H3("Награды"),
        html.Ul(id="awards"),

        html.H3("Позиции в жанрах"),
        html.Ul(id="positions")
    ])

    # ---------- CALLBACK ----------

    @app.callback(
        [
            Output("sentiment", "figure"),
            Output("activity", "figure"),
            Output("awards", "children"),
            Output("positions", "children")
        ],
        Input("book", "value")
    )
    def update_book(name):

        if not name:
            return {}, {}, [], []

        b = get_book_by_name(data, name)

        # sentiment
        sents = sentiment_analysis_comments(
            b, model, tokenizer, preprocess
        )
        cnt = Counter(sents)

        fig_sent = px.pie(
            names=list(cnt.keys()),
            values=list(cnt.values()),
            title="Тональность комментариев"
        )

        # activity
        act = get_comment_activity(b)

        if act.empty:
            fig_act = px.line(title="Нет комментариев")
        else:
            df_act = pd.DataFrame({
                "date": act.index.astype(str),
                "count": act.values
            })

            fig_act = px.line(
                df_act,
                x="date",
                y="count",
                title="Динамика комментариев"
            )

        awards = [
            html.Li(f"{t} {d}")
            for t, d in get_awards(b)
        ]

        positions = [
            html.Li(f"{g}: {p}")
            for g, p in get_genre_positions(b)
        ]

        return fig_sent, fig_act, awards, positions

    return app
