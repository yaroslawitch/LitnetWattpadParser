import plotly.io as pio
import os

BASE = os.path.dirname(__file__)


def save_summary(figs: dict):
    """
    Сохраняет общий (summary) HTML-отчёт в analytics/summary_dashboard.html
    """
    path = os.path.join(BASE, "summary_dashboard.html")

    blocks = []
    blocks.append("<h1>Общий аналитический отчёт</h1>")

    for title, fig in figs.items():
        blocks.append(f"<h2>{title}</h2>")
        blocks.append(pio.to_html(fig, full_html=False, include_plotlyjs="cdn"))

    html = "<html><head><meta charset='utf-8'><title>Summary</title></head><body>" + "".join(blocks) + "</body></html>"

    with open(path, "w", encoding="utf-8") as f:
        f.write(html)

    print("summary_dashboard.html сохранён в:", path)
