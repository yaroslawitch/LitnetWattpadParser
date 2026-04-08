import sys
import os

BASE = os.path.dirname(os.path.dirname(__file__))
sys.path.append(BASE)

from analytics.analysis import load_data
from analytics.dashboard import create_dashboard


def main():

    path = os.path.join(
        BASE,
        "dev",
        "parser",
        "ParserV2",
        "books.json"
    )

    data = load_data(path)
    app = create_dashboard(data)
    app.run(debug=True)


if __name__ == "__main__":
    main()






