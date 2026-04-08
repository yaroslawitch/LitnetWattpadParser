import re
import nltk
from nltk.corpus import stopwords

try:
    stop_words = set(stopwords.words('russian'))
except:
    nltk.download('stopwords')
    stop_words = set(stopwords.words('russian'))


def preprocess(text: str) -> str:
    """
    Безопасная предобработка (совместима с Python 3.11)
    """
    if not isinstance(text, str):
        return ""

    # убираем html
    text = re.sub(r'<.*?>', ' ', text)

    # убираем ссылки
    text = re.sub(r'http\S+|www\S+', ' ', text)

    # оставляем только буквы
    text = re.sub(r'[^а-яА-ЯёЁ\s]', ' ', text)

    text = text.lower()

    tokens = text.split()

    tokens = [
        w for w in tokens
        if w not in stop_words and len(w) > 2
    ]

    return " ".join(tokens)
