CREATE TABLE parser_run (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE "user" (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(127) NOT NULL,
    link VARCHAR(255) UNIQUE,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE genre (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(127) NOT NULL UNIQUE,
    link VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE tag (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(127) NOT NULL UNIQUE,
    link VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE book (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    name VARCHAR(127) NOT NULL,
    link VARCHAR(255) NOT NULL,

    rating INTEGER NOT NULL,
    likes INTEGER NOT NULL,
    views INTEGER NOT NULL,

    cycle VARCHAR(127),

    times_saved_to_library INTEGER,

    publication_start_date DATE,
    publication_end_date DATE,

    price INTEGER,

    contains_profanity BOOLEAN,
    is_finished BOOLEAN,

    age_restriction VARCHAR(32),

    description TEXT,

    run_id UUID NOT NULL REFERENCES parser_run(id) ON DELETE CASCADE
);

CREATE TABLE chapter (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(127) NOT NULL,
    publication_date DATE NOT NULL,

    run_id UUID NOT NULL REFERENCES parser_run(id) ON DELETE CASCADE,
    book_id UUID NOT NULL REFERENCES book(id) ON DELETE CASCADE
);

CREATE TABLE comment (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    text TEXT NOT NULL,
    published_at TIMESTAMPTZ NOT NULL,

    run_id UUID NOT NULL REFERENCES parser_run(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES "user"(id),
    book_id UUID NOT NULL REFERENCES book(id) ON DELETE CASCADE
);

CREATE TABLE reward (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type VARCHAR(127) NOT NULL,
    amount INTEGER NOT NULL DEFAULT 1,

    run_id UUID NOT NULL REFERENCES parser_run(id) ON DELETE CASCADE,
    book_id UUID NOT NULL REFERENCES book(id) ON DELETE CASCADE
);

CREATE TABLE books_users (
    book_id UUID NOT NULL REFERENCES book(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES "user"(id),
    run_id UUID NOT NULL REFERENCES parser_run(id) ON DELETE CASCADE,

    PRIMARY KEY (book_id, user_id, run_id)
);

CREATE TABLE books_genres (
    book_id UUID NOT NULL REFERENCES book(id) ON DELETE CASCADE,
    genre_id UUID NOT NULL REFERENCES genre(id),
    run_id UUID NOT NULL REFERENCES parser_run(id) ON DELETE CASCADE,

    top_position INT NOT NULL,

    PRIMARY KEY (book_id, genre_id, run_id)
);

CREATE TABLE books_tags (
    book_id UUID NOT NULL REFERENCES book(id) ON DELETE CASCADE,
    tag_id UUID NOT NULL REFERENCES tag(id),
    run_id UUID NOT NULL REFERENCES parser_run(id) ON DELETE CASCADE,

    PRIMARY KEY (book_id, tag_id, run_id)
);

CREATE INDEX idx_book_run_id ON book(run_id);
CREATE INDEX idx_comment_run_id ON comment(run_id);
CREATE INDEX idx_chapter_run_id ON chapter(run_id);

CREATE INDEX idx_books_genres_run_id ON books_genres(run_id);
CREATE INDEX idx_books_tags_run_id ON books_tags(run_id);
CREATE INDEX idx_books_users_run_id ON books_users(run_id);