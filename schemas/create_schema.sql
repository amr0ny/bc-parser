CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            hash CHAR(64),
            quantity FLOAT,
            near_amount INTEGER,
            hot_amount INTEGER,
            age INT,
            claim_period INTEGER
        )