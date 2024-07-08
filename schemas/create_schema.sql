CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            hash CHAR(64),
            quantity FLOAT,
            age INT,
            claim_period INT
        )