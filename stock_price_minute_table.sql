CREATE TABLE IF NOT EXISTS stock_price_minute (
    id INTEGER PRIMARY KEY, 
    stock_id INTEGER,
    datetime NOT NULL,
    open NOT NULL, 
    high NOT NULL,
    low NOT NULL, 
    close NOT NULL, 
    volume NOT NULL,
    FOREIGN KEY (stock_id) REFERENCES stock (id)
)
