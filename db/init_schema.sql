CREATE TABLE customers
(
                customer_id TEXT PRIMARY KEY
                , most_recent_transaction_date DATE NOT NULL
                , created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    
)

CREATE TABLE transactions
(
                customer_id TEXT
                ,transaction_id TEXT
                ,transaction_date DATE
                ,source_date TIMESTAMP
                ,merchant_id INTEGER
                ,category_id INTEGER
                ,amount NUMERIC
                ,description TEXT
                ,currency TEXT
                ,ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ,PRIMARY KEY (customer_id, transaction_id)
                ,FOREIGN KEY (customer_id)
)

CREATE TABLE error_log (
                id SERIAL PRIMARY KEY
                ,customer_id TEXT
                ,transaction_id TEXT
                ,error_reason TEXT
                ,raw_data JSONB
                ,logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );