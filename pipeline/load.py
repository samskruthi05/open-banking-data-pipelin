import psycopg2
import logging
from psycopg2.extras import Json
from dotenv import load_dotenv
import os

"""
Module for loading data into the database as part of the ETL pipeline.
"""
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
load_dotenv()
def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT")
    )

def upsert_clean_records(clean_records):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        for record in clean_records:
            try:
                # Insert or update customers table
                cur.execute("""
                    INSERT INTO customers (customer_id, most_recent_transaction_date)
                    VALUES (%s, %s)
                    ON CONFLICT (customer_id)
                    DO UPDATE SET most_recent_transaction_date = 
                        GREATEST(customers.most_recent_transaction_date, EXCLUDED.most_recent_transaction_date)
                """, (
                    record["customer_id"],
                    record["transaction_date"]
                ))

                # Insert or update transactions table
                cur.execute("""
                    INSERT INTO transactions (
                        customer_id, transaction_id, transaction_date, source_date,
                        merchant_id, category_id, amount, description, currency
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (customer_id, transaction_id)
                    DO UPDATE SET 
                        source_date = EXCLUDED.source_date,
                        amount = EXCLUDED.amount,
                        description = EXCLUDED.description,
                        currency = EXCLUDED.currency,
                        transaction_date = EXCLUDED.transaction_date,
                        merchant_id = EXCLUDED.merchant_id,
                        category_id = EXCLUDED.category_id
                    WHERE transactions.source_date < EXCLUDED.source_date
                """, (
                    record["customer_id"],
                    record["transaction_id"],
                    record["transaction_date"],
                    record["source_date"],
                    record["merchant_id"],
                    record["category_id"],
                    record["amount"],
                    record["description"],
                    record["currency"]
                ))
            except Exception as e:
                logging.error(f"failed to upsert transaction {record['transaction_id']}: {e}")

        conn.commit()
        logging.info("successfully committed all clean records")
        cur.close()

    except Exception as e:
        logging.critical(f"database connection or execution failed: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("database connection closed.")

def log_error_records(error_records):
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        for record in error_records:
            try:
                cur.execute("""
                    INSERT INTO error_log (customer_id, transaction_id, error_reason, raw_data)
                    VALUES (%s, %s, %s, %s)
                """, (
                    record.get("customer_id"),
                    record.get("transaction_id"),
                    record["error_reason"],
                    Json(record["raw_data"])
                ))
            except Exception as e:
                logging.error(f"failed to log error for transaction {record.get('transaction_id')}: {e}")

        conn.commit()
        logging.info("error records logged successfully")
        cur.close()

    except Exception as e:
        logging.critical(f"database connection failed {e}")
    finally:
        if conn:
            conn.close()
            logging.info("database connection closed")
