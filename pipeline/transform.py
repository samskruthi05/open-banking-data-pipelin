
from pipeline.dq_checks import generate_dq_errors
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def transform_data(records):
    clean_records = []
    error_records = []
    # To track duplicates
    seen_keys = set()  

    for record in records:
        errors = generate_dq_errors(record)
        key = (record["customerId"], record["transactionId"])
        #print(f'this is key: {key}')

        # Check for duplicate
        if key in seen_keys:
            errors.append("Duplicate transaction")
        else:
            seen_keys.add(key)

        # If any errors, log to error_records
        if errors:
            logging.info(f"Record {record.get('transactionId')} failed: {errors}")
            error_records.append({
                "customer_id": record.get("customerId"),
                "transaction_id": record.get("transactionId"),
                "error_reason": "; ".join(errors),
                "raw_data": record
            })
        else:
            # Remove PII
            clean_record = {
                "customer_id": record["customerId"],
                "transaction_id": record["transactionId"],
                "transaction_date": record["transactionDate"],
                "source_date": record["sourceDate"],
                "merchant_id": record["merchantId"],
                "category_id": record["categoryId"],
                "amount": record["amount"],
                "currency": record["currency"],
                "description": record["description"]
            }
            clean_records.append(clean_record)
            
    logging.info(f"total clean records: {len(clean_records)}")
    logging.info(f"total error records: {len(error_records)}")
    return clean_records, error_records
    

