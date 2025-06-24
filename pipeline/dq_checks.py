from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

ALLOWED_CURRENCIES = {"USD", "EUR", "GBP"}

# Runing all DQ checks and return a list of failed rules
def validate_currency(record):
    return record.get("currency") in ALLOWED_CURRENCIES

def validate_transaction_date(record):
    try: 
        datetime.fromisoformat(record["transactionDate"])
        return True
    except ValueError:
        return False

def validate_amount(record):
    try:
        return float(record["amount"])!=0
    except:
        return False    
# applies all dq checks and returning a list of a failed rules for a given record
def generate_dq_errors(record):

    errors = []
    if not validate_currency(record):
        errors.append("Invalid currency")
    if not validate_transaction_date(record):
        errors.append("Invalid transaction date")
    if not validate_amount(record):
        errors.append("Invalid amount")
    if errors:
        logging.info(f"record {record.get('transactionId')} failed checks- {errors}")

    return errors

if __name__ == "__main__":
    sample_record ={
        "customerId": "abc-123",
        "transactionId": "txn-001",
        "currency" :"INR",
        "transactionDate": "2023-02-30", 
          "amount": "0" 

    }
#errors = generate_dq_errors(sample_record)
#print('DQ erros', errors)
