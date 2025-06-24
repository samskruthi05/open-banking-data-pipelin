import logging
from pipeline.extract import read_json_files
from pipeline.transform import transform_data
from pipeline.load import upsert_clean_records, log_error_records

logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

if __name__ == "__main__":
    logging.info("Pipeline started")

    files = [
        "data/tech_assessment_transactions_01.json",
        "data/tech_assessment_transactions_02.json"
    ]
    raw = read_json_files(files)
    clean, errors = transform_data(raw)
    upsert_clean_records(clean)
    log_error_records(errors)

    logging.info("Pipeline finished successfully")