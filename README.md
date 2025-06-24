#  Open Banking etl pipeline

This project implements a Python-based ETL pipeline to process Open Banking transaction data, as outlined in the Snoop technical assessment. The solution covers extraction, validation, transformation, and loading into a PostgreSQL data store, with a focus on data quality and traceability.

## What the pipeline does

- Extracts JSON data from local files
- Performs data quality checks:
  - Currency validation
  - Invalid transaction date detection
  - Duplicate record detection based on (customerId, transactionId)
- Transforms the data by removing PII (e.g., customer name)
- Loads valid records into a PostgreSQL data mart:
  - `customers` table with latest transaction date per customer
  - `transactions` table with unique transaction records
- Logs records that fail validation into an `error_log` table with detailed reasons

## Notebooks

- `pipeline/extract_notebook.ipynb`: Personal scratchpad used for experimenting with JSON structure and extraction logic during development. Kept for transparency to showcase exploration steps.

## environment variables
This project uses a .env file to manage sensitive settings like database credentials

## Project Structure

```
snoop-task/
├── pipeline/
│   ├── extract.py            # Reads JSON files
│   ├── transform.py          # Cleans data & applies DQ rules
│   ├── load.py               # Loads data into PostgreSQL
│   └── dq_checks.py          # Validation logic
├── tests/
│   ├── test_dq_checks.py     # Unit tests for DQ checks
│   └── test_mock_db.py       # Tests DB logging using mocks
├── data/                     # Contains input JSON files
├── main.py                   # Pipeline entry point
├── requirements.txt          # Python dependencies
├── .github/workflows/ci.yml  # GitHub Actions CI workflow
|___ .env                     # Store secrets
|___ db/                       # contains the db schema
```

## How to run

```bash
1. Clone the repository and set up a virtual environment:
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
2.	Set up a PostgreSQL database named snoop and run schema.sql to create required tables.
3.	Place the input JSON files in a folder named data/.
4.	Run the pipeline:
    python main.py
4.  Run tests:
    pytest -v
    This will run:
	•	Unit tests for data quality logic (test_dq_checks.py)
	•	Database logging tests using mock database objects (test_mock_db.py)

 ## Key design decisions
	•	ETL approach was chosen over ELT for early validation and cleaner loading
	•	Used psycopg2 for PostgreSQL operations
	•	Applied upsert logic to both customers and transactions using ON CONFLICT
	•	Ensured idempotency by checking (customerId, transactionId)
	•	Built modular, testable components with clear separation of responsibilities
	•	Used logging instead of print statements for production-style observability

## Security considerations

While this is a local demo pipeline, in a real-world production scenario we would also:
	•	Store DB credentials in environment variables or a secret manager, not in source code
	•	Encrypt sensitive data in transit and at rest
	•	Validate and sanitize inputs from external sources to prevent SQL injection
	•	Audit and monitor logs for anomalies
	•	Apply access controls on the database level

## CI Integration

Basic GitHub Actions CI is configured to:
	•	Run unit tests automatically on each push
	•	Enforce that the codebase remains functional

See .github/workflows/ci.yml for details.
![CI](https://github.com/samskruthi05/snoop-etl-pipeline-/actions/workflows/ci.yml/badge.svg)

## Future extensions

With more time, the following could be implemented:
	•	S3-based ingestion using boto3
	•	Docker containerization for easy portability
	•	Deployment via Terraform or similar IaC
	•	Incremental ingestion support
	•	Lightweight API for monitoring pipeline runs

		Incremental ingestion support
	•	Lightweight API for monitoring pipeline runs


# for commit message



