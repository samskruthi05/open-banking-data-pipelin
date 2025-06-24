import unittest
from pipeline.dq_checks import generate_dq_errors

class TestDQChecks(unittest.TestCase):

    def test_valid_record(self):
        record = {
            "customerId": "123",
            "transactionId": "txn-001",
            "transactionDate": "2023-01-01",
            "currency": "USD",
            "amount": "100.00"
        }
        self.assertEqual(generate_dq_errors(record), [])

    def test_invalid_currency(self):
        record = {
            "customerId": "123",
            "transactionId": "txn-002",
            "transactionDate": "2023-01-01",
            "currency": "INR",
            "amount": "100.00"
        }
        self.assertIn("Invalid currency", generate_dq_errors(record))

    def test_invalid_date(self):
        record = {
            "customerId": "123",
            "transactionId": "txn-003",
            "transactionDate": "2023-13-01",
            "currency": "USD",
            "amount": "100.00"
        }
        self.assertIn("Invalid transaction date", generate_dq_errors(record))

    def test_zero_amount(self):
        record = {
            "customerId": "123",
            "transactionId": "txn-004",
            "transactionDate": "2023-01-01",
            "currency": "USD",
            "amount": "0"
        }
        self.assertIn("Invalid amount", generate_dq_errors(record))


if __name__ == '__main__':
    unittest.main()
