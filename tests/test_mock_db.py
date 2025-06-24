
from unittest.mock import MagicMock
from pipeline.load import log_error_records


def test_log_errors(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mocker.patch("pipeline.load.psycopg2.connect", return_value=mock_conn)

    errors = [{
        "customer_id": "C001",
        "transaction_id": "T001",
        "error_reason": "Invalid amount",
        "raw_data": {"customer_id": "C001", "amount": 0}
    }]

    log_error_records(errors)
    
    mock_cursor.execute.assert_called_once()
    mock_conn.commit.assert_called_once()
