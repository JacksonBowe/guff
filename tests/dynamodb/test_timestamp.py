from datetime import datetime, timezone
from guff import dynamodb

def test_timestamp_type():
    """
    Test that timestamp returns an integer.
    """
    current_timestamp = dynamodb.timestamp()
    assert isinstance(current_timestamp, int), "The timestamp is not an integer."

def test_timestamp_value():
    """
    Test that timestamp returns the current UTC time in milliseconds.
    """
    before_call = int(datetime.now(timezone.utc).timestamp() * 1000)
    current_timestamp = dynamodb.timestamp()
    after_call = int(datetime.now(timezone.utc).timestamp() * 1000)

    assert before_call <= current_timestamp <= after_call, "The timestamp is not within the expected range."
