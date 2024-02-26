import re
import uuid
from guff import dynamodb 

def test_new_id_format_and_uniqueness():
    """
    Test that new_id returns a UUID4 string and each call returns a unique value.
    """
    id1 = dynamodb.new_id()
    id2 = dynamodb.new_id()

    # Check UUID format
    uuid4_regex = re.compile(r'^[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}\Z', re.I)
    assert uuid4_regex.match(id1), "The generated ID does not match the UUID4 format."
    assert uuid4_regex.match(id2), "The generated ID does not match the UUID4 format."

    # Check uniqueness
    assert id1 != id2, "Two calls to new_id returned the same value."