import pytest
from guff import dynamodb

def test_serialize():
    """
    Test the serialize function to ensure it correctly serializes Python dictionaries
    to DynamoDB AttributeValues format.
    """
    input_dict = {
        'String': 'example_value',
        'Number': 123,
        'Boolean': True,
        'Null': None,
        'List': ['value1', 123],
        'Dict': {'nested_key': 'nested_value'}
    }

    expected_output = {
        'String': {'S': 'example_value'},
        'Number': {'N': '123'},
        'Boolean': {'BOOL': True},
        'Null': {'NULL': True},
        'List': {'L': [{'S': 'value1'}, {'N': '123'}]},
        'Dict': {'M': {'nested_key': {'S': 'nested_value'}}}
    }

    # Call the serialize function
    serialized_output = dynamodb.serialize(input_dict)

    # Verify the output matches the expected DynamoDB AttributeValues format
    assert serialized_output == expected_output, "Serialized output does not match the expected format."

def test_serialize_raises_type_error():
    """
    Test the serialize function raises a TypeError for unsupported input types.
    """
    with pytest.raises(TypeError):
        # Attempt to serialize an unsupported type (e.g., float, which DynamoDB does not directly support)
        dynamodb.serialize({'UnsupportedType': 1.2})