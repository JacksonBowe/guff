
from guff.dynamodb import remove_none_values

def test_remove_none_values():
    original_data = {
        'a': None,
        'b': 2,
        'c': [
            None,
            1,
            {'c1': None, 'c2': 3},
            [None, 4, {'d1': None, 'd2': 5}]
        ],
        'd': 'text',
        'e': {'e1': None, 'e2': {'e2a': None, 'e2b': 6}},
        'f': [None, 7]
    }

    expected_data = {
        'b': 2,
        'c': [
            1,
            {'c2': 3},
            [4, {'d2': 5}]
        ],
        'd': 'text',
        'e': {'e2': {'e2b': 6}},
        'f': [7]
    }

    # Remove None values at all levels
    cleaned_data = remove_none_values(original_data)

    assert cleaned_data == expected_data, "The function should correctly remove None values from nested structures."