import os
import boto3

import pytest
from moto import mock_aws

from guff.dynamodb import build_update_operation, UpdateOperation

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"

@pytest.fixture(scope="function")
def aws(aws_credentials):
    with mock_aws():
        yield

@pytest.fixture     
def user_table(aws):
    # Set up the mock DynamoDB table
    dynamodb = boto3.resource('dynamodb', region_name='ap-southeast-2')
    table = dynamodb.create_table(
        TableName='UserTable',
        KeySchema=[
            {
                'AttributeName': 'PK',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'SK',
                'KeyType': 'RANGE'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'PK',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'SK',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )

    # Ensure the table is active
    table.meta.client.get_waiter('table_exists').wait(TableName='UserTable')
    
    # Set the environment variable
    os.environ['SST_TABLE_TABLENAME_USERTABLE'] = table.table_name    

    return table


    result = s3.list_buckets()
    assert len(result["Buckets"]) == 1
   
@pytest.fixture
def base_dict(user_table):
    item = {
        'PK': 'user_id',
        'SK': 'range',
        'a': 1,
        'b': { 'b1': 2 }
    }
    # Initially populate the table
    user_table.put_item(Item=item)
    return item
    
def apply_update_operation(table, key, operation: UpdateOperation):
    """Apply the generated update operation to the DynamoDB table."""
    table.update_item(
        Key=key,
        UpdateExpression=operation.expression,
        ExpressionAttributeNames=operation.names,
        ExpressionAttributeValues=operation.values
    )

def test_add_operation(user_table, base_dict):    
    update_dict_add = {
        'c': 3
    }
    
    # Operation validation
    operation = build_update_operation(base_dict, update_dict_add)
    
    assert operation.expression == "set #c=:c"
    assert operation.names == {'#c': 'c'}
    assert operation.values == {':c': 3}
    
    # DDB validation
    key = {'PK': base_dict['PK'], 'SK': base_dict['SK']}
    apply_update_operation(user_table, key, operation)
    response = user_table.get_item(Key=key)
    item = response.get('Item', {})
    
    assert item.get('c') == 3  # Assert the new field 'c' was added with value 3

def test_modify_operation(user_table, base_dict):
    update_dict_modify = {
        'b': {'b1': 3}
    }
    
    # Operation validation
    operation = build_update_operation(base_dict, update_dict_modify)
    
    assert "set #b.#b1=:bb1" in operation.expression
    assert operation.names == {'#b': 'b', '#b1': 'b1'}
    assert operation.values == {':bb1': 3}
    
    # DDB validation
    key = {'PK': base_dict['PK'], 'SK': base_dict['SK']}
    apply_update_operation(user_table, key, operation)
    response = user_table.get_item(Key=key)
    item = response.get('Item', {})
    
    assert item['b']['b1'] == 3  # Assert the new field 'c' was added with value 3

def test_remove_operation(user_table, base_dict):
    update_dict_remove = {
        'a': None  # Assuming None signifies a removal
    }
    
    # Operation validation
    operation = build_update_operation(base_dict, update_dict_remove)
    
    assert "remove #a" in operation.expression
    assert operation.names == {'#a': 'a'}
    assert operation.values == {}
    
    # DDB validation
    key = {'PK': base_dict['PK'], 'SK': base_dict['SK']}
    apply_update_operation(user_table, key, operation)
    response = user_table.get_item(Key=key)
    item = response.get('Item', {})
    
    assert item.get('a') is None

def test_combined_operation(user_table, base_dict):
    update_dict_combined = {
        'b': { 'b2': 4 },  # Add new field to 'b'
        'c': None  # Remove 'c'
    }
    
    # Operation validation
    operation = build_update_operation(base_dict, update_dict_combined)
    
    assert operation.expression == "set #b.#b2=:bb2"
    assert operation.names == {'#b': 'b', '#b2': 'b2'}
    assert operation.values == {':bb2': 4}
    
    # DDB validation
    key = {'PK': base_dict['PK'], 'SK': base_dict['SK']}
    apply_update_operation(user_table, key, operation)
    response = user_table.get_item(Key=key)
    item = response.get('Item', {})
    
    assert item.get('b') == { 'b1': 2, 'b2': 4 }
    
def test_third_level_nested_addition(user_table, base_dict):
    update_dict = {
        'b': { 'b1': { 'b1a': { 'b1a1': 4 } } }
    }
    
    # Operation validation
    operation = build_update_operation(base_dict, update_dict)
    
    assert "set #b.#b1=:bb1" in operation.expression
    assert operation.names == {'#b': 'b', '#b1': 'b1'}
    assert operation.values == {':bb1': {'b1a': {'b1a1': 4}}}
    
    # DDB validation
    key = {'PK': base_dict['PK'], 'SK': base_dict['SK']}
    apply_update_operation(user_table, key, operation)
    response = user_table.get_item(Key=key)
    item = response.get('Item', {})
    
    assert item.get('b') == { 'b1': { 'b1a': { 'b1a1': 4 } } }

def test_fourth_level_nested_modification(user_table, base_dict):
    # Extending base_dict for this test
    base_dict['b']['b1'] = { 'b1a': { 'b1a1': 3 } }
    user_table.put_item(Item=base_dict)
    update_dict = {
        'b': { 'b1': { 'b1a': { 'b1a1': 4 } } }
    }
    
    # Operation validation
    operation = build_update_operation(base_dict, update_dict)
    
    assert "set #b.#b1.#b1a.#b1a1=:bb1b1ab1a1" in operation.expression
    assert operation.names == {'#b': 'b', '#b1': 'b1', '#b1a': 'b1a', '#b1a1': 'b1a1'}
    assert operation.values == {':bb1b1ab1a1': 4}
    
    # DDB validation
    key = {'PK': base_dict['PK'], 'SK': base_dict['SK']}
    apply_update_operation(user_table, key, operation)
    response = user_table.get_item(Key=key)
    item = response.get('Item', {})
    
    assert item['b']['b1']['b1a']['b1a1'] == 4

def test_override_non_dict_with_dict(user_table, base_dict):
    '''
    Validates that a non-dictionary attribute (b1) is overridden by a dictionary
    when applying the update operation. Initially, b1 is a number and is expected
    to be replaced by a dictionary containing nested attributes.
    '''
    # Set up the base state where 'b1' is a number
    base_dict.update({'b': {'b1': 2}})  # Assuming 'b' exists and 'b1' is a number
    user_table.put_item(Item=base_dict)

    # Define the update that replaces 'b1' with a dictionary
    update_dict = {
        'b': {'b1': {'b2': None, 'b3': {'b3a': 5}}}
    }
    
    # Generate the update operation
    operation = build_update_operation(base_dict, update_dict)
    
    print(operation)
    # Operation validation
    # Since 'b1' was a number and now becomes a dictionary, no 'remove' operation is expected for 'b1'
    # 'b2' being None in the update_dict doesn't result in a 'remove' operation since 'b1' itself is replaced
    assert "set #b.#b1=:bb1" in operation.expression
    assert operation.names == {'#b': 'b', '#b1': 'b1'}
    assert operation.values == {':bb1': {'b3': {'b3a': 5}}}
    
    # Apply the update operation to DynamoDB
    key = {'PK': base_dict['PK'], 'SK': base_dict['SK']}
    apply_update_operation(user_table, key, operation)
    
    # Validate the change in DynamoDB
    response = user_table.get_item(Key=key)
    item = response.get('Item', {})
    
    # Checking the new structure of 'b1'
    assert isinstance(item['b']['b1'], dict), "b1 should now be a dictionary"
    assert 'b2' not in item['b']['b1'], "b2 should not exist since b1 was overridden"
    assert item['b']['b1'].get('b3', {}).get('b3a') == 5, "b3a should be 5"

def test_multiple_nested_operations(user_table, base_dict):
    print()
    # Set up the base state where 'b1' is a dict
    base_dict.update({'b': { 'b1': {'b1a': {'b1a1': 4}}}})  # Assuming 'b' exists and 'b1' is a number
    user_table.put_item(Item=base_dict)
    
    update_dict = {
        'a': None,
        'b': {
            'b1': {
                'b1a': { 'b1a1': 5 },   # Nested update
                'b1b': { 'b1b1': 4 }    # Nested put
            },
            'b2': None,                 # Nested remove
        },
        'd': 'dog',                     # Top level put
        'z': None                       # Top level remove non-existing key
    }
    
    # Operation validation
    operation = build_update_operation(base_dict, update_dict)
    print(operation)
    assert operation.expression == "set #b.#b1.#b1a.#b1a1=:bb1b1ab1a1, #b.#b1.#b1b=:bb1b1b, #d=:d  remove #a"
    assert operation.names == {'#a': 'a', '#b': 'b', '#b1': 'b1', '#b1a': 'b1a', '#b1a1': 'b1a1', '#b1b': 'b1b', '#d': 'd'}
    assert operation.values == {':bb1b1ab1a1': 5, ':bb1b1b': {'b1b1': 4}, ':d': 'dog'}
    
    # DDB validation
    key = {'PK': base_dict['PK'], 'SK': base_dict['SK']}
    apply_update_operation(user_table, key, operation)
    response = user_table.get_item(Key=key)
    item = response.get('Item', {})
    
    assert 'a' not in item                      # a removed
    assert 'b2' not in item['b']                # b2 removed
    assert item['b']['b1']['b1a']['b1a1'] == 5  # b1a1 modified
    assert item['b']['b1']['b1b']['b1b1'] == 4  # b1b1 added
    assert item['d'] == 'dog'                   # d added

# Example nested dictionaries
# dictA = {'a': 1, 'c': { "w": "okay", "y": 44}}
# dictB = {'a': None, 'b': {'x': { 'y': 100 }, 'z': 40}, 'd': 5, 'c': { "w": "done"}}

# operation = build_update_operation(dictA, dictB)

# print(operation)

if __name__ == "__main__":
    # test_build_update_operation()
    pass