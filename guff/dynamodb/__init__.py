from typing import Mapping, List, Tuple, Any, Dict, Optional
from dataclasses import dataclass

def remove_none_values(obj):
    """
    Recursively removes None values from a data structure that may include
    dictionaries, lists, strings, and integers. When a dictionary is encountered,
    it removes keys that have None values. When a list is encountered, it removes
    None values and processes its elements.

    Parameters:
    - obj: The data structure to process. Can be of any type.

    Returns:
    - The processed data structure with None values removed.
    """
    if isinstance(obj, dict):
        # For dictionaries, create a new dictionary without None values
        return {k: remove_none_values(v) for k, v in obj.items() if v is not None}
    elif isinstance(obj, list):
        # For lists, remove None values and process each element
        return [remove_none_values(item) for item in obj if item is not None]
    else:
        # For other types like strings and integers, return as is
        return obj

def _deep_merge_and_log_changes(base_dict: Mapping[str, any], update_dict: Mapping[str, any], path: Optional[List[str]] = None, changelog: Optional[Dict[str, any]] = None) -> Dict[str, any]:
    """
    Merges updates from update_dict into base_dict and logs the changes.
    
    Parameters:
    - base_dict (dict): Dictionary to update.
    - update_dict (dict): Dictionary with updates.
    - path (List[str], optional): Path to current attribute.
    - changelog (Dict[str, any], optional): Log of changes.
    
    Returns:
    - Dict[str, any]: Log of changes with paths as keys and updated values as values.
    """
    if path is None:
        path = []
    if changelog is None:
        changelog = {}

    print('Base', base_dict)
    print('Update', update_dict)
    for key, value in update_dict.items():
        current_path = path + [key]  # Build the path for the current key
        print('Current path', current_path)
        if key not in base_dict and value != None:
            print('Setting', value)
            changelog.update({
                '.'.join(current_path): value,
            })
            
            base_dict[key] = value
        else:
            if key in base_dict and isinstance(base_dict[key], dict) and isinstance(value, dict):
                # Recurse into nested dictionaries
                _deep_merge_and_log_changes(base_dict[key], value, current_path, changelog)
            elif key in base_dict and base_dict[key] != value:
                print('how', key, base_dict[key])
                print('Setting', value)
                changelog.update({
                    '.'.join(current_path): value,
                })
                base_dict[key] = value

    return changelog

@dataclass
class UpdateOperation:
    """
    Represents a database update operation including the expression,
    attribute names, and values.
    """
    expression: str
    names: dict
    values: dict

def build_update_operation(base_dict: Dict[str, any], update_dict: Dict[str, any]) -> UpdateOperation:
    """
    Creates an UpdateOperation from changes between base_dict and update_dict.
    
    Parameters:
    - base_dict (Dict[str, any]): Original dictionary.
    - update_dict (Dict[str, any]): Updates to apply.
    
    Returns:
    - UpdateOperation: Contains DynamoDB update expression, attribute names, and values.
    """
    # ---- Build the changelog
    changelog = _deep_merge_and_log_changes(base_dict, update_dict)
    print('CHANGELOG', changelog)
        
    # ---- Build the update operation
    set_expression = []
    remove_expression = []
    update_names = dict()
    update_values = dict()
    
    # For each key in the map, alias it
    for full_key, val in changelog.items():
        parts = full_key.split('.')
        for part in parts:
            key = part
            
            update_names[f'#{key}'] = key # To avoid 'reserved word' conflicts
            
        full_key_hash = '#' + full_key.replace('.', '.#') # convert 'attr1.sub1' to '#attr1.#sub1'
        full_key_val = ':' + full_key.replace('.', '') # convert 'attr1.sub1' to ':attr1.:sub1'
        
        if val is not None:
            # SET new values
            if not set_expression: set_expression.append('set')
            set_expression.append(f" {full_key_hash}={full_key_val},")
            update_values[f"{full_key_val}"] = val # To avoid 'reserved word' conflicts
            
        elif val is None:
            # REMOVE values
            if not remove_expression: remove_expression.append('remove')
            remove_expression.append(f" {full_key_hash},")
    
    update_expression =  "".join(set_expression)[:-1] + "  " + "".join(remove_expression)[:-1]
    
    return UpdateOperation(
        update_expression.strip(), 
        update_names, 
        remove_none_values(update_values)
    )