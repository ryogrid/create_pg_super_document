# replace_auto_config_value

## Location
[src/backend/utils/misc/guc.c:4540-4609](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/misc/guc.c#L4540-L4609)

## Overview
Updates a linked list of configuration parameters by adding, replacing, or deleting entries for a specified parameter name, maintaining the automatic configuration system's parameter list.

## Definition


## Detailed Description
This function manages updates to the automatic configuration parameter list by performing three types of operations: adding new parameters, replacing existing parameters, or deleting parameters (when value is NULL). The function first removes any existing entries that match the specified parameter name to avoid duplicates, then adds a new entry if a value is provided.

The function handles the linked list maintenance properly, updating both head and tail pointers as needed when items are removed or added. When creating new entries, it initializes all fields of the ConfigVariable structure with appropriate default values, including copying strings and setting metadata fields.

This function is essential for the ALTER SYSTEM command functionality, allowing dynamic updates to the automatic configuration without requiring manual file editing.

## Parameters / Member Variables
- : Pointer to the pointer of the first node in the configuration variable linked list
- : Pointer to the pointer of the last node in the configuration variable linked list  
- : The name of the configuration parameter to add, replace, or delete
- : The new value for the parameter (NULL to delete the parameter)

## Dependencies
- Functions called/Symbols referenced:
  - [guc_name_compare](../g/guc_name_compare.md)
  - [palloc](../p/palloc.md)
  - [pstrdup](../p/pstrdup.md)
  - [pfree](../p/pfree.md)
- Data structures used:
  - ConfigVariable
- Called from (representative examples):
  - [AlterSystemSetConfigFile](../A/AlterSystemSetConfigFile.md)

## Notes and Other Information
- This is a static function, only accessible within the guc.c source file
- Removes all existing matches for the parameter name to handle cases where external tools may have created duplicates
- When value is NULL, the function performs deletion by removing existing entries and returning without adding new ones
- New entries are initialized with empty filename, zero sourceline, and false flags for ignore and applied
- Memory management includes freeing all allocated strings (name, value, filename) when removing entries
- The function maintains proper linked list integrity by updating both head and tail pointers during operations
- Uses case-insensitive parameter name comparison through guc_name_compare function