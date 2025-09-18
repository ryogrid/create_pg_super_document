# simple_string_list_member

## Location
src/fe_utils/simple_list.c: 87 - 105

## Overview
Checks whether a specific string value exists in a simple linked list structure designed for frontend PostgreSQL utilities, and marks the found entry as "touched".

## Definition
```c
bool simple_string_list_member(SimpleStringList *list, const char *val)
```

## Detailed Description
This function performs a linear search through a SimpleStringList to determine if a given string value is present in the list. It iterates through each cell in the linked list, comparing the stored string with the target string using strcmp. If a match is found, the function sets the "touched" field of that cell to true and returns true. If no match is found after traversing the entire list, it returns false. This is part of the simple list facilities designed for frontend code, providing basic membership testing functionality with an additional tracking mechanism for processed strings.

## Parameters / Member Variables
- `list`: Pointer to the SimpleStringList structure to search in
- `val`: The null-terminated string value to search for in the list

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (for string comparison)
- Data structures used:
  - [SimpleStringList](../S/SimpleStringList.md) (the list container structure)
  - [SimpleStringListCell](../S/SimpleStringListCell.md) (individual list node structure)
- Called from (representative examples):
  - [main](../m/main.md) functions in pg_createsubscriber (src/bin/pg_basebackup/pg_createsubscriber.c:1969, 2010, 2022, 2034)
  - [_tocEntryRequired](../t/_tocEntryRequired.md) (src/bin/pg_dump/pg_backup_archiver.c:3069, 3075, 3092, 3100, 3110, 3118)
  - [dumpDatabases](../d/dumpDatabases.md) (src/bin/pg_dump/pg_dumpall.c:1618)

## Notes and Other Information
- Performs O(n) linear search through the list using string comparison
- Returns false immediately if an empty list is provided
- Sets the "touched" field to true for the first matching entry found, enabling tracking of which strings have been accessed
- The touched mechanism is useful for identifying unused filter patterns or command-line arguments
- No memory allocation is performed during the search operation
- This is specifically designed for frontend utilities and is simpler than backend List facilities
- The function is commonly used in PostgreSQL dump and restore utilities for filtering operations and command-line argument processing