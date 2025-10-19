# simple_string_list_append

## Location
[src/fe_utils/simple_list.c:63-86](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/simple_list.c#L63-L86)

## Overview
Appends a string value to a simple linked list structure designed for frontend PostgreSQL utilities, creating a copy of the provided string.

## Definition
```c
void simple_string_list_append(SimpleStringList *list, const char *val)
```

## Detailed Description
This function adds a new string value to the end of a SimpleStringList. It allocates memory for a new SimpleStringListCell with enough space to hold both the cell structure and the string data. The function copies the provided string into the cell's flexible array member, initializes the touched flag to false, and properly links the cell to maintain the list structure. The function handles both empty lists (where head is NULL) and non-empty lists by updating the tail pointer appropriately. This is part of the simple list facilities designed for frontend code, providing basic list functionality without the complexity of the backend's List infrastructure.

## Parameters / Member Variables
- `list`: Pointer to the SimpleStringList structure to append to
- `val`: The null-terminated string value to append to the list (will be copied)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md) (for memory allocation)
  - strlen (for string length calculation)
  - strcpy (for string copying)
  - offsetof (for calculating memory offset)
- Data structures used:
  - [SimpleStringList](../S/SimpleStringList.md) (the list container structure)
  - [SimpleStringListCell](../S/SimpleStringListCell.md) (individual list node structure)
- Called from (representative examples):
  - [main](../m/main.md) functions in various pg_dump utilities (pg_dump.c, pg_dumpall.c, pg_restore.c)
  - [read_dump_filters](../r/read_dump_filters.md) (src/bin/pg_dump/pg_dump.c:19088-19143)
  - [reindex_one_database](../r/reindex_one_database.md) (src/bin/scripts/reindexdb.c:322)
  - [vacuum_one_database](../v/vacuum_one_database.md) (src/bin/scripts/vacuumdb.c:795)

## Notes and Other Information
- The string is copied into the cell, so the original string does not need to survive past the call
- Memory is allocated using pg_malloc with size calculated as offsetof(SimpleStringListCell, val) + strlen(val) + 1
- The touched field is initialized to false for use in tracking which strings have been processed
- Uses a flexible array member to store the string data efficiently within the same allocation
- The function maintains both head and tail pointers for efficient O(1) append operations
- This is specifically designed for frontend utilities and is simpler than backend List facilities
- Widely used across PostgreSQL frontend tools for managing lists of database object names, filter patterns, and command-line arguments

## Simplified Source

```c
void simple_string_list_append(SimpleStringList *list, const char *val) {
    // Allocate memory for cell structure plus string data
    SimpleStringListCell *cell = (SimpleStringListCell *)
        pg_malloc(offsetof(SimpleStringListCell, val) + strlen(val) + 1);

    // Initialize the new cell
    cell->next = NULL;
    cell->touched = false;
    strcpy(cell->val, val);  // Copy the string into the cell

    // Link the cell to the list
    if (list->tail) {
        list->tail->next = cell;  // Add to end of existing list
    } else {
        list->head = cell;        // First element in empty list
    }
    list->tail = cell;           // Update tail pointer
}
```