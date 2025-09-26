# simple_oid_list_append

## Location
[src/fe_utils/simple_list.c:26-44](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/simple_list.c#L26-L44)

## Overview
Appends an OID (Object Identifier) value to a simple linked list structure designed for frontend PostgreSQL utilities.

## Definition
```c
void simple_oid_list_append(SimpleOidList *list, Oid val)
```

## Detailed Description
This function adds a new OID value to the end of a SimpleOidList. It allocates memory for a new SimpleOidListCell, sets its value to the provided OID, and properly links it to maintain the list structure. The function handles both empty lists (where head is NULL) and non-empty lists by updating the tail pointer appropriately. This is part of the simple list facilities designed for frontend code like pg_dump, providing basic list functionality without the complexity of the backend's List infrastructure.

## Parameters / Member Variables
- `list`: Pointer to the SimpleOidList structure to append to
- `val`: The OID value to append to the list

## Dependencies
- Functions called/Symbols referenced:
  - [pg_malloc](../p/pg_malloc.md) (for memory allocation)
- Data structures used:
  - [SimpleOidList](../S/SimpleOidList.md) (the list container structure)
  - [SimpleOidListCell](../S/SimpleOidListCell.md) (individual list node structure)
- Called from (representative examples):
  - [expand_schema_name_patterns](../e/expand_schema_name_patterns.md) (src/bin/pg_dump/pg_dump.c:1493)
  - [expand_extension_name_patterns](../e/expand_extension_name_patterns.md) (src/bin/pg_dump/pg_dump.c:1546)
  - [expand_foreign_server_name_patterns](../e/expand_foreign_server_name_patterns.md) (src/bin/pg_dump/pg_dump.c:1598)
  - [expand_table_name_patterns](../e/expand_table_name_patterns.md) (src/bin/pg_dump/pg_dump.c:1692)

## Notes and Other Information
- Memory is allocated using pg_malloc, which will exit on allocation failure
- The function maintains both head and tail pointers for efficient O(1) append operations
- This is specifically designed for frontend utilities and is simpler than backend List facilities
- The list cells are linked via next pointers forming a singly-linked list structure