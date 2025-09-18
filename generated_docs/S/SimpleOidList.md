# SimpleOidList

## Location
src/include/fe_utils/simple_list.h: 26 - 30

## Overview
SimpleOidList is a singly-linked list container that manages a collection of PostgreSQL Object Identifier (Oid) values, providing efficient head and tail access for frontend utility operations.

## Definition
```c
typedef struct SimpleOidList
{
    SimpleOidListCell *head;
    SimpleOidListCell *tail;
} SimpleOidList;
```

## Detailed Description
SimpleOidList implements a singly-linked list specifically designed for storing Oid values in PostgreSQL frontend utilities. The structure maintains both head and tail pointers to the underlying SimpleOidListCell nodes, enabling efficient append operations at both ends of the list. This design is particularly useful for building lists of object identifiers during database operations like pg_dump, where multiple database objects need to be tracked and processed. The list provides O(1) append operations and supports typical list operations through associated utility functions.

## Parameters / Member Variables
- `head`: Pointer to the first SimpleOidListCell in the list, or NULL if the list is empty
- `tail`: Pointer to the last SimpleOidListCell in the list, or NULL if the list is empty; enables efficient append operations

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleOidListCell](SimpleOidListCell.md) (the node type used to build the linked list)
- Called from (representative examples):
  - [simple_oid_list_append](../s/simple_oid_list_append.md)
  - [simple_oid_list_member](../s/simple_oid_list_member.md)  
  - [simple_oid_list_destroy](../s/simple_oid_list_destroy.md)
  - [OidOptions](../O/OidOptions.md) (in pg_dump for various option filtering)
  - [expand_schema_name_patterns](../e/expand_schema_name_patterns.md)
  - [expand_table_name_patterns](../e/expand_table_name_patterns.md)
  - [expand_extension_name_patterns](../e/expand_extension_name_patterns.md)
  - [expand_foreign_server_name_patterns](../e/expand_foreign_server_name_patterns.md)

## Notes and Other Information
- Primarily used by PostgreSQL frontend tools, especially pg_dump for managing lists of database object identifiers
- The dual head/tail pointer design allows for efficient appending without traversing the entire list
- Memory management is handled by associated utility functions in simple_list.c
- Commonly used in pg_dump's OidOptions structure for filtering and organizing database objects during dump operations
- The structure supports typical linked list operations while being optimized for Oid storage and manipulation