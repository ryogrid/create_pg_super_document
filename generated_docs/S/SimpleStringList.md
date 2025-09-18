# SimpleStringList

## Location
[src/include/fe_utils/simple_list.h:40-44](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/fe_utils/simple_list.h#L40-L44)

## Overview
SimpleStringList is a singly-linked list container that manages collections of strings with tracking capabilities, providing efficient head and tail access for PostgreSQL frontend utility operations.

## Definition
```c
typedef struct SimpleStringList
{
    SimpleStringListCell *head;
    SimpleStringListCell *tail;
} SimpleStringList;
```

## Detailed Description
SimpleStringList implements a singly-linked list specifically designed for managing collections of strings in PostgreSQL frontend utilities. The structure maintains both head and tail pointers to SimpleStringListCell nodes, enabling efficient append operations and list traversal. This design is extensively used throughout PostgreSQL's command-line tools for managing lists of database names, table names, schema names, and other string-based identifiers. The list supports tracking functionality through the 'touched' flag in each cell, making it useful for validation and processing workflows where it's important to know which items have been accessed.

## Parameters / Member Variables
- `head`: Pointer to the first SimpleStringListCell in the list, or NULL if the list is empty
- `tail`: Pointer to the last SimpleStringListCell in the list, or NULL if the list is empty; enables efficient O(1) append operations

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleStringListCell](SimpleStringListCell.md) (the node type used to build the linked list)
- Called from (representative examples):
  - [simple_string_list_append](../s/simple_string_list_append.md)
  - [simple_string_list_member](../s/simple_string_list_member.md)
  - [simple_string_list_destroy](../s/simple_string_list_destroy.md)
  - [simple_string_list_not_touched](../s/simple_string_list_not_touched.md)
  - [CreateSubscriberOptions](../C/CreateSubscriberOptions.md) (in pg_createsubscriber)
  - [_restoreOptions](../r/_restoreOptions.md) (in pg_dump/pg_restore)
  - [OidOptions](../O/OidOptions.md) (in pg_dump)
  - ReindexType (in reindexdb)
  - VacObjFilter (in vacuumdb)
  - [verifier_context](../v/verifier_context.md) (in pg_verifybackup)

## Notes and Other Information
- Extensively used across PostgreSQL frontend tools including pg_dump, pg_dumpall, clusterdb, reindexdb, vacuumdb, and pg_verifybackup
- The dual head/tail pointer design allows for efficient list building without traversing the entire list for each append operation
- Supports tracking functionality through SimpleStringListCell's 'touched' flag, useful for determining which strings have been processed
- Memory-efficient design with variable-length string storage in each cell using flexible array members
- Common use cases include managing filter lists, option lists, and collections of database object names in command-line utilities
- The structure is part of PostgreSQL's frontend utility framework, designed for client-side tools rather than server-side operations